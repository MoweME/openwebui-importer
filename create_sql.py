#!/usr/bin/env python3
"""Generate SQL insert statements from open-webui chat JSON files."""
import argparse
import json
import os
import uuid
import re
import base64
import mimetypes


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def build_meta(tags: list[str]) -> str:
    meta = json.dumps({"tags": tags}, ensure_ascii=True)
    return escape_sql_string(meta)


def slugify(value: str) -> str:
    """Return a slug suitable for use as an identifier."""
    value = value.lower()
    value = re.sub(r"[^a-z0-9_-]+", "-", value)
    return re.sub(r"-+", "-", value).strip("-")


def tag_upserts(user_id: str, meta_tags: list[str]) -> list[str]:
    """Return SQL statements to ensure tags exist for the user."""
    base_tags = [
        ("imported-grok", "imported-grok"),
        ("imported-chatgpt", "imported-chatgpt"),
        ("imported-claude", "imported-claude"),
    ]
    for t in meta_tags:
        slug = slugify(t)
        base_tags.append((slug, t))

    unique: dict[str, str] = {}
    for tag_id, name in base_tags:
        unique[tag_id] = name

    stmts = []
    for tag_id, name in unique.items():
        stmts.append(
            'INSERT INTO "main"."tag" ("id","name","user_id","meta") '
            f"VALUES ('{tag_id}','{name}','{user_id}','null') "
            'ON CONFLICT("id","user_id") DO UPDATE SET "name"=excluded."name";'
        )
    return stmts


def process_files(data: dict, json_path: str) -> None:
    """
    Process files in chat history:
    1. Read file from media/ directory.
    2. Convert to Base64 Data URI.
    3. Update file object in message.
    4. Remove Markdown image link from content.
    """
    messages_map = data.get("history", {}).get("messages", {})
    
    # Track processed IDs to avoid double processing if referenced multiple times (unlikely in this structure but good practice)
    processed_files = set()

    for msg_id, msg in messages_map.items():
        files = msg.get("files", [])
        if not files:
            continue
            
        new_files_list = []
        content = msg.get("content", "")
        
        for f in files:
            file_id = f.get("id")
            filename = f.get("name")
            
            if not file_id or not filename:
                # Malformed file entry, keep as is or skip?
                new_files_list.append(f)
                continue

            # Find the actual file on disk
            # convert_chatgpt names file as {id}_{name} in media/ dir
            real_filename = f"{file_id}_{filename}"
            media_path = os.path.join(os.path.dirname(json_path), "media", real_filename)
            
            # Fallback for file finding
            if not os.path.exists(media_path):
                media_path_alt = os.path.join(os.path.dirname(json_path), "media", filename)
                if os.path.exists(media_path_alt):
                    media_path = media_path_alt
            
            if os.path.exists(media_path):
                # Guess mime type
                mime_type, _ = mimetypes.guess_type(media_path)
                if not mime_type:
                    mime_type = "application/octet-stream"
                
                # Read and encode
                try:
                    with open(media_path, "rb") as image_file:
                        encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
                    
                    data_uri = f"data:{mime_type};base64,{encoded_string}"
                    
                    # Create new file object compliant with OpenWebUI embedded format
                    new_file_obj = {
                        "type": "image" if mime_type.startswith("image/") else "file",
                        "url": data_uri,
                        "name": filename
                    }
                    new_files_list.append(new_file_obj)
                    
                    # Remove markdown link from content
                    # Pattern: ![filename](/uploads/imported/file_id_filename)
                    # or ![filename](media/file_id_filename)
                    # We match partially on the file_id to be safe
                    
                    # Escape ID for regex
                    safe_id = re.escape(file_id)
                    # Regex to match ![...](...id...)
                    # We want to remove the whole image tag including newlines around it if possible
                    # to avoid gaps.
                    
                    # This regex matches ![alt](...id...)
                    # It handles the case where the path might differ.
                    pattern = r"\!?\[.*?\]\(.*?" + safe_id + r".*?\)"
                    content = re.sub(pattern, "", content)
                    
                except Exception as e:
                    print(f"Error processing file {media_path}: {e}")
                    new_files_list.append(f) # Keep original on error
            else:
                print(f"Warning: File not found {media_path}")
                new_files_list.append(f)

        # Update message
        msg["files"] = new_files_list
        msg["content"] = content.strip()


def json_to_sql(path: str, tags: list[str]) -> tuple[str, str]:
    data = load_json(path)
    
    user_id = data.get("userId")
    if not user_id:
        raise ValueError(f"userId missing in {path}")
        
    # Process files to embed them
    process_files(data, path)
    
    chat_json = json.dumps(data, ensure_ascii=True)
    chat_json = escape_sql_string(chat_json)

    title = escape_sql_string(data.get("title", ""))
    timestamp_ms = data.get("timestamp", 0)
    created_at = int(int(timestamp_ms) / 1000)

    base = os.path.splitext(os.path.basename(path))[0]
    possible_id = base.split("_")[-1]
    try:
        uuid.UUID(possible_id)
        record_id = possible_id
    except ValueError:
        record_id = str(uuid.uuid4())

    meta = build_meta(tags)

    sql = (
        f"DELETE FROM \"main\".\"chat\" WHERE \"id\" = '{record_id}';\n"
        "INSERT INTO \"main\".\"chat\" "
        "(\"id\",\"user_id\",\"title\",\"share_id\",\"archived\",\"created_at\",\"updated_at\",\"chat\",\"pinned\",\"meta\",\"folder_id\")\n"
        f"VALUES ('{record_id}','{user_id}','{title}',NULL,0,{created_at},{created_at},'{chat_json}',0,'{meta}',NULL);"
    )
    return sql, user_id


def gather_files(paths: list[str]) -> list[str]:
    result = []
    for p in paths:
        if os.path.isdir(p):
            for name in os.listdir(p):
                if name.endswith('.json'):
                    result.append(os.path.join(p, name))
        else:
            result.append(p)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Create SQL inserts for open-webui chats")
    parser.add_argument("files", nargs="+", help="Chat JSON files or directories")
    parser.add_argument("--tags", default="imported", help="Comma-separated tags for the meta field")
    parser.add_argument("--output", help="Write SQL statements to this file")
    args = parser.parse_args()

    tags = [t.strip() for t in args.tags.split(',') if t.strip()] or ["imported"]

    files = gather_files(args.files)
    chat_inserts = []
    user_ids: set[str] = set()
    for fpath in files:
        try:
            sql, uid = json_to_sql(fpath, tags)
            chat_inserts.append(sql)
            user_ids.add(uid)
        except Exception as exc:
            raise SystemExit(f"Failed to process {fpath}: {exc}")

    prefix = []
    for uid in sorted(user_ids):
        prefix.extend(tag_upserts(uid, tags))

    # Combine: Tags -> Chats (Files are embedded)
    output = "\n".join(prefix + chat_inserts)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output + "\n")
    else:
        print(output)


if __name__ == "__main__":
    main()
