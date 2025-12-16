#!/usr/bin/env python3
"""Generate SQL insert statements from open-webui chat JSON files."""
import argparse
import json
import os
import uuid
import re
import hashlib


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


def calculate_hash(file_path: str) -> str:
    """Calculate SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except FileNotFoundError:
        return "NULL"


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


def process_files(data: dict, json_path: str, user_id: str) -> list[str]:
    """Process files in chat history and return INSERT statements."""
    file_inserts = []
    messages_map = data.get("history", {}).get("messages", {})
    
    # Also handle list format if present (though OpenWebUI typically uses history.messages dict)
    # But convert_chatgpt produces history.messages as dict.
    
    processed_file_ids = set()

    for msg_id, msg in messages_map.items():
        files = msg.get("files", [])
        if not files:
            continue
            
        for f in files:
            file_id = f.get("id")
            if not file_id or file_id in processed_file_ids:
                continue
                
            processed_file_ids.add(file_id)
            
            filename = f.get("name")
            meta = f.get("meta", {})
            # Add collection_name to meta as seen in working.sql
            if "collection_name" not in meta:
                meta["collection_name"] = f"file-{file_id}"
            
            file_data = f.get("data", {"status": "completed"})
            timestamp = msg.get("timestamp", data.get("timestamp", 0))
            created_at = int(timestamp) # timestamp is usually unix timestamp (seconds or ms?)
            # data.get("timestamp") in json_to_sql is ms. msg timestamp in convert_chatgpt is seconds (int(ts)).
            # working.sql created_at is seconds (176...)
            # convert_chatgpt produces msg["timestamp"] as int(ts) which is seconds.
            
            # Find the actual file to calculate hash
            # convert_chatgpt names file as {id}_{name} in media/ dir
            real_filename = f"{file_id}_{filename}"
            media_path = os.path.join(os.path.dirname(json_path), "media", real_filename)
            
            file_hash = calculate_hash(media_path)
            if file_hash == "NULL":
                # Try finding it without ID prefix if failed (fallback)
                media_path_alt = os.path.join(os.path.dirname(json_path), "media", filename)
                file_hash_alt = calculate_hash(media_path_alt)
                if file_hash_alt != "NULL":
                    file_hash = file_hash_alt
                    # But path should still use ID prefix if that's the convention
            
            hash_val = f"'{file_hash}'" if file_hash != "NULL" else "NULL"
            
            # Paths
            # DB Path: /app/backend/data/uploads/imported/{id}_{filename}
            # URL: /uploads/imported/{id}_{filename}
            
            db_path = f"/app/backend/data/uploads/imported/{file_id}_{filename}"
            url_path = f"/uploads/imported/{file_id}_{filename}"
            
            # Update Markdown content in message
            content = msg.get("content", "")
            # convert_chatgpt produces: ![name](media/{id}_{name}) or [Media: ...](media/...)
            # regex replace media/ with /uploads/imported/
            # Be careful not to replace other media/ strings
            # Look for (media/{file_id}_{filename})
            
            # We need to update the msg object in place so it gets saved to chat JSON
            # Construct the exact string convert_chatgpt produces to target it
            # It uses f"{media_url_prefix}/{new_filename}" where prefix is "media" by default.
            
            # Regex replacement for this specific file
            # Pattern: (media/file_id_filename) -> (/uploads/imported/file_id_filename)
            # escape filename for regex
            safe_name = re.escape(real_filename)
            pattern = r"\(media/" + safe_name + r"\)"
            replacement = f"({url_path})"
            
            new_content = re.sub(pattern, replacement, content)
            msg["content"] = new_content
            
            # Generate SQL
            meta_str = escape_sql_string(json.dumps(meta, ensure_ascii=True))
            data_str = escape_sql_string(json.dumps(file_data, ensure_ascii=True))
            filename_esc = escape_sql_string(filename)
            db_path_esc = escape_sql_string(db_path)
            
            insert_stmt = (
                f"INSERT INTO \"main\".\"file\" "
                f"(\"id\", \"user_id\", \"filename\", \"meta\", \"created_at\", \"hash\", \"data\", \"updated_at\", \"path\", \"access_control\") "
                f"VALUES ('{file_id}', '{user_id}', '{filename_esc}', '{meta_str}', {created_at}, {hash_val}, '{data_str}', {created_at}, '{db_path_esc}', 'null');"
            )
            file_inserts.append(insert_stmt)
            
    return file_inserts


def json_to_sql(path: str, tags: list[str]) -> tuple[str, list[str], str]:
    data = load_json(path)
    
    user_id = data.get("userId")
    if not user_id:
        raise ValueError(f"userId missing in {path}")
        
    # Process files first to update content in data
    file_inserts = process_files(data, path, user_id)
    
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
    return sql, file_inserts, user_id


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
    all_file_inserts = []
    user_ids: set[str] = set()
    for fpath in files:
        try:
            sql, f_inserts, uid = json_to_sql(fpath, tags)
            chat_inserts.append(sql)
            all_file_inserts.extend(f_inserts)
            user_ids.add(uid)
        except Exception as exc:
            raise SystemExit(f"Failed to process {fpath}: {exc}")

    prefix = []
    for uid in sorted(user_ids):
        prefix.extend(tag_upserts(uid, tags))

    # Combine: Tags -> Files -> Chats
    output = "\n".join(prefix + all_file_inserts + chat_inserts)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output + "\n")
    else:
        print(output)


if __name__ == "__main__":
    main()
