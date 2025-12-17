#!/usr/bin/env python3
"""Generate SQL insert statements from open-webui chat JSON files."""
import argparse
import json
import os
import uuid
import re
import base64
import mimetypes
import shutil


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def build_meta(tags: list[str]) -> str:
    meta = json.dumps({"tags": tags}, ensure_ascii=False)
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


def compute_file_hash(filepath: str) -> str:
    """Compute SHA256 hash of a file."""
    import hashlib
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def process_files(data: dict, json_path: str, uploads_dir: str, user_id: str) -> list[dict]:
    """
    Process files in chat history:
    1. Read file from media/ directory.
    2. If image, Convert to Base64 Data URI.
    3. If other, copy to uploads/ directory with proper format.
    4. Update file object in message.
    5. Remove Markdown image link from content.
    6. Return list of file records for SQL INSERT.
    """
    messages_map = data.get("history", {}).get("messages", {})
    messages_list = data.get("messages", [])
    file_records = []  # For SQL INSERT into file table
    
    # Create lookup for messages_list by id
    messages_list_lookup = {m.get("id"): m for m in messages_list}
    
    for msg_id, msg in messages_map.items():
        files = msg.get("files", [])
        if not files:
            continue
            
        new_files_list = []
        content = msg.get("content", "")
        
        for f in files:
            # Check if already embedded image (base64 data URI)
            if f.get("type") == "image" and f.get("url", "").startswith("data:"):
                new_files_list.append(f)
                continue
            
            # Check if already in full format with nested file object
            if f.get("type") == "file" and isinstance(f.get("file"), dict):
                nested_file = f["file"]
                file_id = f.get("id") or nested_file.get("id")
                filename = f.get("name") or nested_file.get("filename")
                
                if file_id and filename:
                    # Find and copy the file to uploads
                    real_filename = f"{file_id}_{filename}"
                    media_path = os.path.join(os.path.dirname(json_path), "media", real_filename)
                    
                    if os.path.exists(media_path):
                        # Copy to uploads
                        dst_path = os.path.join(uploads_dir, real_filename)
                        if not os.path.exists(dst_path):
                            shutil.copy2(media_path, dst_path)
                        
                        file_size = os.path.getsize(media_path)
                        file_hash = compute_file_hash(media_path)
                        mime_type, _ = mimetypes.guess_type(media_path)
                        if not mime_type:
                            mime_type = nested_file.get("meta", {}).get("content_type", "application/octet-stream")
                        current_time = int(os.path.getmtime(media_path))
                        
                        # Add to file records for SQL INSERT
                        file_records.append({
                            "id": file_id,
                            "user_id": user_id,
                            "filename": filename,
                            "meta": {
                                "name": filename,
                                "content_type": mime_type,
                                "size": file_size,
                                "data": {},
                                "collection_name": f"file-{file_id}"
                            },
                            "created_at": current_time,
                            "updated_at": current_time,
                            "hash": file_hash,
                            "data": {"status": "completed"},
                            "path": f"/app/backend/data/uploads/{real_filename}",
                            "access_control": None
                        })
                        
                        # Remove markdown link from content
                        safe_id = re.escape(file_id)
                        pattern = r"\!?\[.*?\]\(.*?" + safe_id + r".*?\)"
                        content = re.sub(pattern, "", content)
                
                new_files_list.append(f)
                continue

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
                
                # Logic: Embed images, copy others
                is_image = mime_type.startswith("image/")
                
                try:
                    if is_image:
                        # Read and encode
                        with open(media_path, "rb") as image_file:
                            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
                        
                        data_uri = f"data:{mime_type};base64,{encoded_string}"
                        
                        # Create new file object compliant with OpenWebUI embedded format
                        new_file_obj = {
                            "type": "image",
                            "url": data_uri,
                            "name": filename
                        }
                    else:
                        # Copy to uploads
                        dst_path = os.path.join(uploads_dir, real_filename)
                        shutil.copy2(media_path, dst_path)
                        
                        file_size = os.path.getsize(media_path)
                        file_hash = compute_file_hash(media_path)
                        current_time = int(os.path.getmtime(media_path))
                        item_id = str(uuid.uuid4())
                        
                        # Create full file structure matching OpenWebUI format
                        new_file_obj = {
                            "type": "file",
                            "file": {
                                "id": file_id,
                                "user_id": user_id,
                                "hash": file_hash,
                                "filename": filename,
                                "data": {"status": "completed"},
                                "meta": {
                                    "name": filename,
                                    "content_type": mime_type,
                                    "size": file_size,
                                    "data": {}
                                },
                                "created_at": current_time,
                                "updated_at": current_time,
                                "status": True,
                                "path": f"/app/backend/data/uploads/{real_filename}",
                                "access_control": None
                            },
                            "id": file_id,
                            "url": f"/api/v1/files/{file_id}",
                            "name": filename,
                            "status": "uploaded",
                            "size": file_size,
                            "error": "",
                            "itemId": item_id
                        }
                        
                        # Add to file records for SQL INSERT
                        file_records.append({
                            "id": file_id,
                            "user_id": user_id,
                            "filename": filename,
                            "meta": {
                                "name": filename,
                                "content_type": mime_type,
                                "size": file_size,
                                "data": {},
                                "collection_name": f"file-{file_id}"
                            },
                            "created_at": current_time,
                            "updated_at": current_time,
                            "hash": file_hash,
                            "data": {"status": "completed"},
                            "path": f"/app/backend/data/uploads/{real_filename}",
                            "access_control": None
                        })

                    new_files_list.append(new_file_obj)
                    
                    # Remove markdown link from content
                    safe_id = re.escape(file_id)
                    pattern = r"\!?\[.*?\]\(.*?" + safe_id + r".*?\)"
                    content = re.sub(pattern, "", content)
                    
                except Exception as e:
                    print(f"Error processing file {media_path}: {e}")
                    new_files_list.append(f) # Keep original on error
            else:
                print(f"Warning: File not found {media_path}")
                new_files_list.append(f)

        # Update message in history.messages
        msg["files"] = new_files_list
        msg["content"] = content.strip()
        
        # Also update the corresponding message in messages array
        if msg_id in messages_list_lookup:
            messages_list_lookup[msg_id]["files"] = new_files_list
            messages_list_lookup[msg_id]["content"] = content.strip()
    
    return file_records


def build_file_sql(file_record: dict) -> str:
    """Generate SQL INSERT statement for a file record."""
    file_id = file_record["id"]
    user_id = file_record["user_id"]
    filename = escape_sql_string(file_record["filename"])
    meta_json = escape_sql_string(json.dumps(file_record["meta"], ensure_ascii=False))
    created_at = file_record["created_at"]
    updated_at = file_record["updated_at"]
    file_hash = file_record["hash"]
    data_json = escape_sql_string(json.dumps(file_record["data"], ensure_ascii=False))
    path = escape_sql_string(file_record["path"])
    
    return (
        f"DELETE FROM \"file\" WHERE \"id\" = '{file_id}';\n"
        "INSERT INTO \"file\" "
        "(\"id\",\"user_id\",\"filename\",\"meta\",\"created_at\",\"hash\",\"data\",\"updated_at\",\"path\",\"access_control\")\n"
        f"VALUES ('{file_id}','{user_id}','{filename}','{meta_json}',{created_at},'{file_hash}','{data_json}',{updated_at},'{path}','null');"
    )


def json_to_sql(path: str, tags: list[str], uploads_dir: str) -> tuple[str, str, list[str]]:
    data = load_json(path)
    
    # Handle list wrapper (OpenWebUI export format)
    if isinstance(data, list) and len(data) > 0:
        data = data[0]

    # Handle object wrapper with 'chat' key
    if "chat" in data and isinstance(data["chat"], dict):
        wrapper_user_id = data.get("user_id") or data.get("userId")
        data = data["chat"]
        # Ensure userId is available in the chat object
        if wrapper_user_id and not data.get("userId"):
            data["userId"] = wrapper_user_id

    user_id = data.get("userId")
    if not user_id:
        raise ValueError(f"userId missing in {path}")
        
    # Process files to embed them or move to uploads
    file_records = process_files(data, path, uploads_dir, user_id)
    
    # Generate file SQL statements
    file_sqls = [build_file_sql(fr) for fr in file_records]
    
    chat_json = json.dumps(data, ensure_ascii=False)
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
    return sql, user_id, file_sqls


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

    output_path = args.output or "input/chats.sql"
    
    # Ensure directories exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    uploads_dir = os.path.join(os.path.dirname(output_path), "uploads")
    os.makedirs(uploads_dir, exist_ok=True)

    files = gather_files(args.files)
    chat_inserts = []
    file_inserts = []
    user_ids: set[str] = set()
    for fpath in files:
        try:
            sql, uid, file_sqls = json_to_sql(fpath, tags, uploads_dir)
            chat_inserts.append(sql)
            file_inserts.extend(file_sqls)
            user_ids.add(uid)
        except Exception as exc:
            # raise SystemExit(f"Failed to process {fpath}: {exc}")
            print(f"Failed to process {fpath}: {exc}")

    prefix = []
    for uid in sorted(user_ids):
        prefix.extend(tag_upserts(uid, tags))

    # Combine: Tags -> Files -> Chats
    output = "\n".join(prefix + file_inserts + chat_inserts)
    
    with open(output_path, "w", encoding="utf-8-sig") as f:
        f.write(output + "\n")
    print(f"SQL written to {output_path}")


if __name__ == "__main__":
    main()
