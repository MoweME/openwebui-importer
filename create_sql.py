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
import hashlib
import sys
import ijson
import mmap
from tqdm import tqdm
from typing import Generator, Any, Tuple, List, Set, Dict

# --- Helper Classes ---

class LazyImage:
    """Delays loading of image data until JSON serialization."""
    def __init__(self, path: str, mime_type: str):
        self.path = path
        self.mime_type = mime_type

    def to_data_uri(self) -> str:
        try:
            with open(self.path, "rb") as image_file:
                encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
            return f"data:{self.mime_type};base64,{encoded_string}"
        except Exception as e:
            sys.stderr.write(f"Error reading image {self.path}: {e}\n")
            return "" # Or handle gracefully

class CustomJSONEncoder(json.JSONEncoder):
    """Encodes LazyImage objects to their data URI string representation."""
    def default(self, obj):
        if isinstance(obj, LazyImage):
            return obj.to_data_uri()
        return super().default(obj)

# --- Helper Functions ---

def escape_sql_string(value: str) -> str:
    if not isinstance(value, str):
        return str(value)
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
    """Compute SHA256 hash of a file using mmap for memory efficiency."""
    if not os.path.exists(filepath):
        return ""
        
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            # Handle empty files
            if os.path.getsize(filepath) == 0:
                return sha256_hash.hexdigest()
                
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmapped_file:
                for i in range(0, len(mmapped_file), 8192):
                    sha256_hash.update(mmapped_file[i:i+8192])
    except Exception as e:
        sys.stderr.write(f"Error computing hash for {filepath}: {e}\n")
        # Fallback to standard read if mmap fails
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
                
    return sha256_hash.hexdigest()

def process_files(data: dict, json_path: str, uploads_dir: str, user_id: str, embed_images: bool = True) -> list[dict]:
    """
    Process files in chat history with memory optimization.
    Returns list of file records for SQL INSERT.
    """
    messages_map = data.get("history", {}).get("messages", {})
    messages_list = data.get("messages", [])
    file_records = []
    
    # Create lookup for messages_list by id
    messages_list_lookup = {m.get("id"): m for m in messages_list}
    
    # Identify keys to process to avoid iterating everything if possible
    # We need to update both map and list if they exist
    
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
                        
                        # Remove markdown link
                        safe_id = re.escape(file_id)
                        pattern = r"\!?\[.*?\]\(.*?" + safe_id + r".*?\)"
                        content = re.sub(pattern, "", content)
                
                new_files_list.append(f)
                continue

            file_id = f.get("id")
            filename = f.get("name")
            
            if not file_id or not filename:
                new_files_list.append(f)
                continue

            real_filename = f"{file_id}_{filename}"
            media_path = os.path.join(os.path.dirname(json_path), "media", real_filename)
            
            # Fallback
            if not os.path.exists(media_path):
                media_path_alt = os.path.join(os.path.dirname(json_path), "media", filename)
                if os.path.exists(media_path_alt):
                    media_path = media_path_alt
            
            if os.path.exists(media_path):
                mime_type, _ = mimetypes.guess_type(media_path)
                if not mime_type:
                    mime_type = "application/octet-stream"
                
                is_image = mime_type.startswith("image/")
                
                try:
                    if is_image and embed_images:
                        # Use LazyImage to delay loading
                        new_file_obj = {
                            "type": "image",
                            "url": LazyImage(media_path, mime_type),
                            "name": filename
                        }
                    else:
                        # Copy to uploads
                        dst_path = os.path.join(uploads_dir, real_filename)
                        if not os.path.exists(dst_path):
                            shutil.copy2(media_path, dst_path)
                        
                        file_size = os.path.getsize(media_path)
                        file_hash = compute_file_hash(media_path)
                        current_time = int(os.path.getmtime(media_path))
                        item_id = str(uuid.uuid4())
                        
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
                    
                    safe_id = re.escape(file_id)
                    pattern = r"\!?\[.*?\]\(.*?" + safe_id + r".*?\)"
                    content = re.sub(pattern, "", content)
                    
                except Exception as e:
                    sys.stderr.write(f"Error processing file {media_path}: {e}\n")
                    new_files_list.append(f)
            else:
                sys.stderr.write(f"Warning: File not found {media_path}\n")
                new_files_list.append(f)

        msg["files"] = new_files_list
        msg["content"] = content.strip()
        
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

def process_single_conversation(data: dict, json_path: str, tags: list[str], uploads_dir: str, embed_images: bool) -> Tuple[str, str, List[str]]:
    """Process a single conversation object and return SQL statements."""
    
    # Handle object wrapper with 'chat' key (common in some exports)
    if "chat" in data and isinstance(data["chat"], dict):
        wrapper_user_id = data.get("user_id") or data.get("userId")
        data = data["chat"]
        if wrapper_user_id and not data.get("userId"):
            data["userId"] = wrapper_user_id

    user_id = data.get("userId")
    if not user_id:
        # If no user_id, warn and skip or use placeholder?
        # sys.stderr.write(f"Warning: userId missing in conversation, using 'unknown'\n")
        return "", "", []
        
    file_records = process_files(data, json_path, uploads_dir, user_id, embed_images)
    file_sqls = [build_file_sql(fr) for fr in file_records]
    
    # Use CustomJSONEncoder to handle LazyImage serialization
    chat_json = json.dumps(data, ensure_ascii=False, cls=CustomJSONEncoder)
    chat_json = escape_sql_string(chat_json)

    title = escape_sql_string(data.get("title", ""))
    timestamp_ms = data.get("timestamp", 0)
    created_at = int(int(timestamp_ms) / 1000)

    # Determine record ID
    # Try to extract from path if possible, or use data ID?
    # Original logic used filename. Here we might need to rely on ID in data or generate new.
    # Since we are streaming, we might not rely on filename for ID if inside a list.
    # But we have json_path.
    
    record_id = data.get("id")
    if not record_id:
         # Fallback to logic based on path if available, or random
         base = os.path.splitext(os.path.basename(json_path))[0]
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

def process_file_path(path: str, tags: list[str], uploads_dir: str, output_file, embed_images: bool, batch_size: int, pbar: tqdm = None) -> Set[str]:
    """Reads a file (which can be a list or dict) and writes SQL to output."""
    user_ids = set()
    
    try:
        # Detect if file is list or dict
        # We can use ijson to stream items if it's a list.
        # If it's a dict, we just load it (assuming single dict fits in memory).
        
        # Check first char
        is_list = False
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(1024)
                if not chunk: break
                s = chunk.strip()
                if not s: continue
                if s.startswith(b'['):
                    is_list = True
                break
        
        # Helper to wrap file for tqdm
        # We only wrap if we have a pbar provided
        def get_file_ctx():
            if pbar:
                return tqdm.wrapattr(open(path, 'rb'), "read", total=os.path.getsize(path), file=pbar)
            return open(path, 'rb')

        # Since we use one global pbar, we can't easily use wrapattr for multiple files unless we update the global one.
        # Instead, let's create a wrapper class that updates the pbar manually.
        
        class ProgressFile:
            def __init__(self, path, pbar):
                self.f = open(path, 'rb')
                self.pbar = pbar
                
            def read(self, size=-1):
                data = self.f.read(size)
                if self.pbar:
                    self.pbar.update(len(data))
                return data
                
            def close(self):
                self.f.close()
                
            def __enter__(self):
                return self
                
            def __exit__(self, exc_type, exc_val, exc_tb):
                self.close()

        if is_list:
            with ProgressFile(path, pbar) as f:
                # Iterate over items in the list
                # ijson.items(f, 'item') parses each item in the top-level list
                items = ijson.items(f, 'item')
                count = 0
                for data in items:
                    sql, uid, file_sqls = process_single_conversation(data, path, tags, uploads_dir, embed_images)
                    if sql:
                        # Write Files SQL first
                        for fsql in file_sqls:
                            output_file.write(fsql + "\n")
                        output_file.write(sql + "\n")
                        user_ids.add(uid)
                        count += 1
                        
                        if count % batch_size == 0:
                            output_file.flush()
                            if pbar:
                                pbar.set_description(f"Processing (convs: {count})")
        else:
            # Assume single object
            # For single object, ijson works but json.load is simpler if it fits. 
            # But we want progress. Let's stick to reading with ProgressFile and json.load
            # json.load accepts a file-like object with .read()
            with ProgressFile(path, pbar) as f:
                data = json.load(f)
            
            # Handle the case where it might be wrapped in a list but short (old behavior)
            if isinstance(data, list) and len(data) > 0:
                data = data[0] # Original script behavior for lists
            
            sql, uid, file_sqls = process_single_conversation(data, path, tags, uploads_dir, embed_images)
            if sql:
                for fsql in file_sqls:
                    output_file.write(fsql + "\n")
                output_file.write(sql + "\n")
                user_ids.add(uid)
                output_file.flush()

    except Exception as e:
        sys.stderr.write(f"Failed to process {path}: {e}\n")
        import traceback
        traceback.print_exc()

    return user_ids

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
    parser = argparse.ArgumentParser(description="Create SQL inserts for open-webui chats (Optimized)")
    parser.add_argument("files", nargs="+", help="Chat JSON files or directories")
    parser.add_argument("--tags", default="imported", help="Comma-separated tags for the meta field")
    parser.add_argument("--output", help="Write SQL statements to this file", default="input/chats.sql")
    parser.add_argument("--batch-size", type=int, default=50, help="Flush to disk after N conversations")
    parser.add_argument("--no-embed-images", action="store_true", help="Do not embed images as base64")
    # Added for compatibility with plan, though we always try to be memory efficient now
    parser.add_argument("--low-memory", action="store_true", help="Use memory optimized processing (default behavior now)")
    
    args = parser.parse_args()

    tags = [t.strip() for t in args.tags.split(',') if t.strip()] or ["imported"]
    output_path = args.output
    
    # Ensure directories exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    uploads_dir = os.path.join(os.path.dirname(output_path), "uploads")
    os.makedirs(uploads_dir, exist_ok=True)

    files = gather_files(args.files)
    all_user_ids = set()
    
    # Calculate total size
    total_size = sum(os.path.getsize(f) for f in files if os.path.exists(f))
    
    print(f"Processing {len(files)} files ({total_size / (1024*1024):.2f} MB) to {output_path}...")
    
    # Open output file once
    with open(output_path, "w", encoding="utf-8-sig") as out_f:
        # Use a single progress bar for all files
        with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
            for fpath in files:
                pbar.set_description(f"Processing {os.path.basename(fpath)}")
                uids = process_file_path(
                    fpath, 
                    tags, 
                    uploads_dir, 
                    out_f, 
                    not args.no_embed_images, 
                    args.batch_size,
                    pbar
                )
                all_user_ids.update(uids)

        # Write tag upserts at the end
        if all_user_ids:
            out_f.write("\n-- Tag Upserts --\n")
            for uid in sorted(all_user_ids):
                stmts = tag_upserts(uid, tags)
                for stmt in stmts:
                    out_f.write(stmt + "\n")

    print(f"\nSQL written to {output_path}")

if __name__ == "__main__":
    main()
