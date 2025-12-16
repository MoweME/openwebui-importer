#!/usr/bin/env python3
"""Convert ChatGPT exports to open-webui JSON."""

import argparse
import json
import os
import re
import shutil
import mimetypes
import base64
from typing import Any, Dict, List, Tuple
import time
import uuid
from datetime import datetime

INVALID_RE = re.compile(r"[\ue000-\uf8ff]")


def sanitize_text(text: Any) -> str:
    """Return ``text`` without private-use Unicode characters."""
    if not isinstance(text, str):
        return ""
    return INVALID_RE.sub("", text)


MODEL = "openai/GPT-5"
MODEL_NAME = "OpenAI: GPT-5"
SUBDIR = "chatgpt"


def extract_last_sentence(text: Any) -> str:
    """Return the last sentence of ``text`` if it is a string."""
    if not isinstance(text, str):
        return ""
    cleaned = text.strip()
    if not cleaned:
        return ""
    matches = re.findall(r"[^.!?]*[.!?]", cleaned, flags=re.DOTALL)
    if matches:
        return matches[-1].strip()
    lines = [ln.strip() for ln in cleaned.splitlines() if ln.strip()]
    return lines[-1] if lines else cleaned


def _parts_to_text(parts: List[Any], assets_mapping: Dict[str, str] = None, export_dir: str = None, media_dir: str = None, media_url_prefix: str = "media") -> Tuple[str, List[Dict[str, Any]]]:
    """Return concatenated text and list of files from ChatGPT message parts."""
    texts: List[str] = []
    files: List[Dict[str, Any]] = []
    
    for part in parts:
        if isinstance(part, str):
            texts.append(sanitize_text(part))
        elif isinstance(part, dict):
            if "text" in part:
                val = part.get("text")
                if isinstance(val, str):
                    texts.append(sanitize_text(val))
            else:
                # Check for various asset pointer types
                asset_pointer = None
                content_type = part.get("content_type", "")
                
                # Generic asset pointer detection
                if "asset_pointer" in content_type or "multimodal" in content_type:
                    # Try to find asset pointer in common keys
                    keys_to_check = [
                        "asset_pointer",
                        "image_asset_pointer",
                        "file_asset_pointer", 
                        "document_asset_pointer",
                        "audio_asset_pointer",
                        "video_asset_pointer"
                    ]
                    
                    for key in keys_to_check:
                        val = part.get(key)
                        if isinstance(val, str):
                            asset_pointer = val
                            break
                        elif isinstance(val, dict):
                            # Handle nested structure like { "asset_pointer": "..." }
                            if "asset_pointer" in val:
                                asset_pointer = val["asset_pointer"]
                                break

                # Fallback: check if asset_pointer key exists directly if not found yet
                if not asset_pointer and "asset_pointer" in part:
                    asset_pointer = part["asset_pointer"]

                if asset_pointer:
                    filename = None
                    if assets_mapping:
                        filename = assets_mapping.get(asset_pointer)
                    
                    # Fallback: search in export_dir if not found in mapping
                    if not filename and export_dir:
                        # Strip common prefixes to get the ID
                        # Handles sediment:// and file-service://
                        asset_id = asset_pointer.replace("sediment://", "").replace("file-service://", "")
                        
                        # Search recursively in export_dir
                        for root, _, fs in os.walk(export_dir):
                            for f in fs:
                                if asset_id in f:
                                    # Found a match, use relative path from export_dir
                                    abs_path = os.path.join(root, f)
                                    filename = os.path.relpath(abs_path, export_dir)
                                    break
                            if filename:
                                break

                    if filename:
                        if export_dir:
                            src = os.path.join(export_dir, filename)
                            
                            # Ensure source exists
                            if os.path.exists(src):
                                file_id = str(uuid.uuid4())
                                original_name = os.path.basename(filename)
                                mime_type, _ = mimetypes.guess_type(original_name)
                                file_size = os.path.getsize(src)

                                # Check if it is an image
                                is_image = mime_type and mime_type.startswith("image/")
                                
                                if is_image:
                                    with open(src, "rb") as image_file:
                                        encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
                                    
                                    data_uri = f"data:{mime_type};base64,{encoded_string}"
                                    
                                    files.append({
                                        "id": file_id,
                                        "name": original_name,
                                        "type": "image",
                                        "url": data_uri,
                                        "meta": {
                                            "name": original_name,
                                            "content_type": mime_type,
                                            "size": file_size,
                                        },
                                        "data": {
                                            "status": "completed"
                                        }
                                    })
                                    # Do NOT append markdown link for embedded images
                                    
                                elif media_dir:
                                    # Non-image files: copy to media_dir
                                    new_filename = f"{file_id}_{original_name}"
                                    dst = os.path.join(media_dir, new_filename)
                                    shutil.copy2(src, dst)
                                    
                                    files.append({
                                        "id": file_id,
                                        "name": original_name,
                                        "meta": {
                                            "name": original_name,
                                            "content_type": mime_type,
                                            "size": file_size,
                                        },
                                        "data": {
                                            "status": "completed"
                                        }
                                    })

                                    # Use forward slashes for URLs
                                    url_path = f"{media_url_prefix}/{new_filename}".replace("\\", "/")
                                    if content_type == "audio_asset_pointer":
                                        texts.append(f"\n[Audio: {original_name}]({url_path})\n")
                                    else:
                                        texts.append(f"\n[Media: {original_name}]({url_path})\n")
                            else:
                                texts.append(f"\n[Media not found: {filename}]\n")
                        else:
                            texts.append(f"\n[Media: {filename}]\n")
                    else:
                        texts.append(f"\n[Media: {asset_pointer}]\n")
    return "".join(texts), files


def parse_timestamp(value: Any, default: float) -> float:
    """Convert ``value`` to a Unix timestamp."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    return default


def parse_chatgpt(data: Any, assets_mapping: Dict[str, str] = None, export_dir: str = None, media_dir: str = None, media_url_prefix: str = "media") -> List[dict]:
    conversations = data if isinstance(data, list) else [data]
    result = []
    for item in conversations:
        if not isinstance(item, dict):
            continue
        title = item.get("title") or item.get("name") or "Untitled"
        ts_raw = item.get("create_time") or item.get("update_time") or time.time()
        ts = parse_timestamp(ts_raw, time.time())
        conv_id = item.get("conversation_id") or item.get("id")
        messages: List[Tuple[str, str, float, List[Dict]]] = []
        if isinstance(item.get("chat_messages"), list):
            for idx, msg in enumerate(item["chat_messages"]):
                text = msg.get("text")
                msg_files = []
                if not text and isinstance(msg.get("content"), list):
                    text, msg_files = _parts_to_text(msg["content"], assets_mapping, export_dir, media_dir, media_url_prefix)
                text = sanitize_text(text)
                if text:
                    role = "user" if idx % 2 == 0 else "assistant"
                    messages.append((role, text, ts, msg_files))
        elif isinstance(item.get("mapping"), dict):
            mapping = item["mapping"]
            node = None
            current_id = item.get("current_node")
            if current_id and isinstance(mapping.get(current_id), dict):
                node = mapping[current_id]
                stack: List[Tuple[str, str, float, List[Dict]]] = []
                while isinstance(node, dict):
                    msg = node.get("message") or {}
                    parts = msg.get("content", {}).get("parts", [])
                    if parts:
                        role = msg.get("author", {}).get("role", "assistant")
                        if role == "tool":
                            role = "assistant"
                        if role in {"user", "assistant"}:
                            ts_val = msg.get("create_time") or msg.get("timestamp") or ts
                            text, msg_files = _parts_to_text(parts, assets_mapping, export_dir, media_dir, media_url_prefix)
                            text = sanitize_text(text)
                            if text:
                                stack.append((role, text, parse_timestamp(ts_val, ts), msg_files))
                    parent_id = node.get("parent")
                    if not parent_id:
                        break
                    node = mapping.get(parent_id)
                messages.extend(reversed(stack))
            else:
                node = mapping.get("client-created-root")
                if not isinstance(node, dict):
                    # Some exports don't use the "client-created-root" key. In
                    # those cases, attempt to locate the root node by finding the
                    # entry with no parent value.
                    for val in mapping.values():
                        if isinstance(val, dict) and not val.get("parent"):
                            node = val
                            break
                if isinstance(node, dict):
                    next_ids = node.get("children") or []
                    while next_ids:
                        node = mapping.get(next_ids[0])
                        if not isinstance(node, dict):
                            break
                        msg = node.get("message") or {}
                        parts = msg.get("content", {}).get("parts", [])
                        if parts:
                            role = msg.get("author", {}).get("role", "assistant")
                            if role in {"user", "assistant"}:
                                ts_val = msg.get("create_time") or msg.get("timestamp") or ts
                                text, msg_files = _parts_to_text(parts, assets_mapping, export_dir, media_dir, media_url_prefix)
                                text = sanitize_text(text)
                                if text:
                                    messages.append((role, text, parse_timestamp(ts_val, ts), msg_files))
                        next_ids = node.get("children") or []
        else:
            messages.append(("user", title, ts, []))
        result.append({
            "title": title,
            "timestamp": ts,
            "messages": messages,
            "conversation_id": conv_id,
        })
    return result


def build_webui(conversation: dict, user_id: str) -> Tuple[Dict[str, Any], str]:
    conv_uuid = str(uuid.uuid4())
    messages_map: Dict[str, Any] = {}
    messages_list: List[Dict[str, Any]] = []
    prev_id: str | None = None
    for role, content, ts, msg_files in conversation["messages"]:
        msg_id = str(uuid.uuid4())
        clean = sanitize_text(content)
        msg = {
            "id": msg_id,
            "parentId": prev_id,
            "childrenIds": [],
            "role": role,
            "content": clean,
            "timestamp": int(ts),
            "files": msg_files
        }
        if role == "user":
            msg["models"] = [MODEL]
        else:
            msg.update(
                {
                    "model": MODEL,
                    "modelName": MODEL_NAME,
                    "modelIdx": 0,
                    "userContext": None,
                    "lastSentence": extract_last_sentence(clean),
                    "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                    "done": True,
                }
            )
        if prev_id:
            messages_map[prev_id]["childrenIds"].append(msg_id)
        messages_map[msg_id] = msg
        messages_list.append(msg)
        prev_id = msg_id
    webui = {
        "id": "",
        "title": conversation["title"],
        "models": [MODEL],
        "params": {},
        "history": {"messages": messages_map, "currentId": prev_id},
        "messages": messages_list,
        "tags": [],
        "timestamp": int(conversation["timestamp"] * 1000),
        "files": [],
    }
    if user_id:
        webui["userId"] = user_id
    return webui, conv_uuid


def slugify(text: Any) -> str:
    if not isinstance(text, str):
        text = str(text)
    text = re.sub(r"\s+", "_", text.strip())
    text = re.sub(r"[^a-zA-Z0-9_\-]", "", text)
    return text[:50] or "chat"


def parse_assets_mapping(export_dir: str) -> Dict[str, str]:
    """Parse chat.html to extract assets mapping."""
    chat_html_path = os.path.join(export_dir, "chat.html")
    if not os.path.exists(chat_html_path):
        return {}
    
    try:
        with open(chat_html_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        # Try to find the variable assignment directly
        start_marker = "var assetsJson = "
        start = content.find(start_marker)
        if start != -1:
            start += len(start_marker)
            try:
                decoder = json.JSONDecoder()
                mapping, _ = decoder.raw_decode(content[start:])
                return mapping
            except json.JSONDecodeError:
                pass
    except Exception as e:
        print(f"Warning: Failed to parse assets from chat.html: {e}")
    
    return {}


def convert_file(path: str, user_id: str, outdir: str, media_url_prefix: str) -> None:
    export_dir = os.path.dirname(path)
    assets_mapping = parse_assets_mapping(export_dir)
    media_dir = os.path.join(outdir, "media")
    
    if assets_mapping:
        os.makedirs(media_dir, exist_ok=True)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    conversations = parse_chatgpt(data, assets_mapping, export_dir, media_dir, media_url_prefix)
    os.makedirs(outdir, exist_ok=True)
    for conv in conversations:
        out, conv_uuid = build_webui(conv, user_id)
        conv_id = conv.get("conversation_id")
        unique = conv_id if conv_id else conv_uuid
        fname = f"{slugify(conv['title'])}_{unique}.json"
        
        # Wrap in expected OpenWebUI import format
        wrapped = [
            {
                "id": "",
                "user_id": user_id,
                "title": out.get("title", "Untitled"),
                "chat": out
            }
        ]
        
        with open(os.path.join(outdir, fname), "w", encoding="utf-8") as fh:
            json.dump(wrapped, fh, ensure_ascii=False, indent=2)


def run_cli() -> None:
    parser = argparse.ArgumentParser(description="Convert ChatGPT exports to open-webui JSON")
    parser.add_argument("files", nargs="+", help="ChatGPT export JSON files")
    parser.add_argument("--userid", required=True, help="User ID for output files")
    parser.add_argument("--output-dir", default="output", help="Directory for output JSON files")
    parser.add_argument("--media-url-prefix", default="media", help="URL prefix for media files in Markdown links")
    args = parser.parse_args()
    outdir = os.path.join(args.output_dir, SUBDIR)
    for path in args.files:
        try:
            convert_file(path, args.userid, outdir, args.media_url_prefix)
        except Exception as exc:
            print(f"Failed to convert {path}: {exc}")


if __name__ == "__main__":
    run_cli()
