# openwebui-importer

**Import Grok, Claude and ChatGPT chats into [open-webui](https://github.com/open-webui/open-webui).**

This importer tool has two Python scripts: one for converting the model JSON files to openweb-ui format JSON, the second for 
creating a SQL script to import the JSON into the openweb-ui SQLite database.  

The imported chats are given the tags `imported-chatgpt`, `imported-claude` and `imported-grok`.

Any private-use Unicode characters occasionally found in model exports are stripped from the message text during conversion.

*There were problems exporting chats from Gemini, so it's not currently supported. DeepSeek and others could be added without much effort.*

## Quick start

```
python convert_chatgpt.py --userid="get-this-from-your-webui.db" ./your-gpt-export/conversations.json
python create_sql.py ./output/chatgpt --tags="imported-chatgpt"
```

## Quickstart Docker

```bash
docker run --rm -v $(pwd)/data:/data \
  ghcr.io/moweme/openwebui-importer:latest \
  python convert_chatgpt.py --userid="get-this-from-your-webui.db" /data/your-gpt-export/conversations.json

docker run --rm -v $(pwd)/data:/data \
  ghcr.io/moweme/openwebui-importer:latest \
  python create_sql.py /data/output/chatgpt --tags="imported-chatgpt"
```

```powershell
docker run --rm -v ${PWD}/data:/data `
  ghcr.io/moweme/openwebui-importer:latest `
  python convert_chatgpt.py python convert_chatgpt.py --userid="get-this-from-your-webui.db" /data/your-gpt-export/conversations.json

docker run --rm -v ${PWD}/data:/data `
  ghcr.io/moweme/openwebui-importer:latest `
  python create_sql.py /data/output/chatgpt --tags="imported-chatgpt"
```

Full example for GPT:

```
python .\convert_chatgpt.py --userid="example-9cef-4387-8ee4-b82eb2e1c637" .\chatgpt.json   
python .\create_sql.py ./output/chatgpt --tags="imported-chatgpt"
# Now run the scripts inside DB Browser, migrate "upload" files and hit save
```

## Scripts

Install the required Python dependencies first:

```bash
pip install -r requirements.txt
```

### convert_chatgpt.py

```
usage: convert_chatgpt.py [-h] --userid USERID [--output-dir OUTPUT_DIR] [--media-url-prefix MEDIA_URL_PREFIX] files [files ...]

Convert ChatGPT exports to open-webui JSON
```

The `--media-url-prefix` argument allows you to specify the URL prefix for media files in the generated Markdown links. This is useful if you are hosting the media files on a separate server or if you are mounting the media directory to a specific path in OpenWebUI. Default is `media`.

### convert_grok.py

```
usage: convert_grok.py [-h] --userid USERID [--output-dir OUTPUT_DIR] files [files ...]

Convert Grok exports to open-webui JSON
```

### convert_claude.py

```
usage: convert_claude.py [-h] --userid USERID [--output-dir OUTPUT_DIR] files [files ...]

Convert Claude exports to open-webui JSON
```

All converter scripts name the output files using the original conversation ID
so running them again will produce the same filename for the same conversation.
Converted files are saved in a subdirectory named after the model (for example
`output/grok` or `output/claude`).

### create_sql.py

```
usage: create_sql.py [-h] [--tags TAGS] [--output OUTPUT] files [files ...]

Create SQL inserts for open-webui chats. Existing chat records are deleted
before inserting so they are replaced if already present. Tags are inserted
with UPSERT statements, ensuring the default import tags (and any tags passed
via `--tags`) exist for each user.

positional arguments:
  files            Chat JSON files or directories

options:
  -h, --help       show this help message and exit
  --tags TAGS      Comma-separated tags for the meta field
  --output OUTPUT  Write SQL statements to this file
```

## Example workflow

1. Create an export from Claude, ChatGPT or Grok.
2. Unzip the archive and locate the JSON file (for ChatGPT this is `conversations.json`).
3. Convert the export to open-webui JSON using the appropriate script:
   ```bash
   python convert_chatgpt.py --userid="<your openwebui userid> " ./chatgpt-export/conversations.json
   ```
   The converter writes JSON files to a subdirectory such as `output/chatgpt`.

   **For ChatGPT exports with media:**
   If your export contains media files (images, audio), the script will automatically extract them to a `media` subdirectory within the output folder (e.g., `output/chatgpt/media`).
   
   The files are renamed with a UUID prefix (e.g., `550e8400-e29b-41d4-a716-446655440000_image.png`) to match OpenWebUI's storage convention and avoid conflicts. The generated JSON includes file metadata compatible with OpenWebUI.

   To ensure these files are accessible in OpenWebUI, you should run the create_sql.py in order to generate the needed SQL statements to import the media files into OpenWebUI.

4. Generate SQL statements from the converted JSON files:
   ```bash
   python create_sql.py ./output/chatgpt
   ```

   Creates a new folder called `input` which contains the ready-to-import SQL file and prepared media files.

   Note: OpenWebUI saves most images in the database. The created uploads folder may not contain these images.
   They're included in the generated SQL file.

   The resulting SQL removes any existing chats with the same IDs before
   inserting new ones, while tags are inserted using UPSERTs so they are
   updated if they already exist. Any tags passed with `--tags` are also created
   for each user.

5. Make a copy of your `webui.db` database.
6. Stop your running OpenWebUI instance
7. Execute the generated SQL using a tool such as [DB Browser for SQLite](https://sqlitebrowser.org/dl/) or [HeidiSQL](https://www.heidisql.com/download.php).
   Ensure you save the database.
   
   Note: HeidiSQL is much faster and more reliable for large SQL files!
8. Copy the contents of the `input/uploads` into OpenWebUI's `uploads` directory.
9. Start your OpenWebUI instance again.