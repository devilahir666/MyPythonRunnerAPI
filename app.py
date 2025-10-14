import os
import re
from flask import Flask, Response, request
from pyrogram import Client as SyncClient 
from pyrogram.errors import FileReferenceExpired, RPCError

app = Flask(__name__)

# --- HARDCODED KEYS (GitHub par mat daalna! Secrets use honge) ---
# NOTE: Render par deploy karte waqt, yeh values Render Secrets mein daalni hain, 
# code mein nahi. Lekin abhi testing ke liye rakh rahe hain.
API_ID = 23692613
API_HASH = "8bb69956d38a8226433186a199695f57"
STRING_SESSION = "BQFphUUAHgXd4GHGzpOXFKX1kJ-ScusfrHGvhk_cbLGN5DDa9-IMe08WtUU1pzFuz1DGy9jnwMMsJo2FnUZSQHtXNsaBm-UFA22ZqN4htnHk4-qdkACNpeTXayIcvETMsH97WLERuVr9t9NJTMpVkg4zD57b4CkmLljxqwt81_WQS99wTKzW7uDj412nIFudlHddsqDyiw2aXKM8Ar1yPKXUkpf_xTfrzUEKkrVppdTRdattUqahoq0zrlAUYxUCB-iTipGWJDeDrszD_QsOQs9p2F9wYP44s6Zx9nznRXcv0EmO0MHjH-Zmew6yHcBAcu_r0-b7wShXytzzJ9EySBRul_2KwAAAAHq-0sgAA"
BOT_TOKEN = "8075063062:AAH8lWaA7yk6ucGnV7N5F_U87nR9FRwKv98"

# Pyrogram Sync Client
try:
    if not all([API_ID, API_HASH, STRING_SESSION, BOT_TOKEN]):
        print("CRITICAL ERROR: Keys are missing or empty.")
        exit(1)
        
    bot = SyncClient(
        BOT_TOKEN.split(":")[0],
        session_string=STRING_SESSION, 
        api_id=int(API_ID), 
        api_hash=API_HASH, 
        bot_token=BOT_TOKEN
    )
    bot.start() 
    print("Telegram Client Connected Successfully!")
except Exception as e:
    print(f"Connection Error during bot.start(): {e}")
    exit(1)


@app.route('/')
def home():
    return "Telegram Streaming Proxy is Running. Use /api/stream/<chat_id>/<message_id> to stream.", 200

# Chat ID aur Message ID se stream (String ID supported)
@app.route("/api/stream/<chat_id>/<message_id>")
def stream_file_by_id(chat_id, message_id):
    print(f"Request received for Chat ID: {chat_id}, Message ID: {message_id}")
    
    try:
        # File Details nikaalo
        message = bot.get_messages(
            chat_id=chat_id, # String (@username) support
            message_ids=int(message_id)
        )
        
        if not message or (not message.video and not message.document):
            return "404 Error: File not found or message does not contain a file.", 404
        
        file_info = message.video or message.document
        file_name = file_info.file_name or "streaming_file.mkv"
        file_size = file_info.file_size
        mime_type = file_info.mime_type or 'video/x-matroska'
        
        # Range Handling
        range_header = request.headers.get('Range')
        start_byte, end_byte = 0, file_size - 1
        status_code = 200
        headers = {
            "Content-Type": mime_type,
            "Content-Disposition": f"inline; filename=\"{file_name}\"",
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size) # Size ab mil gayi hai
        }

        if range_header:
            range_match = re.search(r'bytes=(\d+)-(\d*)', range_header)
            if range_match:
                start_byte = int(range_match.group(1))
                if range_match.group(2):
                    end_byte = int(range_match.group(2))
                
                status_code = 206 
                content_length = end_byte - start_byte + 1
                headers['Content-Length'] = str(content_length)
                headers['Content-Range'] = f'bytes {start_byte}-{end_byte}/{file_size}'
        
        # File ko stream karne ka generator function
        def generate():
            offset = start_byte
            limit = end_byte - start_byte + 1 if end_byte is not None else file_size - start_byte
            
            try:
                for chunk in bot.stream_media(
                    message, 
                    offset=offset, 
                    limit=limit
                ): 
                    yield chunk
            except Exception as e:
                print(f"Stream generation error: {e}")
                raise
        
        return Response(
            generate(),
            status=status_code,
            headers=headers
        )
        
    except Exception as e:
        print(f"Error in stream_file_by_id: {e}")
        return "500 Internal Server Error: Streaming failed.", 500


# Render/Heroku mein gunicorn se chalaenge
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=os.environ.get("PORT", 8080))
    
