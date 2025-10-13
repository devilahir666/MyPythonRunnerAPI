import os
from flask import Flask, Response
from pyrogram import Client
from pyrogram.errors import PeerIdInvalid, UserNotParticipant, AccessTokenInvalid, FileIdInvalid

app = Flask(__name__)

# --- KEYS ARE READ SECURELY FROM RENDER ENVIRONMENT VARIABLES ---
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
STRING_SESSION = os.environ.get("STRING_SESSION") 
BOT_TOKEN = os.environ.get("BOT_TOKEN") 

# Pyrogram Client ko initialize karein
try:
    if not all([API_ID, API_HASH, STRING_SESSION, BOT_TOKEN]):
        print("CRITICAL ERROR: API_ID, API_HASH, STRING_SESSION, ya BOT_TOKEN missing hain.")
        exit(1)
        
    # Final Client Initialization: STRING_SESSION aur BOT_TOKEN dono ka use
    bot = Client(
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

# Simple home route
@app.route('/')
def home():
    return "Telegram Streaming Proxy is Running. Use /api/stream/<file_id> to stream.", 200

# 🎯 FINAL FILE STREAMING ENDPOINT (Uses FILE_ID directly)
@app.route("/api/stream/<file_id>")
def stream_file_by_id(file_id):
    print(f"Request received for File ID: {file_id}")
    
    try:
        # 1. File Object ko seedhe FILE_ID se lein (Ismein file_name aur size mil jaayega)
        file_object = bot.get_file(file_id) 

        file_name = file_object.file_name if file_object.file_name else "file.bin"
        file_size = file_object.file_size
        mime_type = file_object.mime_type if file_object.mime_type else 'application/octet-stream'
        
        # 2. File object ko stream karein
        def generate():
            # bot.stream_media File object ya File ID string dono le sakta hai
            for chunk in bot.stream_media(file_object):
                yield chunk
        
        # 3. HTTP Response set karein
        return Response(
            generate(),
            mimetype=mime_type, 
            headers={
                "Content-Disposition": f"attachment; filename=\"{file_name}\"",
                "Content-Length": str(file_size),
                "Accept-Ranges": "bytes"
            }
        )

    # Specific error handling for the file ID issues
    except FileIdInvalid as e:
        print(f"CRITICAL FILE ID ERROR: File ID galat hai ya expired hai. {e}")
        return "400 Bad Request: File ID galat hai ya expired ho gayi hai.", 400

    except Exception as e:
        print(f"Unforeseen Error during streaming: {e}")
        return "500 Internal Server Error: Streaming failed.", 500

# Yeh function Gunicorn run karega
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=os.environ.get("PORT", 5000))
    
