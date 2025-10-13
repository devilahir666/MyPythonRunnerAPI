import os
from flask import Flask, Response
from pyrogram import Client
from pyrogram.errors import PeerIdInvalid, UserNotParticipant, AccessTokenInvalid

app = Flask(__name__)

# --- KEYS ARE READ SECURELY FROM RENDER ENVIRONMENT VARIABLES ---
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
STRING_SESSION = os.environ.get("STRING_SESSION") 
BOT_TOKEN = os.environ.get("BOT_TOKEN") 

# Pyrogram Client ko initialize karein (STRING_SESSION aur BOT_TOKEN dono use karke)
try:
    if not all([API_ID, API_HASH, STRING_SESSION, BOT_TOKEN]):
        print("CRITICAL ERROR: API_ID, API_HASH, STRING_SESSION, ya BOT_TOKEN missing hain.")
        exit(1)
        
    # 🔥 FINAL FIX: Bot mode ko ON karna aur BOT_TOKEN ko session_name mein use karna
    # BOT_TOKEN.split(":")[0] se hum Bot ID lete hain, jisse session file ka naam chota rahe.
    bot = Client(
        BOT_TOKEN.split(":")[0], # <--- Session file ka chota aur fixed naam (Bot ID)
        session_string=STRING_SESSION, # <--- String Session se login (Access ke liye)
        api_id=int(API_ID), 
        api_hash=API_HASH, 
        bot_token=BOT_TOKEN, 
        is_bot=True # <--- Yahi woh aakhri setting hai jo access confusion ko theek karti hai
    )
    bot.start() 
    print("Telegram Client Connected Successfully (Forced Bot Mode)!")
except Exception as e:
    print(f"Connection Error during bot.start(): {e}")
    exit(1)

# Simple home route
@app.route('/')
def home():
    return "Telegram Streaming Proxy is Running. Use /api/stream/<channel_id>/<message_id> to stream.", 200

# 🎯 File Streaming Endpoint
@app.route("/api/stream/<channel_id>/<int:message_id>")
def stream_file(channel_id, message_id):
    print(f"Request received for Channel: {channel_id}, Message: {message_id}")
    
    try:
        # CRITICAL FIX: get_chat se access confirm karenge
        bot.get_chat(channel_id) 
        
        message = bot.get_messages(channel_id, message_id)
        
        if not message.media or not message.document:
            return "404 Not Found: File not attached to this message.", 404

        file_name = message.document.file_name
        file_size = message.document.file_size
        mime_type = message.document.mime_type
        
        def generate():
            for chunk in bot.stream_media(message):
                yield chunk
        
        return Response(
            generate(),
            mimetype=mime_type or 'application/octet-stream', 
            headers={
                "Content-Disposition": f"attachment; filename=\"{file_name}\"",
                "Content-Length": str(file_size),
                "Accept-Ranges": "bytes"
            }
        )

    # Specific error handling for the access issues
    except (PeerIdInvalid, UserNotParticipant, AccessTokenInvalid) as e:
        print(f"CRITICAL ACCESS ERROR: {e}. Session failed.")
        return "500 Internal Server Error: Session failed. Check STRING_SESSION/Bot Access.", 500
    
    except Exception as e:
        print(f"Unforeseen Error during streaming: {e}")
        return "500 Internal Server Error: Could not process file.", 500

# Yeh function Gunicorn run karega
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=os.environ.get("PORT", 5000))
    
