import os
from flask import Flask, Response
from pyrogram import Client
from pyrogram.errors import PeerIdInvalid, UserNotParticipant, AccessTokenInvalid

app = Flask(__name__)

# --- KEYS ARE READ SECURELY FROM RENDER ENVIRONMENT VARIABLES ---
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
STRING_SESSION = os.environ.get("STRING_SESSION") 

# Pyrogram Client ko initialize karein
try:
    if not all([API_ID, API_HASH, STRING_SESSION]):
        print("CRITICAL ERROR: API_ID, API_HASH, ya STRING_SESSION environment mein set nahi hai.")
        exit(1)
        
    # 🔥 FIXED CODE: Ab session ka naam chota rakhenge ('stream_session')
    # aur STRING_SESSION ko 'session_string' argument se pass karenge.
    bot = Client(
        "stream_session", # <--- Session file ka chota aur fixed naam
        session_string=STRING_SESSION, # <--- Asli data yahan se jaayega
        api_id=int(API_ID), 
        api_hash=API_HASH, 
    )
    bot.start() 
    print("Telegram Client Connected Successfully (using String Session)!")
except Exception as e:
    print(f"Connection Error during bot.start(): {e}")
    # Error aane par server ko band kar denge
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
        return "500 Internal Server Error: Session failed. Check STRING_SESSION and ensure the user is in the channel.", 500
    
    except Exception as e:
        print(f"Unforeseen Error during streaming: {e}")
        return "500 Internal Server Error: Could not process file.", 500
        
