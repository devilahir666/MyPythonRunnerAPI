import os
from flask import Flask, Response
from pyrogram import Client
from pyrogram.errors import PeerIdInvalid, UserNotParticipant, AccessTokenInvalid

app = Flask(__name__)

# --- KEYS ARE READ SECURELY FROM RENDER ENVIRONMENT VARIABLES ---
# Ab STRING_SESSION bhi zaroori hai
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
STRING_SESSION = os.environ.get("STRING_SESSION") 

# Pyrogram Client ko initialize karein (User Client mode for stability)
try:
    # Saari zaroori keys check karein
    if not all([API_ID, API_HASH, STRING_SESSION]):
        print("CRITICAL ERROR: API_ID, API_HASH, ya STRING_SESSION environment mein set nahi hai.")
        exit(1)
        
    # Client ko chalu karein (STRING_SESSION use karke)
    # Yeh bot_token se zyaada powerful hota hai aur access issue fix karta hai
    bot = Client(
        STRING_SESSION, # Session string hi session ka naam banega
        api_id=int(API_ID), 
        api_hash=API_HASH, 
        # Bot token is optional here, we rely on the User Session for access
    )
    bot.start() 
    print("Telegram Client Connected Successfully (using String Session)!")
except Exception as e:
    print(f"Connection Error during bot.start(): {e}")
    exit(1)

# Simple home route for health check
@app.route('/')
def home():
    return "Telegram Streaming Proxy is Running. Use /api/stream/<channel_id>/<message_id> to stream.", 200

# 🎯 File Streaming Endpoint: /api/stream/<channel_id>/<message_id>
@app.route("/api/stream/<channel_id>/<int:message_id>")
def stream_file(channel_id, message_id):
    print(f"Request received for Channel: {channel_id}, Message: {message_id}")
    
    try:
        # 🔥 CRITICAL FIX: PEER_ID_INVALID aur access issue theek karne ke liye
        # Ab String Session hai, toh yeh 100% kaam karega
        bot.get_chat(channel_id) 
        
        # 1. File ki information Telegram se lein
        message = bot.get_messages(channel_id, message_id)
        
        # Check karein ki message mein media (document) hai ya nahi
        if not message.media or not message.document:
            print(f"Error: Message {message_id} is not a valid document.")
            return "404 Not Found: File not attached to this message.", 404

        file_name = message.document.file_name
        file_size = message.document.file_size
        mime_type = message.document.mime_type
        
        # 2. File ko stream karne ka generator function
        def generate():
            for chunk in bot.stream_media(message):
                yield chunk
        
        # 3. HTTP Response set karein
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
        print(f"CRITICAL ACCESS ERROR: {e}. String Session/Channel issue.")
        return "500 Internal Server Error: Bot has no access. Check your STRING_SESSION and ensure the user is in the channel.", 500
    
    except Exception as e:
        print(f"Unforeseen Error during streaming: {e}")
        return "500 Internal Server Error: Could not process file.", 500

# Yeh function Gunicorn run karega
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=os.environ.get("PORT", 5000))
    
