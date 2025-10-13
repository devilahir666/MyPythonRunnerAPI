# Proxy Server - Python (Flask aur Pyrogram ke saath)

from flask import Flask, Response
from pyrogram import Client, filters
from pyrogram.enums import ParseMode

app = Flask(__name__)

# --- 🚨 APNI KEYS YAHAAN DAALEIN 🚨 ---
API_ID = 29243403
API_HASH = "1adc479f6027488cdd13259028056b73"
BOT_TOKEN = "8160479582:AAGhe7dQlOgRhfGED36xlcFHq01QV_daAu4"
# ---------------------------------------

# Pyrogram Client ko initialize karein
# 'my_streamer_session' ek session ka naam hai
bot = Client(
    "my_streamer_session", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN
)

# Server chalu hone par Telegram client ko connect karein
try:
    bot.start() 
    print("Telegram Client Connected Successfully!")
except Exception as e:
    print(f"Error connecting to Telegram: {e}")
    # Aapko yahan server ko rokna padega agar connection fail ho
    # For now, we continue with a warning.

# 🎯 File Streaming Endpoint: /api/stream/<channel_id>/<message_id>
@app.route("/api/stream/<channel_id>/<int:message_id>")
def stream_file(channel_id, message_id):
    try:
        # 1. File ki information Telegram se lein
        # message_id ek integer hona chahiye
        message = bot.get_messages(channel_id, message_id)
        
        # Check karein ki message mein media hai ya nahi
        if not message.media or not message.document:
            return "404 Not Found: File not attached to this message.", 404

        file_name = message.document.file_name
        file_size = message.document.file_size
        mime_type = message.document.mime_type
        
        # 2. File ko stream karne ka generator function
        def generate():
            # bot.stream_media() file ko tukdon (chunks) mein padhta hai.
            # Yahi woh logic hai jo badi files ko handle karta hai.
            for chunk in bot.stream_media(message):
                yield chunk
        
        # 3. HTTP Response set karein
        # Headers download ko turant shuru kar dete hain browser mein
        return Response(
            generate(),
            mimetype=mime_type or 'application/octet-stream', # File ka type
            headers={
                "Content-Disposition": f"attachment; filename=\"{file_name}\"",
                "Content-Length": str(file_size),
                "Accept-Ranges": "bytes" # Zaroori, taaki download resume ho sake
            }
        )

    except Exception as e:
        print(f"Error during streaming: {e}")
        # Agar koi error ho toh 500 (Internal Server Error) return karein
        return "500 Internal Server Error: Could not process file.", 500

if __name__ == "__main__":
    # Production mein, aapko port environment variable se lena chahiye
    app.run(host='0.0.0.0', port=5000)
    
