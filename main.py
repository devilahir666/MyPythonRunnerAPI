import os
import re
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from fastapi import FastAPI, Request, HTTPException

# .env फ़ाइल से variables लोड करें (लोकल टेस्टिंग के लिए)
load_dotenv()

# *********************************************
# RENDER/ENVIRONMENT VARIABLES (ज़रूरी चीज़ें)
# *********************************************

API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
BIN_CHANNEL = int(os.environ.get("BIN_CHANNEL")) # वह चैनल जहां फाइलें स्टोर होंगी
WEBHOOK_URL = os.environ.get("WEBHOOK_URL") # Render Service URL (जैसे: https://your-service.onrender.com)
PORT = int(os.environ.get("PORT", 8000))

# ---------------------------------------------
# Pyrogram Client Setup
# ---------------------------------------------

app = Client(
    "TG_Direct_Link_Bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# ---------------------------------------------
# Helper Functions for Link Generation
# ---------------------------------------------

def generate_direct_link(message_id: int, file_name: str) -> str:
    """
    टेलीग्राम मैसेज ID का उपयोग करके डायरेक्ट स्ट्रीम/डाउनलोड लिंक बनाता है।
    """
    # यहाँ हम Render के URL का उपयोग करेंगे।
    # /dl/ मैसेज_आईडी/फ़ाइल_नाम.एक्सटेंशन
    # Render का URL 'WEBHOOK_URL' में है, हमें '/bot_token' हिस्सा हटाना होगा।
    base_url = WEBHOOK_URL.replace(f"/{BOT_TOKEN}", "")
    
    # फ़ाइल का नाम URL-फ्रेंडली बनाते हैं
    safe_file_name = re.sub(r"[^a-zA-Z0-9.\-_]", "_", file_name)
    
    # डायरेक्ट लिंक फॉर्मेट
    direct_link = f"{base_url}/dl/{message_id}/{safe_file_name}"
    
    return direct_link

# ---------------------------------------------
# BOT COMMANDS AND HANDLERS
# ---------------------------------------------

@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    """/start कमांड को हैंडल करता है"""
    await message.reply_text(
        "👋 **नमस्ते!** मैं आपके टेलीग्राम फ़ाइलों के लिए डायरेक्ट लिंक जेनरेट कर सकता हूँ।\n\n"
        "बस मुझे कोई भी फ़ाइल, वीडियो या मीडिया भेजिए।",
        parse_mode=ParseMode.MARKDOWN
    )

@app.on_message(filters.media & filters.private)
async def generate_link_handler(client: Client, message: Message):
    """फ़ाइल या मीडिया प्राप्त होने पर डायरेक्ट लिंक जेनरेट करता है"""
    
    # 1. फ़ाइल का पता लगाओ
    media = message.document or message.video or message.audio or message.photo
    
    if not media:
        await message.reply_text("माफ़ करना, मैं इस तरह के मीडिया को हैंडल नहीं कर सकता।")
        return
    
    status_msg = await message.reply_text("🔗 डायरेक्ट लिंक जेनरेट किया जा रहा है... कृपया प्रतीक्षा करें।")
    
    try:
        # 2. फ़ाइल को BIN_CHANNEL में फ़ॉरवर्ड करो
        forwarded_msg = await client.forward_messages(
            chat_id=BIN_CHANNEL,
            from_chat_id=message.chat.id,
            message_ids=message.id
        )
        
        # 3. फ़ॉरवर्ड किए गए मैसेज की ID से लिंक बनाओ
        # Pyrogram फ़ाइल का नाम सही से देता है।
        file_name = getattr(media, "file_name", "telegram_file.dat")
        message_id = forwarded_msg.id
        
        direct_link = generate_direct_link(message_id, file_name)
        
        # 4. यूजर को लिंक भेजो
        response_text = (
            f"📥 **फ़ाइल का नाम:** `{file_name}`\n"
            f"🔗 **डायरेक्ट लिंक:** [यहां क्लिक करें]({direct_link})\n\n"
            f"`{direct_link}`" # यूजर को कॉपी करने में आसानी के लिए
        )
        
        await status_msg.edit_text(response_text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

    except Exception as e:
        print(f"Error during link generation: {e}")
        await status_msg.edit_text(f"❌ लिंक जेनरेट करने में कोई समस्या आई:\n`{e}`")


# ---------------------------------------------
# FastAPI (Web Server) Setup for Render
# ---------------------------------------------

fastapi_app = FastAPI()

# 1. Start-up: Pyrogram Client शुरू करो और Webhook सेट करो
@fastapi_app.on_event("startup")
async def startup_event():
    print("Starting Pyrogram client and setting webhook...")
    try:
        await app.start()
        
        if WEBHOOK_URL:
            # Webhook set करें
            await app.set_webhook(url=f"{WEBHOOK_URL}/{BOT_TOKEN}")
            print(f"Webhook set to: {WEBHOOK_URL}/{BOT_TOKEN}")
        else:
            print("ERROR: WEBHOOK_URL not set. Bot will not work correctly on Render!")
            
    except Exception as e:
        print(f"Startup failed: {e}")

# 2. Webhook Route: Telegram से आने वाले सभी अपडेट यहाँ हैंडल होंगे
@fastapi_app.post(f"/{BOT_TOKEN}")
async def bot_webhook(request: Request):
    """Telegram Webhook updates को हैंडल करता है"""
    # Note: Telegram Bot API Secret Token को भी चेक करना सुरक्षा के लिए अच्छा है।
    
    data = await request.json()
    
    # Pyrogram को अपडेट भेजें
    await app.process_update(data)
    
    return {"ok": True}

# 3. Direct Link Route: यह वह असली लिंक है जो फ़ाइल को स्ट्रीम करेगा
@fastapi_app.get("/dl/{message_id}/{file_name}")
async def direct_link_handler(message_id: int, file_name: str):
    """डायरेक्ट लिंक से फ़ाइल को स्ट्रीम करता है"""
    
    # यह फ़ंक्शन Pyrogram client का उपयोग करके फ़ाइल को स्ट्रीम करता है
    try:
        # get_messages से मैसेज की जानकारी मिलती है
        forwarded_msg = await app.get_messages(chat_id=BIN_CHANNEL, message_ids=message_id)
        
        # फ़ाइल को स्ट्रीम करने का logic
        # Pyrogram का get_file और FileStreamer क्लास का इस्तेमाल करना होगा।
        
        # **नोट:** सीधे FastAPI से Pyrogram Streaming को इंटीग्रेट करना थोड़ा मुश्किल है।
        # इस शॉर्टकट में, हम फ़िलहाल इसे एक placeholder रखते हैं, 
        # और यूजर को एक 'Download' मैसेज दे सकते हैं जब तक आप Streaming Logic नहीं जोड़ते।
        
        # ***TEMP Placeholder***
        return {"error": "Streaming logic not fully implemented yet.", 
                "message": f"File ID {message_id} in BIN_CHANNEL is ready to stream/download."}
        
    except Exception as e:
        return {"error": "File not found or access denied.", "details": str(e)}

# 4. Health Check (Render के लिए ज़रूरी)
@fastapi_app.get("/")
async def health_check():
    """Render के लिए हेल्थ चेक।"""
    return {"status": "ok", "service": "TG Direct Link Generator Bot is running"}


# 5. Shut-down: Client बंद करो और Webhook हटाओ
@fastapi_app.on_event("shutdown")
async def shutdown_event():
    print("Stopping Pyrogram client and deleting webhook...")
    try:
        await app.stop()
        if WEBHOOK_URL:
            await app.delete_webhook()
    except Exception as e:
        print(f"Shutdown failed: {e}")

# Uvicorn entry point
app = fastapi_app
  
