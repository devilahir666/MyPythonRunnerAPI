import os
import re
import asyncio
from fastapi import FastAPI, HTTPException, Request 
from starlette.responses import StreamingResponse 
# 🔥 Pyrogram ki jagah Telethon aur iske tools import kar rahe hain
from telethon import TelegramClient
from telethon.tl.types import InputMessagesFilterDocument
from telethon.errors import RPCError
from typing import AsyncGenerator

# --- MANDATORY TELETHON KEYS (Keys are hardcoded as requested) ---
# आपके पुराने API ID और HASH
API_ID = 23692613
API_HASH = "8bb69956d38a8226433186a199695f57"
# आपका Bot Token
BOT_TOKEN = "8075063062:AAH8lWaA7yk6ucGnV7N5F_U87nR9FRwKv98"
SESSION_NAME = "stream_bot_session" 

# Telethon ke liye bada chunk size (4MB)
CHUNK_SIZE_BYTES = 4 * 1024 * 1024 

# FastAPI aur Telethon Client ka setup
app = FastAPI(title="Telegram Telethon Stable Streamer")
client = None 

@app.on_event("startup")
async def startup_event():
    global client
    try:
        # Telethon Client initialization
        # 'bot_token' parameter se Bot Token login hota hai
        client = TelegramClient(
            session=SESSION_NAME, 
            api_id=int(API_ID), 
            api_hash=API_HASH
        )
        
        # client.start() mein bot token dete hain
        await client.start(bot_token=BOT_TOKEN) 
        
        print("Telegram Telethon Bot Client Connected Successfully!")
    except Exception as e:
        print(f"Connection Error during client.start(): {e}")
        client = None
        # Agar client start na ho, toh exception raise karo
        raise

@app.on_event("shutdown")
async def shutdown_event():
    global client
    if client:
        # client ko gracefully disconnect karo
        await client.disconnect()
        print("Telegram Telethon Bot Client Disconnected.")

@app.get("/")
async def home():
    return {"message": "Telethon Stable Streaming Proxy is Running. Use /api/stream/{chat_id}/{message_id} to stream."}

# Generator function for streaming
async def stream_generator(file_info, offset: int, limit: int) -> AsyncGenerator[bytes, None]:
    global client 
    
    bytes_to_read = limit
    
    try:
        # 🔥 Telethon ka iter_content() file reference expire hone par auto-refresh karta hai
        async for chunk in client.iter_content(
            file_info, # Message object ya media object
            chunk_size=CHUNK_SIZE_BYTES,
            offset=offset
        ):
            # Limit check (agar range set ho toh)
            if bytes_to_read <= 0:
                break
                
            chunk_len = len(chunk)
            if chunk_len > bytes_to_read:
                yield chunk[:bytes_to_read]
                break
            
            yield chunk
            bytes_to_read -= chunk_len

        return
            
    except asyncio.CancelledError:
        # Browser ya server cancellation ko gracefully handle karo
        print("Stream cancelled by client/server. Exiting generator.")
        return 
        
    except Exception as e:
        print(f"Telethon Stream generation failed with unhandled error: {e}")
        # Agar koi aur critical error aaye toh exception throw karo
        raise 

# 🎯 FINAL ASYNC ROUTE
@app.get("/api/stream/{chat_id}/{message_id}")
async def stream_file_by_id(chat_id: str, message_id: int, request: Request):
    global client 
    print(f"Request received for Chat ID: {chat_id}, Message ID: {message_id}")
    
    if not client:
        raise HTTPException(status_code=503, detail="Service Unavailable: Telegram client not initialized.")

    try:
        # Message details fetch karo (Telethon mein get_messages)
        message = await client.get_messages(
            entity=chat_id,
            ids=message_id
        )

        if not message or not message.media:
            raise HTTPException(status_code=404, detail="File not found or message does not contain media.")
        
        # File info nikalna
        file_info = message.media.document or message.media.video
        if not file_info:
             raise HTTPException(status_code=404, detail="File not found or message does not contain a file.")

        # File attributes se name, size, mime type nikalna
        file_name = getattr(file_info.attributes[0], 'file_name', None) or "streaming_file.mkv"
        file_size = file_info.size
        mime_type = file_info.mime_type or 'video/x-matroska'

        # Range Handling (HTTP standard ke liye)
        range_header = request.headers.get('Range')
        start_byte, end_byte = 0, file_size - 1
        status_code = 200
        
        headers = {
            "Content-Type": mime_type,
            "Content-Disposition": f"inline; filename=\"{file_name}\"",
            "Accept-Ranges": "bytes",
        }

        content_length = file_size
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
        
        headers['Content-Length'] = str(content_length)

        limit = end_byte - start_byte + 1 if end_byte is not None else file_size - start_byte

        # StreamingResponse
        return StreamingResponse(
            content=stream_generator(message, start_byte, limit),
            status_code=status_code,
            headers=headers,
            media_type=mime_type
        )

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in stream_file_by_id: {e}")
        # Agar koi aur error aaye toh 500 Internal Server Error return karo
        raise HTTPException(status_code=500, detail=f"Internal Server Error: Telethon Streaming Failed. {str(e)}")
