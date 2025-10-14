import os
import re
import asyncio
from fastapi import FastAPI, HTTPException, Request 
from starlette.responses import StreamingResponse 
from pyrogram import Client
from pyrogram.errors import FileReferenceExpired, RPCError
from typing import AsyncGenerator

# --- HARDCODED KEYS (Only Bot Token needed for public channels) ---
API_ID = 23692613
API_HASH = "8bb69956d38a8226433186a199695f57"
BOT_TOKEN = "8075063062:AAH8lWaA7yk6ucGnV7N5F_U87nR9FRwKv98"

# Pyrogram ke liye bada chunk size (4MB)
CHUNK_SIZE_BYTES = 4 * 1024 * 1024 

# FastAPI aur Pyrogram Client ka setup
app = FastAPI(title="Telegram Async Streamer")
client = None 

# Client ko startup par sirf ek baar start karna
@app.on_event("startup")
async def startup_event():
    global client
    try:
        if not all([API_ID, API_HASH, BOT_TOKEN]):
            print("CRITICAL ERROR: API keys or BOT_TOKEN are missing.")
            raise Exception("Missing Environment Variables")

        # Bot Client se connect kar rahe hain
        client = Client(
            name=BOT_TOKEN.split(":")[0],
            api_id=int(API_ID),
            api_hash=API_HASH,
            bot_token=BOT_TOKEN, 
            no_updates=True
        )
        await client.start()
        print("Telegram Bot Client Connected Successfully!")
    except Exception as e:
        print(f"Connection Error during client.start(): {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    global client
    if client:
        await client.stop()
        print("Telegram Bot Client Stopped.")

@app.get("/")
async def home():
    return {"message": "Telegram Async Streaming Proxy is Running. Use /api/stream/{chat_id}/{message_id} to stream."}

# Generator function for streaming
async def stream_generator(message, offset: int, limit: int) -> AsyncGenerator[bytes, None]:
    global client # Global client use ho raha hai
    max_retries = 3
    
    # 🔥 FIX: async with client: ko hata diya gaya hai
    for attempt in range(max_retries):
        try:
            # client.stream_media() mein humne bada chunk size set kiya hai
            async for chunk in client.stream_media(
                message,
                offset=offset,
                limit=limit,
                chunk_size=CHUNK_SIZE_BYTES 
            ):
                yield chunk
            return
        
        except FileReferenceExpired:
            print(f"FileReferenceExpired on attempt {attempt + 1}. Retrying...")
            try:
                # Naya message object fetch karne se naya file reference milta hai
                message = await client.get_messages(
                    chat_id=message.chat.id,
                    message_ids=message.id
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(1) 
                    continue
                else:
                    raise
            except Exception as e:
                print(f"Error refreshing message reference: {e}")
                raise
        
        except Exception as e:
            print(f"Stream generation error: {e}")
            raise

# 🎯 FINAL ASYNC ROUTE
@app.get("/api/stream/{chat_id}/{message_id}")
async def stream_file_by_id(chat_id: str, message_id: int, request: Request):
    global client # Global client use ho raha hai
    print(f"Request received for Chat ID: {chat_id}, Message ID: {message_id}")
    
    if not client:
        raise HTTPException(status_code=503, detail="Service Unavailable: Telegram client not initialized.")

    try:
        # Message details fetch karo
        message = await client.get_messages(
            chat_id=chat_id,
            message_ids=message_id
        )

        if not message or (not message.video and not message.document):
            raise HTTPException(status_code=404, detail="File not found or message does not contain a file.")
        
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
        raise HTTPException(status_code=500, detail="Internal Server Error: Streaming failed.")
