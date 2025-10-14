import os
import re
import asyncio
from fastapi import FastAPI, HTTPException, Request 
from starlette.responses import StreamingResponse 
from telethon import TelegramClient
from telethon.tl.types import InputDocumentFileLocation
from typing import AsyncGenerator
# 🔥 Humari custom db file import karte hain
from db import get_movie_data 

# --- MANDATORY TELETHON KEYS (Hardcoded) ---
API_ID = 23692613
API_HASH = "8bb69956d38a8226433186a199695f57"
BOT_TOKEN = "8075063062:AAH8lWaA7yk6ucGnV7N5F_U87nR9FRwKv98"
SESSION_NAME = "stream_bot_session" 

CHUNK_SIZE_BYTES = 4 * 1024 * 1024 

app = FastAPI(title="Telegram Telethon Supabase Streamer")
client = None 

# Startup aur Shutdown Events (Jismein client initialize hota hai)
@app.on_event("startup")
async def startup_event():
    global client
    try:
        client = TelegramClient(
            session=SESSION_NAME, 
            api_id=int(API_ID), 
            api_hash=API_HASH
        )
        await client.start(bot_token=BOT_TOKEN) 
        print("Telegram Telethon Bot Client Connected Successfully!")
    except Exception as e:
        print(f"Connection Error during client.start(): {e}")
        client = None
        raise

@app.on_event("shutdown")
async def shutdown_event():
    global client
    if client:
        await client.disconnect()
        print("Telegram Telethon Bot Client Disconnected.")

@app.get("/")
async def home():
    # Ab home page par Supabase ID (UUID) se streaming ka example denge
    example_uuid = "a03d685b-fe9a-4dda-a65f-bd20a1bf191b" # Example UUID
    return {"message": "Telethon Supabase Streaming Proxy is Running.", 
            "usage": f"Use /api/stream/movie/{{movie_uuid}} to stream using Supabase UUID (e.g., /api/stream/movie/{example_uuid})."}


# Generator function (Same stable logic)
async def stream_generator(file_id_info, size: int, offset: int, limit: int) -> AsyncGenerator[bytes, None]:
    global client 
    bytes_to_read = limit
    
    try:
        # Telethon's iter_content is the key to stability (it handles file reference refresh)
        async for chunk in client.iter_content(
            file_id_info, 
            chunk_size=CHUNK_SIZE_BYTES,
            offset=offset
        ):
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
        print("Stream cancelled by client/server. Exiting generator.")
        return 
    except Exception as e:
        print(f"Telethon Stream generation failed with unhandled error: {e}")
        raise 

# 🔥 FINAL ROUTE: Supabase Movie UUID se streaming
@app.get("/api/stream/movie/{movie_uuid}")
async def stream_file_by_db_id(movie_uuid: str, request: Request):
    global client 
    print(f"Request received for Movie UUID: {movie_uuid}")
    
    if not client:
        raise HTTPException(status_code=503, detail="Service Unavailable: Telegram client not initialized.")
    
    # 🔥 STEP 1: Supabase se permanent file ID, size aur title nikalo
    movie_data = get_movie_data(movie_uuid)
    
    if not movie_data:
        # Check if the UUID is invalid or data is missing
        raise HTTPException(status_code=404, detail=f"Movie ID {movie_uuid} not found in Supabase or DB error.")
    
    encoded_file_id = movie_data['file_id']
    file_size = movie_data['file_size']
    file_name = movie_data['title']

    try:
        # 🔥 STEP 2: Permanent file ID ko Telethon ke object mein badlo
        # Telethon yeh object use karke direct permanent file se download karega
        file_info = client.get_input_media(encoded_file_id)

        # File type check
        if not hasattr(file_info, 'document'):
            raise HTTPException(status_code=400, detail="Invalid Telegram File ID stored in DB.")

        mime_type = 'video/x-matroska' 

        # Range Handling (HTTP standard ke liye) - Wahi robust logic
        range_header = request.headers.get('Range')
        start_byte, end_byte = 0, file_size - 1
        status_code = 200
        
        headers = {
            "Content-Type": mime_type,
            "Content-Disposition": f"inline; filename=\"{file_name}.mkv\"",
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
        limit = end_byte - start_byte + 1 

        # StreamingResponse
        return StreamingResponse(
            content=stream_generator(file_info, file_size, start_byte, limit),
            status_code=status_code,
            headers=headers,
            media_type=mime_type
        )

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in stream_file_by_db_id: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: Streaming Failed. {str(e)}")
