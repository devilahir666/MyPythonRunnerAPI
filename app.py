import re
import asyncio
from fastapi import FastAPI, HTTPException, Request 
from starlette.responses import StreamingResponse 
from telethon import TelegramClient
from telethon.tl.types import InputDocumentFileLocation
from telethon.errors.rpcerrorlist import FileReferenceExpiredError
from typing import AsyncGenerator

# db.py se Supabase function import karein
from db import get_movie_data 

# --- HARDCODED TELEGRAM KEYS ---
# NOTE: Telethon ko Bot Token se connect karne ke liye BOT_TOKEN hi kafi hai, 
# lekin API_ID aur API_HASH bhi dalna zaroori hai.
API_ID = 23692613
API_HASH = "8bb69956d38a8226433186a199695f57"
BOT_TOKEN = "8075063062:AAH8lWaA7yk6ucGn7N5F_U87nR9FRwKv98"
SESSION_NAME = "stream_bot_session"

# Pyrogram ke liye bada chunk size (4MB)
CHUNK_SIZE_BYTES = 4 * 1024 * 1024 

# FastAPI aur Telethon Client ka setup
app = FastAPI(title="Stable Telethon Streaming Proxy (Supabase)")
client: Optional[TelegramClient] = None 

# Client ko startup par sirf ek baar start karna
@app.on_event("startup")
async def startup_event():
    global client
    try:
        if not all([API_ID, API_HASH, BOT_TOKEN]):
            print("CRITICAL ERROR: API keys or BOT_TOKEN are missing.")
            raise Exception("Missing Credentials")

        # Bot Client se connect kar rahe hain
        client = TelegramClient(
            SESSION_NAME, 
            API_ID, 
            API_HASH
        ).start(bot_token=BOT_TOKEN)
        
        # client object ko await karna zaroori hai
        client = await client
        print("Telegram Bot Client (Telethon) Connected Successfully!")
    except Exception as e:
        print(f"Connection Error during client.start(): {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    global client
    if client:
        await client.disconnect()
        print("Telegram Bot Client Disconnected.")

@app.get("/")
async def home():
    return {"message": "Telethon Streaming Proxy is Running. Use /api/stream/movie/{uuid} to stream."}

# Generator function for streaming
async def stream_generator(file_id: str, file_size: int, offset: int, limit: int) -> AsyncGenerator[bytes, None]:
    global client 
    max_retries = 3

    # Permanent File ID se file object banana
    try:
        # File ID ko InputDocumentFileLocation mein convert karein
        # Telethon file_id ko InputDocumentFileLocation mein decode karta hai
        file_location = await client.get_document(file_id)
        
        # Agar get_document fail ho jaye to error aega
        if not file_location:
            raise ValueError(f"Could not resolve file ID: {file_id}")
            
    except Exception as e:
        print(f"Error resolving permanent File ID: {e}")
        raise HTTPException(status_code=500, detail="File ID Resolution Failed.")

    # Streaming start karna
    for attempt in range(max_retries):
        try:
            
            # Telethon mein iter_content() streaming ke liye use hota hai aur ye stable hai.
            # limit ko chunk_size se replace kar rahe hain
            chunk_generator = client.iter_content(
                file_location,
                offset=offset,
                limit=limit,
                chunk_size=CHUNK_SIZE_BYTES
            )
            
            async for chunk in chunk_generator:
                yield chunk
            return
        
        except FileReferenceExpiredError:
            # Telethon mein ye error khud hi handle ho jaana chahiye, par just in case.
            print(f"FileReferenceExpiredError on attempt {attempt + 1}. Telethon should refresh...")
            await asyncio.sleep(1) 
            if attempt >= max_retries - 1:
                raise
        
        except asyncio.CancelledError:
            # Render ya browser ne connection kaata, gracefully exit karna.
            print("Stream cancelled by client (CancelledError).")
            return
            
        except Exception as e:
            print(f"Stream generation internal error: {e}")
            raise

# 🎯 FINAL ASYNC ROUTE: Supabase ID (UUID) se streaming
@app.get("/api/stream/movie/{movie_uuid}")
async def stream_file_by_db_id(movie_uuid: str, request: Request):
    global client 
    print(f"Request received for Movie UUID: {movie_uuid}")
    
    if not client:
        raise HTTPException(status_code=503, detail="Service Unavailable: Telegram client not initialized.")

    # 1. Supabase se Permanent File ID, Title, aur Size fetch karo
    movie_data = get_movie_data(movie_uuid)
    
    if not movie_data:
        raise HTTPException(status_code=404, detail=f"Movie data not found for UUID: {movie_uuid} in Supabase.")
        
    encoded_file_id = movie_data['file_id']
    file_name = movie_data['title'] + ".mkv" # .mkv extension joda gaya
    file_size = movie_data['file_size']
    mime_type = 'video/x-matroska' # Streaming ke liye common MIME type

    # 2. Range Handling Logic (same as before)
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

    limit = content_length # jitna range mein data chahiye

    # 3. StreamingResponse
    try:
        return StreamingResponse(
            content=stream_generator(encoded_file_id, file_size, start_byte, limit),
            status_code=status_code,
            headers=headers,
            media_type=mime_type
        )
    except Exception as e:
        print(f"Error setting up StreamingResponse: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error: Final setup failed.")
