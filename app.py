import os
import re
import asyncio
from fastapi import FastAPI, HTTPException, Request 
from starlette.responses import StreamingResponse 
from pyrogram import Client
from pyrogram.errors import FileReferenceExpired, RPCError
from typing import AsyncGenerator

# --- HARDCODED KEYS ---
API_ID = 23692613
API_HASH = "8bb69956d38a8226433186a199695f57"
STRING_SESSION = "BQFphUUAHgXd4GHGzpOXFKX1kJ-ScusfrHGvhk_cbLGN5DDa9-IMe08WtUU1pzFuz1DGy9jnwMMsJo2FnUZSQHtXNsaBm-UFA22ZqN4htnHk4-qdkACNpeTXayIcvETMsH97WLERuVr9t9NJTMpVkg4zD57b4CkmLljxqwt81_WQS99wTKzW7uDj412nIFudlHddsqDyiw2aXKM8Ar1yPKXUkpf_xTfrzUEKkrVppdTRdattUqahoq0zrlAUYxUCB-iTipGWJDeDrszD_QsOQs9p2F9WwYP44s6Zx9nznRXcv0EmO0MHjH-Zmew6yHcBAcu_r0-b7wShXytzzJ9EySBRul_2KwAAAAHq-0sgAA"
BOT_TOKEN = "8075063062:AAH8lWaA7yk6ucGnV7N5F_U87nR9FRwKv98"

# FastAPI aur Pyrogram Client ka setup
app = FastAPI(title="Telegram Async Streamer")
client = None 

# Client ko start/stop karne ke liye hooks
@app.on_event("startup")
async def startup_event():
    global client
    try:
        if not all([API_ID, API_HASH, STRING_SESSION, BOT_TOKEN]):
            print("CRITICAL ERROR: Keys are missing or empty.")
            raise Exception("Missing Environment Variables")

        client = Client(
            name=BOT_TOKEN.split(":")[0],
            session_string=STRING_SESSION,
            api_id=int(API_ID),
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            no_updates=True
        )
        await client.start()
        print("Telegram Async Client Connected Successfully!")
    except Exception as e:
        print(f"Connection Error during client.start(): {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    global client
    if client:
        await client.stop()
        print("Telegram Async Client Stopped.")

@app.get("/")
async def home():
    return {"message": "Telegram Async Streaming Proxy is Running. Use /api/stream/{chat_id}/{message_id} to stream."}

# Generator function for streaming
async def stream_generator(message, offset: int, limit: int) -> AsyncGenerator[bytes, None]:
    max_retries = 3
    
    # Hum stream karne ki koshish karenge aur agar File Reference Expired hua toh retry karenge
    for attempt in range(max_retries):
        try:
            # client.stream_media() mein Pyrogram ka chunk size default 1MB hai
            async for chunk in client.stream_media(
                message,
                offset=offset,
                limit=limit
            ):
                yield chunk
            # Agar streaming poori ho gayi toh loop se bahar nikal jao
            return
        
        except FileReferenceExpired:
            print(f"FileReferenceExpired on attempt {attempt + 1}. Retrying...")
            # File Reference Expired hone par Pyrogram file ko dobara fetch karne ki koshish karta hai
            # Lekin hum message ko dobara fetch kar lenge taaki naya reference mil jaaye
            try:
                # Naya message object fetch karne se naya file reference milta hai
                message = await client.get_messages(
                    chat_id=message.chat.id,
                    message_ids=message.id
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(1) # Thoda wait, phir retry
                    continue
                else:
                    # Akhri attempt fail
                    raise
            except Exception as e:
                print(f"Error refreshing message reference: {e}")
                raise # Agar refresh fail ho toh asli error fenk do
        
        except Exception as e:
            print(f"Stream generation error: {e}")
            raise # Dusre errors ko seedha fenk do

# 🎯 FINAL ASYNC ROUTE
@app.get("/api/stream/{chat_id}/{message_id}")
async def stream_file_by_id(chat_id: str, message_id: int, request: Request):
    print(f"Request received for Chat ID: {chat_id}, Message ID: {message_id}")
    
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
        else:
             headers['Content-Length'] = str(file_size)

        limit = end_byte - start_byte + 1 if end_byte is not None else file_size - start_byte

        # StreamingResponse use karna
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
        # Agar koi aur error aaye toh 500 return karo
        raise HTTPException(status_code=500, detail="Internal Server Error: Streaming failed.")
