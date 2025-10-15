# --- TELEGRAM STREAMING SERVER (PERMANENT FILE ID & CACHING OPTIMIZED) ---

# Import necessary libraries
import asyncio
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import StreamingResponse, PlainTextResponse
from telethon import TelegramClient
from telethon.errors import RPCError, FileReferenceExpiredError, AuthKeyError
import logging
import time 
from typing import Dict, Any, Optional

# Set up logging 
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger('telethon').setLevel(logging.WARNING)

# --- TELEGRAM CREDENTIALS (HARDCODED) ---
API_ID = 23692613
API_HASH = "8bb69956d38a8226433186a199695f57" 
BOT_TOKEN = "8075063062:AAH8lWaA7yk6ucGnV7N5F_U87nR9FRwKv98" 
SESSION_NAME = None 
# ------------------------------------------

# --- CONFIGURATION (OPTIMIZED FOR LOW RAM & CACHING) ---
TEST_CHANNEL_ENTITY_USERNAME = '@serverdata00'
OPTIMAL_CHUNK_SIZE = 1024 * 1024 * 2 # 2 MB chunk size (Low latency)
BUFFER_CHUNK_COUNT = 4 # 4 chunks (Low RAM usage)

# 🌟 JUGAD: Metadata Caching Setup (Keyed by Telegram Permanent File ID)
# Cache ki key ab permanent file ID string hogi.
FILE_METADATA_CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_TTL = 3600 # 60 minutes tak cache rakhenge

# 💡 MOCK DATABASE: Tumhe yeh function apni async Supabase query se badalna hoga.
# Ab DB mein permanent file ID store hai.
MOCK_SUPABASE_DATA = {
    # UUID jo public URL mein use hoga
    "a1b2c3d4-stream-001": {
        # Telegram Permanent File ID
        "file_id_permanent": "BAACAgEAAxkBAAMCaO0vt5pQ2NRhkCaf5Dhk2PUM9p8AAgUCAALeo2FHyL80nhjJ1W4eBA",
        # Original Message ID (sirf re-fetching ke liye needed)
        "message_id": 12345, 
        "size": 50000000, 
        "title": "Big_Buck_Bunny.mp4"
    },
    "e5f6g7h8-stream-002": {
        "file_id_permanent": "BAACAgEAAxkBAAMCA-0vt5pQ2NRhkCaf5Dhk2PUM9p8AAgUCAALeo2FHyL80nhjJ1W4eBB",
        "message_id": 67890,
        "size": 120000000, 
        "title": "Sintel_Movie.mkv"
    },
}
async def _get_data_from_supabase(file_uuid: str) -> Optional[Dict[str, Any]]:
    """
    MOCK FUNCTION: Fetches Telegram permanent file_id, message_id, size, and title from Supabase using UUID.
    """
    # Real world mein yahan tumhara async DB query code aayega jo UUID se data laayega
    await asyncio.sleep(0.01) # Simulating a fast DB lookup
    
    return MOCK_SUPABASE_DATA.get(file_uuid)
# -------------------------------

app = FastAPI(title="Telethon UUID Streaming Proxy")
client: TelegramClient = None
resolved_channel_entity: Optional[Any] = None 
entity_resolve_lock = asyncio.Lock()


@app.on_event("startup")
async def startup_event():
    global client
    logging.info("Attempting to connect Telegram Client...")
    
    try:
        client_instance = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        await client_instance.start(bot_token=BOT_TOKEN)
        client = client_instance
        
        if client and await client.is_user_authorized():
             logging.info("Telegram Client connected and authorized successfully!")
        else:
             logging.error("Telegram Client failed to authorize user or bot.")
             client = None

    except Exception as e:
        logging.error(f"FATAL TELETHON CONNECTION ERROR: {type(e).__name__}: {e}. Client will remain disconnected.")
        client = None 

@app.on_event("shutdown")
async def shutdown_event():
    global client
    if client:
        logging.info("Closing Telegram Client connection...")
        await client.disconnect()

@app.get("/")
async def root():
    chunk_mb = OPTIMAL_CHUNK_SIZE // (1024 * 1024)
    total_buffer_mb = chunk_mb * BUFFER_CHUNK_COUNT
    if client and await client.is_user_authorized():
        status_msg = f"Streaming Proxy Active. Target Channel: {TEST_CHANNEL_ENTITY_USERNAME}. Chunk Size: {chunk_mb}MB. Buffer: {total_buffer_mb}MB. Cache Key: Permanent File ID."
    else:
        status_msg = "Client is NOT connected/authorized. (503 Service Unavailable)."

    return PlainTextResponse(f"Streaming Proxy Status: {status_msg}")


async def _get_or_resolve_channel_entity():
    """Channel entity ko resolve karta hai aur global variable mein cache karta hai (Lazy Caching)."""
    global resolved_channel_entity
    
    if resolved_channel_entity:
        return resolved_channel_entity

    async with entity_resolve_lock:
        if resolved_channel_entity:
            return resolved_channel_entity
            
        logging.info(f"LAZY RESOLVE: Resolving channel entity for {TEST_CHANNEL_ENTITY_USERNAME}...")
        try:
            if client is None:
                 raise Exception("Telegram client is not initialized.")
                 
            resolved_channel_entity = await client.get_entity(TEST_CHANNEL_ENTITY_USERNAME)
            logging.info(f"LAZY RESOLVE SUCCESS: Channel resolved and cached.")
            return resolved_channel_entity
        except Exception as e:
            logging.error(f"LAZY RESOLVE FAILED: Could not resolve target channel entity: {e}")
            raise HTTPException(status_code=500, detail="Could not resolve target channel entity.")


async def download_producer(
    client_instance: TelegramClient,
    file_entity, 
    start_offset: int, 
    end_offset: int, 
    chunk_size: int, 
    queue: asyncio.Queue
):
    """
    Background mein Telegram se data download karke queue mein daalta hai (Producer).
    """
    offset = start_offset
    
    try:
        while offset <= end_offset:
            limit = min(chunk_size, end_offset - offset + 1)
            
            async for chunk in client_instance.iter_download(
                file_entity, 
                offset=offset,
                limit=limit,
                chunk_size=chunk_size
            ):
                await queue.put(chunk)
                offset += len(chunk)

            if offset <= end_offset:
                 logging.error(f"PRODUCER BREAK: Offset reached {offset}, target was {end_offset}. Stopping.")
                 break 
        
    except (FileReferenceExpiredError, RPCError, TimeoutError, AuthKeyError) as e:
        logging.error(f"PRODUCER CRITICAL ERROR: {type(e).__name__} during download.")
    except Exception as e:
        logging.error(f"PRODUCER UNHANDLED EXCEPTION: {e}")
    
    finally:
        await queue.put(None)


async def file_iterator(file_entity_for_download, file_size, range_header, request: Request):
    """
    Queue se chunks nikalta hai aur FastAPI ko stream karta hai (Consumer).
    """
    start = 0
    end = file_size - 1
    
    if range_header:
        try:
            range_value = range_header.split('=')[1]
            if '-' in range_value:
                start_str, end_str = range_value.split('-')
                start = int(start_str) if start_str else 0
                end = int(end_str) if end_str else file_size - 1
        except Exception as e:
            logging.warning(f"Invalid Range header format: {e}. Defaulting to full stream.")
            start = 0
            end = file_size - 1

    queue = asyncio.Queue(maxsize=BUFFER_CHUNK_COUNT)
    
    producer_task = asyncio.create_task(
        download_producer(client, file_entity_for_download, start, end, OPTIMAL_CHUNK_SIZE, queue)
    )
    
    try:
        while True:
            chunk = await queue.get()
            
            if chunk is None:
                break
                
            if await request.is_disconnected():
                logging.info("Client disconnected during stream (Terminating iterator).")
                break 

            yield chunk
            queue.task_done()
            
    except asyncio.CancelledError:
        logging.info("Iterator cancelled (Client disconnect or shutdown).")
    except Exception as e:
        logging.error(f"CONSUMER UNHANDLED EXCEPTION: {e}")

    finally:
        if not producer_task.done():
            producer_task.cancel()
        await asyncio.gather(producer_task, return_exceptions=True)
# -------------------------------------------------------------


@app.get("/api/stream/movie/{file_uuid}")
async def stream_file_by_uuid(file_uuid: str, request: Request):
    """Supabase UUID se file stream karta hai."""
    global client
    if client is None:
        raise HTTPException(status_code=503, detail="Telegram Client not connected.")
        
    resolved_entity = await _get_or_resolve_channel_entity()
    
    db_data = await _get_data_from_supabase(file_uuid)
    if not db_data:
        raise HTTPException(status_code=404, detail=f"File not found for UUID: {file_uuid} in Supabase.")
    
    file_size = db_data['size']
    file_title = db_data['title']
    permanent_file_id = db_data['file_id_permanent']
    file_id_int = db_data['message_id'] # Backup for re-fetch
    
    file_entity_for_download = None 
    
    # --- CACHING LOGIC (Keyed by Permanent File ID) ---
    current_time = time.time()
    cached_data = FILE_METADATA_CACHE.get(permanent_file_id)
    
    if cached_data and (current_time - cached_data['timestamp']) < CACHE_TTL:
        # 🚀 Cache Hit: Most common and fastest path! (No Telegram call, no DB call)
        logging.info(f"Cache HIT for File ID. Streaming instantly from memory.")
        file_entity_for_download = cached_data['entity']
        
    else:
        # Cache Miss/Expired: Telegram API call to get live Document Entity
        logging.info(f"Cache MISS/EXPIRED for File ID. Fetching live Telethon Entity for Message ID: {file_id_int}")

        # 2. Telegram API Call (Only done once per CACHE_TTL)
        try:
            # Message ID se live entity fetch karo
            message = await client.get_messages(resolved_entity, ids=file_id_int) 
            
            media_entity = None
            if message and message.media:
                if hasattr(message.media, 'document') and message.media.document:
                    media_entity = message.media.document
                elif hasattr(message.media, 'video') and message.media.video:
                    media_entity = message.media.video
            
            if media_entity:
                file_entity_for_download = media_entity
                # 3. Cache the live Telethon Entity (with current file_reference)
                FILE_METADATA_CACHE[permanent_file_id] = {
                    'entity': file_entity_for_download,
                    'timestamp': current_time 
                }
                logging.info(f"Metadata fetched and CACHED successfully for Permanent File ID.")
            else:
                raise HTTPException(status_code=404, detail="Media not streamable from Telegram.")

        except HTTPException:
            raise
        except Exception as e:
            logging.error(f"METADATA RESOLUTION ERROR: {type(e).__name__}: {e}")
            FILE_METADATA_CACHE.pop(permanent_file_id, None) 
            raise HTTPException(status_code=500, detail="Internal error resolving Telegram file metadata.")
        
    
    # 4. Range Handling aur Headers
    range_header = request.headers.get("range")
    
    content_type = "video/mp4" 
    if file_title.endswith(".mkv"): content_type = "video/x-matroska"
    elif file_title.endswith(".mp4"): content_type = "video/mp4"

    if range_header:
        try:
            start_str = range_header.split('=')[1].split('-')[0]
            start_range = int(start_str) if start_str else 0
        except:
            start_range = 0
            
        content_length = file_size - start_range
        
        headers = {
            "Content-Type": content_type,
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "Content-Range": f"bytes {start_range}-{file_size - 1}/{file_size}",
            "Content-Disposition": f"inline; filename=\"{file_title}\"",
            "Connection": "keep-alive"
        }
        return StreamingResponse(
            file_iterator(file_entity_for_download, file_size, range_header, request),
            status_code=status.HTTP_206_PARTIAL_CONTENT,
            headers=headers
        )
    else:
        headers = {
            "Content-Type": content_type,
            "Content-Length": str(file_size),
            "Accept-Ranges": "bytes",
            "Content-Disposition": f"inline; filename=\"{file_title}\"",
            "Connection": "keep-alive"
        }
        return StreamingResponse(
            file_iterator(file_entity_for_download, file_size, None, request),
            headers=headers
        )
