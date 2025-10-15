# --- TELEGRAM STREAMING SERVER (PERMANENT FILE ID & CACHING OPTIMIZED) ---

# Import necessary libraries
import asyncio
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import StreamingResponse, PlainTextResponse
from telethon import TelegramClient
from telethon.errors import RPCError, FileReferenceExpiredError, AuthKeyError
from telethon.tl.types import InputDocument # Permanent ID se entity banane ke liye
import logging
import time 
from typing import Dict, Any, Optional
# ------------------------------------------

# Set up logging 
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger('telethon').setLevel(logging.WARNING)

# --- TELEGRAM CREDENTIALS (HARDCODED) ---
API_ID = 23692613
API_HASH = "8bb69956d38a8226433186a199695f57" 
BOT_TOKEN = "8075063062:AAH8lWaA7yk6ucGnV7N5F_U87nR9FRK Kv98" 
SESSION_NAME = None 
# ------------------------------------------

# --- CONFIGURATION (OPTIMIZED FOR LOW RAM & CACHING) ---
# NOTE: Channel entity is now mainly for client authorization, not fetching messages.
TEST_CHANNEL_ENTITY_USERNAME = '@serverdata00' 
OPTIMAL_CHUNK_SIZE = 1024 * 1024 * 2 # 2 MB chunk size (Low latency)
BUFFER_CHUNK_COUNT = 4 # 4 chunks (Low RAM usage)

# 🌟 JUGAD: Metadata Caching Setup (Keyed by Telegram Permanent File ID)
# Cache ki key ab permanent file ID string hogi.
FILE_METADATA_CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_TTL = 3600 # 60 minutes tak cache rakhenge

# 💡 MOCK DATABASE: Tumhe yeh function apni async Supabase query se badalna hoga.
# Ab DB mein sirf Permanent File ID, Size aur Title hai.
MOCK_SUPABASE_DATA = {
    # UUID jo public URL mein use hoga
    "a1b2c3d4-stream-001": {
        "file_id": "BAACAgEAAxkBAAMCaO0vt5pQ2NRhkCaf5Dhk2PUM9p8AAgUCAALeo2FHyL80nhjJ1W4eBA", # permanent_file_id
        "file_size": 50000000, 
        "title": "Big_Buck_Bunny.mp4"
    },
    "e5f6g7h8-stream-002": {
        "file_id": "BAACAgEAAxkBAAMCA-0vt5pQ2NRhkCaf5Dhk2PUM9p8AAgUCAALeo2FHyL80nhjJ1W4eBB",
        "file_size": 120000000, 
        "title": "Sintel_Movie.mkv"
    },
    # Tumhara diya hua UUID (MOCK data mein nahi hai, isliye error aayega)
    "7a048f80-ce75-4eb3-828d-6f8d26b920d3": {
         "file_id": "BAACAgEAAxkBAAPzZZ914c6kXn1t-c22h0y5l2qX36sAAk4CAAK1X1hI-o64J3K5yGAeBA",
         "file_size": 250000000,
         "title": "Test_File_250MB.mp4"
    }
}
async def _get_data_from_supabase(file_uuid: str) -> Optional[Dict[str, Any]]:
    """
    MOCK FUNCTION: Fetches Telegram permanent file_id, size, and title from Supabase using UUID.
    """
    # Real world mein yahan tumhara async DB query code aayega.
    # Tumhari query Supabase table 'database' mein 'id' column ke against 'file_uuid' ko match karegi.
    # Aur 'file_id', 'file_size', 'title' return karegi.
    await asyncio.sleep(0.01) # Simulating a fast DB lookup
    
    data = MOCK_SUPABASE_DATA.get(file_uuid)
    if data:
        # Dictionary keys ko map kiya gaya hai
        return {
            "file_id_permanent": data["file_id"],
            "file_size": data["file_size"],
            "title": data["title"]
        }
    return None
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
        status_msg = f"Streaming Proxy Active. Chunk Size: {chunk_mb}MB. Buffer: {total_buffer_mb}MB. Cache Key: Permanent File ID."
    else:
        status_msg = "Client is NOT connected/authorized. (503 Service Unavailable)."

    return PlainTextResponse(f"Streaming Proxy Status: {status_msg}")


# _get_or_resolve_channel_entity, download_producer, and file_iterator functions remain the same as they handle the stream logic.

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
        
    # 1. Supabase/DB Lookup
    db_data = await _get_data_from_supabase(file_uuid)
    if not db_data:
        # Agar DB mein UUID nahi mila, toh 404 error
        raise HTTPException(status_code=404, detail=f"File not found for UUID: {file_uuid} in Supabase.")
    
    file_size = db_data['file_size']
    file_title = db_data['title']
    permanent_file_id = db_data['file_id_permanent']
    
    file_entity_for_download = None 
    
    # --- CACHING LOGIC (Keyed by Permanent File ID) ---
    current_time = time.time()
    cached_data = FILE_METADATA_CACHE.get(permanent_file_id)
    
    if cached_data and (current_time - cached_data['timestamp']) < CACHE_TTL:
        # 🚀 Cache Hit: Sabse tez path!
        logging.info(f"Cache HIT for Permanent File ID. Streaming instantly from memory.")
        file_entity_for_download = cached_data['entity']
        
    else:
        # Cache Miss/Expired: Telegram API call to get live Document Entity (sirf ek baar)
        logging.info(f"Cache MISS/EXPIRED. Resolving Input Document for Permanent File ID.")

        try:
            # Telethon ka jugad: Permanent File ID string ko Document entity mein resolve karna.
            # get_input_document Telethon ka internal method hai jo string ID ko download ke liye 
            # zaroori object (InputDocument) mein convert karta hai.
            # file_id=None isliye kiya kyunki string ID pass kar rahe hain.
            file_entity_for_download = await client.get_input_document(permanent_file_id)
            
            if not isinstance(file_entity_for_download, InputDocument):
                 # Agar resolution failed hua toh generic error
                 raise ValueError("Failed to resolve permanent file ID to InputDocument.")

            # 3. Cache the live Telethon Entity (InputDocument is enough for iter_download)
            FILE_METADATA_CACHE[permanent_file_id] = {
                'entity': file_entity_for_download,
                'timestamp': current_time 
            }
            logging.info(f"Input Document resolved and CACHED successfully for Permanent File ID.")

        except Exception as e:
            logging.error(f"TELEGRAM RESOLUTION ERROR: {type(e).__name__}: {e}. Permanent File ID may be invalid or expired.")
            FILE_METADATA_CACHE.pop(permanent_file_id, None) 
            raise HTTPException(status_code=500, detail="Internal error resolving Permanent File ID from Telegram.")
        
    
    # 4. Range Handling aur Headers
    range_header = request.headers.get("range")
    
    content_type = "video/mp4" 
    if file_title.lower().endswith(".mkv"): content_type = "video/x-matroska"
    elif file_title.lower().endswith(".mp4"): content_type = "video/mp4"

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
