# --- TELEGRAM STREAMING SERVER (CLOUD READY - ASYNC BUFFERING & LAZY CACHING) ---

# Import necessary libraries
from telethon.sessions import StringSession 
import asyncio
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import StreamingResponse, PlainTextResponse
from telethon import TelegramClient
from telethon.errors import RPCError, FileReferenceExpiredError, AuthKeyError, FloodWaitError
import logging
import time
from typing import Dict, Any, Optional
import httpx 
import os 
# ------------------------------------------

# Set up logging 
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger('telethon').setLevel(logging.WARNING)

# --- GLOBAL CONFIGURATION (USER SESSION & BOT POOL) ---

# 1. USER SESSION CONFIG (Metadata Fetcher)
# Iska kaam sirf channel entity resolve karna aur file reference nikalna hai.
SESSION_FILE_OWNER = "owner.session"

USER_SESSION_CONFIG = {
    'api_id': 23692613,                                # Owner Account ki API ID
    'api_hash': "8bb69956d38a8226433186a199695f57",    # Owner Account ka API Hash
    'session_file': SESSION_FILE_OWNER, 
    'name': 'owner_client'
}

# 2. BOT POOL CONFIG (Download Workers)
# Har bot download speed badhaega.
BOT_TOKENS = [
    "8075063062:AAH8lWaA7yk6ucGnV7N5F_U87nR9FRwKv98", # Aapka diya hua Bot Token
]

# Global pool aur counter
owner_client: Optional[TelegramClient] = None # Single User Client
bot_client_pool: list[TelegramClient] = []   # Bot Clients ka Pool
global_bot_counter = 0                       # Bot pool ke liye Round-Robin counter

# --- Helper function to select client (Round-Robin) ---
def get_next_bot_client() -> Optional[TelegramClient]:
    """Round-Robin tarike se agla bot client chunnta hai (Download Worker)।"""
    global global_bot_counter
    if not bot_client_pool:
        return None
        
    client_index = global_bot_counter % len(bot_client_pool)
    selected_client = bot_client_pool[client_index]
    global_bot_counter += 1 
    
    if global_bot_counter >= len(bot_client_pool) * 1000:
        global_bot_counter = 0

    return selected_client
# --------------------------------------------------------

# --- CONFIGURATION (LOW RAM & CACHING OPTIMIZATION) ---
TEST_CHANNEL_ENTITY_USERNAME = 'https://t.me/+hPquYFblYHkxYzg1' 
OPTIMAL_CHUNK_SIZE = 1024 * 1024 * 2 # 2 MB chunk size
BUFFER_CHUNK_COUNT = 4 

# ** PINGER CONFIGURATION **
PINGER_DELAY_SECONDS = 120
PUBLIC_SELF_PING_URL = "https://telegram-stream-proxy-x63x.onrender.com/"
# Metadata Caching Setup
FILE_METADATA_CACHE: Dict[int, Dict[str, Any]] = {}
CACHE_TTL = 3600 # 60 minutes tak cache rakhenge
# -------------------------------

app = FastAPI(title="Telethon Async Streaming Proxy (User Session + Bot Pool)")

async def keep_alive_pinger():
    """Render Free Tier ko inactive hone se rokne ke liye self-ping karta hai।"""
    logging.info(f"PINGER: Background keep-alive task started (interval: {PINGER_DELAY_SECONDS}s). Target: {PUBLIC_SELF_PING_URL}")
    
    async with httpx.AsyncClient(timeout=10) as client_http: 
        while True:
            await asyncio.sleep(PINGER_DELAY_SECONDS)
            try:
                response = await client_http.get(PUBLIC_SELF_PING_URL) 
                
                if response.status_code == 200:
                    logging.info(f"PINGER: Self-Heartbeat sent successfully (Status: {response.status_code}).")
                else:
                    logging.warning(f"PINGER: Self-Heartbeat FAILED (Status: {response.status_code}). Response text: {response.text[:50]}...")
            except httpx.HTTPError as e:
                 logging.error(f"PINGER: Self-Heartbeat HTTP Error: {type(e).__name__} during request.")
            except Exception as e:
                 logging.error(f"PINGER: Self-Heartbeat General Error: {type(e).__name__}: {e}")


# ----------------------------------------------------------------------
# UPDATED startup_event function (Owner User Session aur Bot Pool ko connect karta hai)
# ----------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    global owner_client, bot_client_pool
    logging.info("Attempting to connect Telegram Clients (User Session & Bot Pool)...") 

    asyncio.create_task(keep_alive_pinger())
    logging.info("PINGER: Keep-alive task scheduled.")

    # --- 1. USER SESSION CONNECTION (Owner - Metadata Fetcher) ---
    config = USER_SESSION_CONFIG
    session_file_path = config.get('session_file')
    client_name = config.get('name')
    
    if not os.path.exists(session_file_path):
         logging.critical(f"FATAL CONNECTION ERROR for {client_name}: Session file {session_file_path} NOT FOUND.")
    else:
        try:
            client_instance = TelegramClient(
                session=session_file_path,
                api_id=config['api_id'], 
                api_hash=config['api_hash'],
                retry_delay=1
            )
            await client_instance.start()
            
            if await client_instance.is_user_authorized():
                 logging.info(f"User Session ({client_name}) connected and authorized successfully! (Role: Metadata Fetcher)")
                 owner_client = client_instance
            else:
                 logging.error(f"User Session ({client_name}) failed to authorize. (Session may be expired.)")
        except Exception as e:
            logging.error(f"FATAL CONNECTION ERROR for {client_name}: {type(e).__name__}: {e}. Client will remain disconnected.")

    # --- 2. BOT POOL CONNECTION (Workers) ---
    for token in BOT_TOKENS:
        try:
            # Bot token se connect karna
            bot_client = await TelegramClient(None, 
                                              api_id=USER_SESSION_CONFIG['api_id'], 
                                              api_hash=USER_SESSION_CONFIG['api_hash']).start(bot_token=token)
            
            bot_info = await bot_client.get_me()
            logging.info(f"Bot Session (@{bot_info.username}) connected successfully! (Role: Download Worker)")
            bot_client_pool.append(bot_client)

        except Exception as e:
            logging.error(f"FATAL BOT CONNECTION ERROR: {type(e).__name__}: {e}. Bot will remain disconnected.")
    
    # Critical Check
    if not owner_client:
         logging.critical("CRITICAL: Owner User Session could not connect. Metadata fetching will fail.")
    if not bot_client_pool:
         logging.critical("CRITICAL: No Bot Clients could connect. Downloading will fail.")
    if not owner_client and not bot_client_pool:
         logging.critical("CRITICAL: Service will be unavailable.")


@app.on_event("shutdown")
async def shutdown_event():
    global owner_client, bot_client_pool
    
    # Close Owner Client
    if owner_client:
        logging.info("Closing Owner Telegram Client connection...")
        await owner_client.disconnect()

    # Close Bot Clients
    for bot_client in bot_client_pool:
        if bot_client:
            try:
                name = (await bot_client.get_me()).username
            except:
                name = 'Bot'
            logging.info(f"Closing Telegram Bot Client connection for @{name}...")
            await bot_client.disconnect()


@app.get("/")
async def root():
    chunk_mb = OPTIMAL_CHUNK_SIZE // (1024 * 1024)
    total_buffer_mb = chunk_mb * BUFFER_CHUNK_COUNT
    
    owner_status = False
    if owner_client and owner_client.is_connected() and await owner_client.is_user_authorized():
        owner_status = True
        
    authorized_bots = 0
    for bot_client in bot_client_pool:
        if bot_client.is_connected():
            authorized_bots += 1

    if owner_status and authorized_bots > 0:
        status_msg = f"Streaming Proxy Active. Owner: Connected. Bots: {authorized_bots}/{len(BOT_TOKENS)}. Target Channel: {TEST_CHANNEL_ENTITY_USERNAME}. Chunk Size: {chunk_mb}MB. Buffer: {BUFFER_CHUNK_COUNT} chunks ({total_buffer_mb}MB). Metadata Cache TTL: {CACHE_TTL//60} mins."
    else:
        status_msg = f"Streaming Proxy Active. Owner: {owner_status}. Bots: {authorized_bots}/{len(BOT_TOKENS)}. (503 Service Unavailable)."

    return PlainTextResponse(f"Streaming Proxy Status: {status_msg}")


async def download_producer(
    client_instance: TelegramClient, # Ab yeh Bot Client hoga
    file_entity, 
    start_offset: int, 
    end_offset: int, 
    chunk_size: int, 
    queue: asyncio.Queue
):
    """Background mein Telegram se data download karke queue mein daalta hai (Producer)।"""
    offset = start_offset
    
    try:
        if hasattr(client_instance.session, 'filename'):
            client_name = client_instance.session.filename
        else:
            client_name = (await client_instance.get_me()).username # Bot Name
    except:
        client_name = 'client/bot'
        
    try:
        while offset <= end_offset:
            limit = min(chunk_size, end_offset - offset + 1)
            
            # CRITICAL: Yahan Bot Client, Owner se mile File Entity ko use karke download karta hai.
            async for chunk in client_instance.iter_download(
                file_entity, 
                offset=offset,
                limit=limit,
                chunk_size=chunk_size
            ):
                await queue.put(chunk)
                offset += len(chunk)

            if offset <= end_offset:
                 logging.error(f"PRODUCER BREAK ({client_name}): Offset reached {offset}, target was {end_offset}. Stopping.")
                 break 
        
    except (FileReferenceExpiredError, RPCError, TimeoutError, AuthKeyError) as e:
        # FileReferenceExpiredError aane par, cache miss/expired ho jana chahiye
        logging.error(f"PRODUCER CRITICAL ERROR ({client_name}): {type(e).__name__} during download.")
    except Exception as e:
        logging.error(f"PRODUCER UNHANDLED EXCEPTION ({client_name}): {e}")
    
    finally:
        await queue.put(None)


async def file_iterator(client_instance_for_download, file_entity_for_download, file_size, range_header, request: Request):
    """Queue se chunks nikalta hai aur FastAPI ko stream karta hai (Consumer)।"""
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
        download_producer(client_instance_for_download, file_entity_for_download, start, end, OPTIMAL_CHUNK_SIZE, queue)
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


# ----------------------------------------------------------------------
# FINAL FIX: stream_file_by_message_id (Metadata Owner se, Download Bot se, NameError fixed)
# ----------------------------------------------------------------------
@app.get("/api/stream/movie/{message_id}")
async def stream_file_by_message_id(message_id: str, request: Request):
    """Telegram Message ID se file stream karta hai।"""
    
    global owner_client, bot_client_pool
    
    # --- 1. Client Check aur Selection ---
    if owner_client is None or not bot_client_pool:
        raise HTTPException(status_code=503, detail="Telegram Clients are not connected or pool is empty.")

    metadata_client = owner_client
    download_client = get_next_bot_client() # Round-Robin se Bot chunna
    
    if download_client is None:
        raise HTTPException(status_code=503, detail="Bot Download Pool is empty.")
        
    try:
        metadata_client_name = metadata_client.session.filename if hasattr(metadata_client.session, 'filename') else 'owner'
        bot_client_name = (await download_client.get_me()).username
    except:
        metadata_client_name = 'owner'
        bot_client_name = 'bot_worker'

    logging.info(f"Metadata client: {metadata_client_name}. Download client: @{bot_client_name} for Message ID: {message_id}")
    
    # --- 2. Channel Resolution (Owner Client se) ---
    resolved_entity = None
    try:
        resolved_entity = await metadata_client.get_entity(TEST_CHANNEL_ENTITY_USERNAME)
        logging.info(f"RESOLVE SUCCESS: Channel entity resolved by {metadata_client_name}.")
    except Exception as e:
        logging.error(f"FATAL RESOLUTION ERROR ({metadata_client_name}): Could not resolve target channel entity: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"Could not resolve target channel entity using {metadata_client_name}.")
        
    # --- 3. Message ID Check ---
    try:
        file_id_int = int(message_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Message ID must be a valid integer.")

    file_size = 0
    file_title = f"movie_{message_id}.mkv" 
    file_entity_for_download = None 
    
    # 🌟 Caching check and Metadata Fetch (Owner Client se)
    current_time = time.time()
    cached_data = FILE_METADATA_CACHE.get(file_id_int) 
    
    if cached_data and (current_time - cached_data['timestamp']) < CACHE_TTL:
        logging.info(f"Cache HIT for message {file_id_int}. Skipping Telegram API call.")
        file_size = cached_data['size']
        file_title = cached_data['title']
        file_entity_for_download = cached_data['entity']
        
    else:
        # Cache Miss/Expired: API se naya data fetch karo
        logging.info(f"Cache MISS/EXPIRED for message {file_id_int}. Fetching metadata using {metadata_client_name}...")
        
        # --- METADATA FETCHING ---
        try:
            message = await metadata_client.get_messages(resolved_entity, ids=file_id_int) 
            
            media_entity = None
            if message and message.media:
                if hasattr(message.media, 'document') and message.media.document:
                    media_entity = message.media.document
                elif hasattr(message.media, 'video') and message.media.video:
                    media_entity = message.media.video
            
            if media_entity:
                file_size = media_entity.size
                file_entity_for_download = media_entity
                
                # CRITICAL CHECK: File reference hona anivarya hai
                if not hasattr(file_entity_for_download, 'file_reference') or not file_entity_for_download.file_reference:
                     logging.error(f"Metadata FAILED: File reference missing for message {file_id_int}. Bot cannot download.")
                     raise HTTPException(status_code=500, detail="Internal error: File reference missing from Telegram API response.")

                if media_entity.attributes:
                    for attr in media_entity.attributes:
                        if hasattr(attr, 'file_name'):
                            file_title = attr.file_name
                            break
                
                # 🌟 Cache the fetched data (entity ke saath file_reference bhi cache ho jayega)
                FILE_METADATA_CACHE[file_id_int] = {
                    'size': file_size,
                    'title': file_title,
                    'entity': file_entity_for_download,
                    'timestamp': current_time
                }
                logging.info(f"Metadata fetched and CACHED successfully for {file_id_int}.")
            else:
                logging.error(f"Metadata FAILED: Message {file_id_int} not found or no suitable media.")
                raise HTTPException(status_code=404, detail="File not found in the specified channel or is not a streamable media type.")

        except HTTPException:
            raise
        except Exception as e:
            logging.error(f"METADATA RESOLUTION ERROR ({metadata_client_name}): {type(e).__name__}: {e}.")
            FILE_METADATA_CACHE.pop(file_id_int, None) 
            raise HTTPException(status_code=500, detail="Internal error resolving Telegram file metadata.")
        
    
    # 4. Range Handling aur Headers 
    range_header = request.headers.get("range")
    
    content_type = "video/mp4" 
    if file_title.endswith(".mkv"): content_type = "video/x-matroska"
    elif file_title.endswith(".mp4"): content_type = "video/mp4"

    # 5. StreamingResponse (Download Client ke roop mein Bot ko pass karna)
    if range_header:
        # Partial Content (206) response
        try:
            start_str = range_header.split('=')[1].split('-')[0]
            start_range = int(start_str) if start_str else 0
        except:
            start_range = 0
            
        content_length = file_size - start_range
        
        headers = { # <--- Headers defined for 206
            "Content-Type": content_type,
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "Content-Range": f"bytes {start_range}-{file_size - 1}/{file_size}",
            "Content-Disposition": f"inline; filename=\"{file_title}\"",
            "Connection": "keep-alive"
        }
        return StreamingResponse(
            file_iterator(download_client, file_entity_for_download, file_size, range_header, request), 
            status_code=status.HTTP_206_PARTIAL_CONTENT,
            headers=headers
        )
    else:
        # Full content request (200) response
        
        # ✅ FIX: Yahan 'headers' ko define kiya gaya hai
        headers = { # <--- Headers defined for 200 (Fix)
            "Content-Type": content_type,
            "Content-Length": str(file_size),
            "Accept-Ranges": "bytes",
            "Content-Disposition": f"inline; filename=\"{file_title}\"",
            "Connection": "keep-alive"
        }
        
        return StreamingResponse(
            file_iterator(download_client, file_entity_for_download, file_size, None, request),
            headers=headers
        )
        
