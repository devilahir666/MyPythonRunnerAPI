# --- TELEGRAM STREAMING SERVER (CLOUD READY - ASYNC BUFFERING & LAZY CACHING) ---

# Import necessary libraries
from telethon.sessions import StringSession 
import asyncio
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import StreamingResponse, PlainTextResponse
from telethon import TelegramClient
# Error handling ke liye saare zaroori imports (FileReferenceExpiredError is the key)
from telethon.errors import RPCError, FileReferenceExpiredError, AuthKeyError, FloodWaitError, BotMethodInvalidError
import logging
import time
from typing import Dict, Any, Optional
import httpx 
import os 
# telethon ke entities jaise Document, Video ke liye Any type
from telethon.tl.types import Document, Channel

# ------------------------------------------

# Set up logging 
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger('telethon').setLevel(logging.WARNING)

# --- GLOBAL CONFIGURATION (USER SESSION & BOT POOL) ---

# 1. USER SESSION CONFIG (Owner - Sirf Connection Status ke liye, download mein use nahi hoga)
SESSION_FILE_OWNER = "owner.session"

USER_SESSION_CONFIG = {
    'api_id': 23692613,                                # Owner Account ki API ID
    'api_hash': "8bb69956d38a8226433186a199695f57",    # Owner Account ka API Hash
    'session_file': SESSION_FILE_OWNER, 
    'name': 'owner_client'
}

# 2. BOT POOL CONFIG (Download Workers)
# Yahan sirf bot tokens add karne hain
BOT_TOKENS = [
    "8075063062:AAH8lWaA7yk6ucGnV7N5F_U87nR9FRwKv98", # Bot 1
    "8175782707:AAEGhy1yEjnL9e583ruxLDdPuI5nZv_26MU",   # Bot 2
    # Naya bot add karne ke liye, token yahan daalein
]

# Global pool aur counter
owner_client: Optional[TelegramClient] = None 
bot_client_pool: list[TelegramClient] = []   
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
    
    # Counter reset karna overflow se bachne ke liye
    if global_bot_counter >= len(bot_client_pool) * 1000:
        global_bot_counter = 0

    return selected_client
# --------------------------------------------------------

# --- CONFIGURATION (LOW RAM & CACHING OPTIMIZATION) ---
TEST_CHANNEL_ENTITY_USERNAME = 'https://t.me/hPquYFblYHkxYzg1' 
OPTIMAL_CHUNK_SIZE = 1024 * 1024 * 2 # 2 MB chunk size

# ** PINGER CONFIGURATION **
PINGER_DELAY_SECONDS = 120
PUBLIC_SELF_PING_URL = "https://telegram-stream-proxy-x63x.onrender.com/" # Apni URL yahan update karein
# Metadata Caching Setup
FILE_METADATA_CACHE: Dict[int, Dict[str, Any]] = {}
CACHE_TTL = 3600 # 60 minutes tak cache rakhenge
# --- NEW CONFIG ---
MAX_REFRESH_ATTEMPTS = 5 # Maximum attempts to refresh file reference
# -------------------------------

app = FastAPI(title="Telethon Async Streaming Proxy (Final Robust Worker Model)")

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
# startup_event function (Client connections)
# ----------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    global owner_client, bot_client_pool
    logging.info("Attempting to connect Telegram Clients (User Session & Bot Pool)...") 

    asyncio.create_task(keep_alive_pinger())
    logging.info("PINGER: Keep-alive task scheduled.")

    # --- 1. USER SESSION CONNECTION (Optional, Status ke liye) ---
    config = USER_SESSION_CONFIG
    session_file_path = config.get('session_file')
    client_name = config.get('name')
    
    if os.path.exists(session_file_path):
        try:
            client_instance = TelegramClient(
                session=session_file_path, api_id=config['api_id'], api_hash=config['api_hash'], retry_delay=1
            )
            await client_instance.start()
            if await client_instance.is_user_authorized():
                 logging.info(f"User Session ({client_name}) connected and authorized successfully!")
                 owner_client = client_instance
            else:
                 logging.warning(f"User Session ({client_name}) failed to authorize.")
        except Exception as e:
            logging.error(f"FATAL CONNECTION ERROR for {client_name}: {type(e).__name__}: {e}.")

    # --- 2. BOT POOL CONNECTION (Workers) ---
    for token in BOT_TOKENS:
        try:
            bot_client = await TelegramClient(None, 
                                              api_id=USER_SESSION_CONFIG['api_id'], 
                                              api_hash=USER_SESSION_CONFIG['api_hash']).start(bot_token=token)
            
            bot_info = await bot_client.get_me()
            logging.info(f"Bot Session (@{bot_info.username}) connected successfully! (Role: Worker)")
            bot_client_pool.append(bot_client)

        except Exception as e:
            logging.error(f"FATAL BOT CONNECTION ERROR: {type(e).__name__}: {e}. Bot will remain disconnected.")
    
    # Critical Check
    if not bot_client_pool:
         logging.critical("CRITICAL: No Bot Clients could connect. Downloading will fail.")


@app.on_event("shutdown")
async def shutdown_event():
    # ... shutdown logic remains the same
    global owner_client, bot_client_pool
    
    if owner_client:
        logging.info("Closing Owner Telegram Client connection...")
        await owner_client.disconnect()

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
    # ... status check remains the same
    chunk_mb = OPTIMAL_CHUNK_SIZE // (1024 * 1024)
    
    owner_status = False
    if owner_client and owner_client.is_connected() and await owner_client.is_user_authorized():
        owner_status = True
        
    authorized_bots = sum(1 for bot_client in bot_client_pool if bot_client.is_connected())

    if authorized_bots > 0:
        status_msg = (
            f"Streaming Proxy Active. Owner: {owner_status}. Bots: {authorized_bots}/{len(BOT_TOKENS)}. "
            f"Target Channel: {TEST_CHANNEL_ENTITY_USERNAME}. Chunk Size: {chunk_mb}MB. "
            f"Metadata Cache TTL: {CACHE_TTL//60} mins. Refreshes: {MAX_REFRESH_ATTEMPTS} attempts."
        )
    else:
        status_msg = f"Streaming Proxy Active. Owner: {owner_status}. Bots: {authorized_bots}/{len(BOT_TOKENS)}. (503 Service Unavailable - No Workers)."

    return PlainTextResponse(f"Streaming Proxy Status: {status_msg}")


# ----------------------------------------------------------------------
# 🔥 CORE FIX: ROBUST STREAMING ITERATOR (Handles Refresh & Client Switch) 🔥
# ----------------------------------------------------------------------
async def download_stream_iterator(
    message_id_int: int,                # Original Message ID
    channel_entity: Any,                # Channel Entity
    initial_file_entity: Any,           # Initial Document/Video Entity (with first file_reference)
    file_size: int,
    range_header: Optional[str],
    request: Request
):
    """
    Asynchronously streams file chunks, handling FileReferenceExpiredError by refreshing
    the file entity and switching to a new bot client for each restart.
    """
    
    current_file_entity = initial_file_entity
    
    # --- Range Setup ---
    start_offset = 0
    end_offset = file_size - 1
    
    if range_header:
        try:
            range_value = range_header.split('=')[1]
            if '-' in range_value:
                start_str, end_str = range_value.split('-')
                start_offset = int(start_str) if start_str else 0
                end_offset = int(end_str) if end_str else file_size - 1
        except:
            logging.warning("STREAM ITERATOR: Invalid Range header format. Defaulting to full stream.")

    current_offset = start_offset
    refresh_count = 0
    
    logging.info(f"STREAM START: Range requested: {current_offset}-{end_offset}/{file_size}")

    while current_offset <= end_offset and refresh_count < MAX_REFRESH_ATTEMPTS:
        # 1. Select the download worker client (Round-Robin)
        # Har naya attempt ek naye bot se shuru hoga
        download_client = get_next_bot_client() 
        if not download_client:
            logging.error("STREAM ABORT: No active bot clients in the pool.")
            break
        
        try:
            client_name = (await download_client.get_me()).username
        except:
            client_name = 'bot_worker_unknown'

        try:
            logging.info(f"DOWNLOAD START (@{client_name}, Attempt {refresh_count + 1}): From offset {current_offset}. Remaining: {end_offset - current_offset + 1} bytes.")

            limit = end_offset - current_offset + 1
            
            # --- DOWNLOAD ATTEMPT ---
            async for chunk in download_client.iter_download(
                current_file_entity, 
                offset=current_offset,
                limit=limit,
                chunk_size=OPTIMAL_CHUNK_SIZE
            ):
                if await request.is_disconnected():
                    logging.info(f"CLIENT DISCONNECTED (@{client_name}): Stream terminated by user.")
                    return # Stop the generator
                
                yield chunk
                current_offset += len(chunk)

                if current_offset > end_offset:
                    break
            
            # Download successfully complete
            if current_offset > end_offset:
                logging.info(f"DOWNLOAD COMPLETE (@{client_name}): Successfully streamed up to offset {end_offset}.")
                return 
            
            # Agar loop khatam ho gaya but offset end tak nahi pahuncha (FileReferenceExpiredError ki assumption)
            logging.warning(f"DOWNLOAD WARNING (@{client_name}): iter_download finished prematurely at offset {current_offset}. Attempting reference refresh.")
            raise FileReferenceExpiredError("Simulated expiration for premature stop.")
            
        except FileReferenceExpiredError:
            refresh_count += 1
            logging.warning(f"REFRESH REQUIRED (@{client_name}): FileReferenceExpiredError caught at offset {current_offset}. Attempt {refresh_count}/{MAX_REFRESH_ATTEMPTS}. Fetching new reference...")
            
            if refresh_count >= MAX_REFRESH_ATTEMPTS:
                logging.error("STREAM FAILED: Maximum refresh attempts reached. Aborting stream.")
                break 
                
            # --- Refresh Logic (Metadata Fetch using next available client) ---
            try:
                # Refresh ke liye naya/agla client chunna
                refresh_client = get_next_bot_client() 
                if not refresh_client:
                    raise Exception("No client available for refreshing file reference.")
                    
                message = await refresh_client.get_messages(channel_entity, ids=message_id_int)
                
                new_entity = None
                if message and message.media:
                    if hasattr(message.media, 'document') and isinstance(message.media.document, Document):
                        new_entity = message.media.document
                    elif hasattr(message.media, 'video') and isinstance(message.media.video, Video):
                        new_entity = message.media.video
                        
                if new_entity and hasattr(new_entity, 'file_reference') and new_entity.file_reference:
                    current_file_entity = new_entity 
                    FILE_METADATA_CACHE.pop(message_id_int, None)
                    logging.info(f"REFRESH SUCCESS: Entity updated (Client: {await refresh_client.get_me()}). Restarting download from offset {current_offset}.")
                    continue # Restart while loop
                else:
                    logging.error("REFRESH FAILED: New file entity/reference is invalid after refresh.")
                    break

            except Exception as refresh_error:
                logging.error(f"REFRESH CRITICAL FAILURE: {type(refresh_error).__name__}: {refresh_error}. Aborting stream.")
                break
                
        except (RPCError, TimeoutError, AuthKeyError) as e:
            logging.error(f"DOWNLOAD RPC ERROR (@{client_name}): {type(e).__name__} at offset {current_offset}. Aborting stream.")
            break 
            
        except Exception as e:
            logging.error(f"DOWNLOAD UNHANDLED EXCEPTION (@{client_name}): {e}. Aborting stream.")
            break

    if current_offset <= end_offset:
        logging.error(f"STREAM FAILED TO COMPLETE: Iterator stopped before reaching the end offset ({current_offset}/{end_offset}).")


# ----------------------------------------------------------------------
# stream_file_by_message_id (Initial Metadata Fetch by the handling bot)
# ----------------------------------------------------------------------
@app.get("/api/stream/movie/{message_id}")
async def stream_file_by_message_id(message_id: str, request: Request):
    """Telegram Message ID se file stream karta hai।"""
    
    global bot_client_pool
    
    if not bot_client_pool:
        raise HTTPException(status_code=503, detail="Bot Download Pool is empty (Service Unavailable).")

    # --- 1. Client Check aur Selection ---
    # Jis bot ko request mili, wohi metadata fetch karega (Round-Robin)
    metadata_client = get_next_bot_client() 
    
    if metadata_client is None:
        raise HTTPException(status_code=503, detail="Bot Download Pool is empty.")
        
    try:
        bot_client_name = (await metadata_client.get_me()).username
    except:
        bot_client_name = 'bot_worker'

    logging.info(f"Request handled by: @{bot_client_name} for Message ID: {message_id}")
    
    # --- 2. Channel Resolution ---
    resolved_entity: Optional[Channel] = None
    try:
        resolved_entity = await metadata_client.get_entity(TEST_CHANNEL_ENTITY_USERNAME)
        logging.info(f"RESOLVE SUCCESS: Channel entity resolved by @{bot_client_name}.")
    except Exception as e:
        logging.error(f"FATAL RESOLUTION ERROR (@{bot_client_name}): Could not resolve target channel entity: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"Could not resolve target channel entity using @{bot_client_name}.")
        
    # --- 3. Message ID Check ---
    try:
        file_id_int = int(message_id) 
    except ValueError:
        raise HTTPException(status_code=400, detail="Message ID must be a valid integer.")

    file_size = 0
    file_title = f"movie_{message_id}.mkv" 
    file_entity_for_download = None 
    
    # 🌟 Caching check and Metadata Fetch (Wohi bot nikalega)
    current_time = time.time()
    cached_data = FILE_METADATA_CACHE.get(file_id_int) 
    range_header = request.headers.get("range")
    
    if cached_data and (current_time - cached_data['timestamp']) < CACHE_TTL:
        logging.info(f"Cache HIT for message {file_id_int}.")
        file_size = cached_data['size']
        file_title = cached_data['title']
        file_entity_for_download = cached_data['entity']
        
    else:
        # Cache Miss/Expired: API se naya data fetch karo (Handling bot se)
        logging.info(f"Cache MISS/EXPIRED for message {file_id_int}. Fetching metadata using @{bot_client_name}...")
        
        try:
            message = await metadata_client.get_messages(resolved_entity, ids=file_id_int) 
            
            media_entity = None
            if message and message.media:
                if hasattr(message.media, 'document') and message.media.document:
                    media_entity = message.media.document
                elif hasattr(message.media, 'video') and message.media.video:
                    media_entity = message.media.video
            
            if media_entity and hasattr(media_entity, 'file_reference') and media_entity.file_reference:
                file_size = media_entity.size
                file_entity_for_download = media_entity
                
                if media_entity.attributes:
                    for attr in media_entity.attributes:
                        if hasattr(attr, 'file_name'):
                            file_title = attr.file_name
                            break
                
                FILE_METADATA_CACHE[file_id_int] = {
                    'size': file_size,
                    'title': file_title,
                    'entity': file_entity_for_download,
                    'timestamp': current_time
                }
                logging.info(f"Metadata fetched and CACHED successfully for {file_id_int}.")
            else:
                logging.error(f"Metadata FAILED: Message {file_id_int} not found or no suitable media/reference.")
                raise HTTPException(status_code=404, detail="File not found or is not a streamable media type.")

        except HTTPException:
            raise
        except Exception as e:
            logging.error(f"METADATA RESOLUTION ERROR (@{bot_client_name}): {type(e).__name__}: {e}.")
            FILE_METADATA_CACHE.pop(file_id_int, None) 
            raise HTTPException(status_code=500, detail="Internal error resolving Telegram file metadata.")
        
    
    # 4. Content Type aur Headers 
    content_type = "application/octet-stream" 
    if file_title.lower().endswith((".mp4", ".m4v")): content_type = "video/mp4"
    elif file_title.lower().endswith((".mkv", ".webm")): content_type = "video/x-matroska"
    elif file_title.lower().endswith((".mp3", ".wav", ".ogg")): content_type = "audio/mpeg"

       # 5. StreamingResponse (Using the robust iterator)
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
            download_stream_iterator(
                file_id_int, 
                resolved_entity, 
                file_entity_for_download, 
                file_size, 
                range_header, 
                request
            ), 
            status_code=status.HTTP_206_PARTIAL_CONTENT,
            headers=headers
        )
    else:
        # Full content request (200) response
        headers = { 
            "Content-Type": content_type,
            "Content-Length": str(file_size),
            "Accept-Ranges": "bytes",
            "Content-Disposition": f"inline; filename=\"{file_title}\"",
            "Connection": "keep-alive"
        }
        
        return StreamingResponse(
            download_stream_iterator(
                file_id_int, 
                resolved_entity, 
                file_entity_for_download, 
                file_size, 
                None,
                request
            ),
            headers=headers
        )
