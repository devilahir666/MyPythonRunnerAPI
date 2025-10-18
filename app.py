# --- TELEGRAM STREAMING SERVER (CLOUD READY - MTPROTO HACK) ---

# Import necessary libraries
from telethon.sessions import StringSession 
import asyncio
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import StreamingResponse, PlainTextResponse
from telethon import TelegramClient
# MTProto Hack ke liye naya import
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputDocumentFileLocation, InputFileLocation, InputPeerPhotoFileLocation, InputPhotoFileLocation, InputWebFileLocation
from telethon.errors import RPCError, FileReferenceExpiredError, AuthKeyError, FloodWaitError, BotMethodInvalidError
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

# 1. USER SESSION CONFIG (Owner - Sirf Connection Status ke liye)
SESSION_FILE_OWNER = "owner.session"

USER_SESSION_CONFIG = {
    'api_id': 23692613,                                # Owner Account ki API ID
    'api_hash': "8bb69956d38a8226433186a199695f57",    # Owner Account ka API Hash
    'session_file': SESSION_FILE_OWNER, 
    'name': 'owner_client'
}

# 2. BOT POOL CONFIG (Download Workers)
# Kripya yahan apne do ya zyada bot tokens daalein
BOT_TOKENS = [
    "8075063062:AAH8lWaA7yk6ucGnV7N5F_U87nR9FRwKv98", # Bot 1 (Example)
    "8175782707:AAEGhy1yEjnL9e583ruxLDdPuI5nZv_26MU",    # Bot 2 (Example)
"7811293412:AAFnE6GUxDkTIReP7tyUf4zGLMMoZ3jVR8k",
]

# Global pool aur counter
owner_client: Optional[TelegramClient] = None # Single User Client (Global access ke liye)
bot_client_pool: list[TelegramClient] = []   # Bot Clients ka Pool
global_bot_counter = 0                       # Bot pool ke liye Round-Robin counter

# --- Helper function to select client (Round-Robin) ---
def get_next_bot_client() -> Optional[TelegramClient]:
    """Round-Robin tarike se agla bot client chunnta hai (Download Worker)।"""
    global global_bot_counter
    
    # Filter out disconnected or placeholder tokens
    active_clients = [c for c in bot_client_pool if c.is_connected()]
    
    if not active_clients:
        return None
        
    client_index = global_bot_counter % len(active_clients)
    selected_client = active_clients[client_index]
    global_bot_counter += 1 
    
    if global_bot_counter >= len(active_clients) * 1000:
        global_bot_counter = 0

    return selected_client
# --------------------------------------------------------

# --- CONFIGURATION (LOW RAM & CACHING OPTIMIZATION) ---
TEST_CHANNEL_ENTITY_USERNAME = 'https://t.me/hPquYFblYHkxYzg1' 
OPTIMAL_CHUNK_SIZE = 1024 * 1024 * 2 # 2 MB chunk size (MTProto ke liye max 1MB se thoda zyada)
BUFFER_CHUNK_COUNT = 4 

# ** PINGER CONFIGURATION **
PINGER_DELAY_SECONDS = 120
PUBLIC_SELF_PING_URL = "https://telegram-stream-proxy-x63x.onrender.com/"
# Metadata Caching Setup (Ab sirf size aur title cache karega)
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
# UPDATED startup_event function
# ----------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    global owner_client, bot_client_pool
    logging.info("Attempting to connect Telegram Clients (User Session & Bot Pool)...") 

    asyncio.create_task(keep_alive_pinger())
    logging.info("PINGER: Keep-alive task scheduled.")

    # --- 1. USER SESSION CONNECTION (Owner - Sirf connection ke liye) ---
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
                 logging.info(f"User Session ({client_name}) connected and authorized successfully!")
                 owner_client = client_instance
            else:
                 logging.error(f"User Session ({client_name}) failed to authorize. (Session may be expired.)")
        except Exception as e:
            logging.error(f"FATAL CONNECTION ERROR for {client_name}: {type(e).__name__}: {e}. Client will remain disconnected.")

    # --- 2. BOT POOL CONNECTION (Workers) ---
    for token in BOT_TOKENS:
        if token.startswith("---") and token.endswith("---"):
            logging.warning("Skipping connection attempt: Found placeholder token.")
            continue
            
        try:
            # Bot token se connect karna
            bot_client = await TelegramClient(None, 
                                              api_id=USER_SESSION_CONFIG['api_id'], 
                                              api_hash=USER_SESSION_CONFIG['api_hash']).start(bot_token=token)
            
            bot_info = await bot_client.get_me()
            logging.info(f"Bot Session (@{bot_info.username}) connected successfully! (Role: Download Worker & Metadata Fetcher)")
            bot_client_pool.append(bot_client)

        except Exception as e:
            logging.error(f"FATAL BOT CONNECTION ERROR: {type(e).__name__}: {e}. Bot will remain disconnected.")
    
    # Critical Check
    if not owner_client:
         logging.critical("CRITICAL: Owner User Session could not connect. Service may be unstable.")
    if not bot_client_pool:
         logging.critical("CRITICAL: No Bot Clients could connect. Downloading will fail.")


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
        
    authorized_bots = sum(1 for bot_client in bot_client_pool if bot_client.is_connected())

    if authorized_bots > 0:
        status_msg = f"Streaming Proxy Active. Owner: {owner_status}. Bots: {authorized_bots}/{len(BOT_TOKENS)}. Target Channel: {TEST_CHANNEL_ENTITY_USERNAME}. Chunk Size: {chunk_mb}MB. Buffer: {BUFFER_CHUNK_COUNT} chunks ({total_buffer_mb}MB). Metadata Cache TTL: {CACHE_TTL//60} mins."
    else:
        status_msg = f"Streaming Proxy Active. Owner: {owner_status}. Bots: {authorized_bots}/{len(BOT_TOKENS)}. (503 Service Unavailable)."

    return PlainTextResponse(f"Streaming Proxy Status: {status_msg}")


# 🚨 UPDATED DOWNLOAD PRODUCER (MTProto HACK - Raw GetFileRequest) 🚨
async def download_producer(
    client_instance: TelegramClient, # Initial client (Bot)
    file_entity, # Yeh ab Document ya Video object hoga
    file_id_int: int, # Message ID integer (logging ke liye)
    start_offset: int, 
    end_offset: int, 
    chunk_size: int, 
    queue: asyncio.Queue
):
    """
    Background mein Telegram se data download karke queue mein daalta hai (Producer)।
    Yeh direct MTProto `upload.GetFileRequest` call karta hai sirf maangi hui bytes ke liye।
    """
    offset = start_offset
    
    try:
        client_name = (await client_instance.get_me()).username
    except:
        client_name = 'bot_worker'
        
    logging.info(f"PRODUCER START (@{client_name}): Streaming range {start_offset} - {end_offset} for file {file_id_int}.")
    
    # --- FIX START: Correctly generate the MTProto File Location ---
    location: Optional[InputFileLocation] = None
    
    # Media entity (Document/Video) se location nikalna
    if hasattr(file_entity, 'id') and hasattr(file_entity, 'access_hash') and hasattr(file_entity, 'file_reference'):
        # Ye 'InputDocumentFileLocation' ke liye hai
        location = InputDocumentFileLocation(
            id=file_entity.id,
            access_hash=file_entity.access_hash,
            file_reference=file_entity.file_reference
        )
    else:
        logging.error(f"PRODUCER CRITICAL ERROR (@{client_name}): File entity is missing required MTProto attributes (id/hash/ref).")
        await queue.put(None)
        return
        
    # --- FIX END: Correctly generate the MTProto File Location ---
    
    try:
        while offset <= end_offset:
            
            # Kitne bytes maangne hain (end_offset tak ya OPTIMAL_CHUNK_SIZE)
            limit = min(chunk_size, end_offset - offset + 1)
            
            # --- MTProto Hack: Raw upload.GetFileRequest Call ---
            result = await client_instance(
                GetFileRequest(
                    location=location, 
                    offset=offset, 
                    limit=limit
                )
            )
            
            # Telegram se bytes (.bytes) extract karna
            chunk = result.bytes 
            
            if not chunk:
                # Agar Telegram ne empty response di aur hum end_offset tak nahi pahuche, toh error.
                logging.error(f"PRODUCER BREAK ({client_name}): Empty chunk received at offset {offset}. Stopping stream.")
                break 
            
            await queue.put(chunk)
            offset += len(chunk)
        
        logging.info(f"PRODUCER END ({client_name}): Successfully reached end_offset {end_offset}.")
        
    # 💥 FileReferenceExpiredError aane par, stream band kar denge.
    except FileReferenceExpiredError as e:
        logging.error(f"PRODUCER CRITICAL ERROR (@{client_name}): FILE REFERENCE EXPIRED. Need to re-fetch metadata. Stopping stream. {e}")
        
    except RPCError as e:
        # RPC Error ko detail mein log karo
        logging.error(f"PRODUCER CRITICAL RPC ERROR (@{client_name}): Code={e.code}, Name={e.name}, Message={e.message}. Stopping stream.")
        
    except (TimeoutError, AuthKeyError) as e:
        logging.error(f"PRODUCER CRITICAL ERROR (@{client_name}): Connection/Auth Error: {type(e).__name__}. Stopping stream.")
        
    except Exception as e:
        logging.error(f"PRODUCER UNHANDLED EXCEPTION (@{client_name}): {type(e).__name__}: {e}")
    
    # FINAL: Queue ko hamesha band karo
    await queue.put(None)


async def file_iterator(client_instance_for_download, file_entity_for_download, file_id_int, file_size, range_header, request: Request):
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
        download_producer(client_instance_for_download, file_entity_for_download, file_id_int, start, end, OPTIMAL_CHUNK_SIZE, queue)
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
# 🔥 stream_file_by_message_id (Metadata Fetching)
# ----------------------------------------------------------------------
@app.get("/api/stream/movie/{message_id}")
async def stream_file_by_message_id(message_id: str, request: Request):
    """
    Telegram Message ID se file stream karta hai.
    FIX: Har request par, selected bot hamesha fresh file reference fetch karega.
    """
    
    global owner_client, bot_client_pool
    
    # --- 1. Client Check aur Selection ---
    if not bot_client_pool:
        raise HTTPException(status_code=503, detail="Bot Download Pool is empty.")

    # Download client Round-Robin se select kiya gaya
    download_client = get_next_bot_client() 
    
    if download_client is None:
        raise HTTPException(status_code=503, detail="Bot Download Pool is empty.")
        
    try:
        bot_client_name = (await download_client.get_me()).username
    except:
        bot_client_name = 'bot_worker'

    logging.info(f"Download/Metadata client: @{bot_client_name} for Message ID: {message_id}")
    
    # --- 2. Channel Resolution (AB BOT CLIENT SE) ---
    resolved_entity = None
    try:
        # Channel entity resolve karna (yahan koi reference issue nahi hai)
        resolved_entity = await download_client.get_entity(TEST_CHANNEL_ENTITY_USERNAME)
        logging.info(f"RESOLVE SUCCESS: Channel entity resolved by @{bot_client_name}.")
    except Exception as e:
        logging.error(f"FATAL RESOLUTION ERROR (@{bot_client_name}): {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"Could not resolve target channel entity using @{bot_client_name}.")
        
    # --- 3. Message ID Check ---
    try:
        file_id_int = int(message_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Message ID must be a valid integer.")

    file_size = 0
    file_title = f"movie_{message_id}.mkv" 
    file_entity_for_download = None 
    
    # --- 4. CACHE CHECK (Only size/title ke liye) ---
    current_time = time.time()
    cached_data = FILE_METADATA_CACHE.get(file_id_int) 
    
    if cached_data and (current_time - cached_data['timestamp']) < CACHE_TTL:
        logging.info(f"Cache HIT for message {file_id_int}. Using cached size/title.")
        file_size = cached_data['size']
        file_title = cached_data['title']
    
    # --- 5. METADATA FETCHING (JUGAD: Always fetch entity for current bot) ---
    logging.info(f"Fetching fresh File Reference using current client (@{bot_client_name})...")
    
    try:
        # BOT client se fresh reference aur metadata le rahe hain
        message = await download_client.get_messages(resolved_entity, ids=file_id_int) 
        
        media_entity = None
        if message and message.media:
            if hasattr(message.media, 'document') and message.media.document:
                media_entity = message.media.document
            elif hasattr(message.media, 'video') and message.media.video:
                # Agar video entity hai, to hum usko hi use kar sakte hain
                media_entity = message.media.video
            
            # Agar sirf Photo (thumbnail) hai toh woh streamable nahi hota, but hum fir bhi document/video hi dekhenge
            if not media_entity and hasattr(message.media, 'document'):
                 media_entity = message.media.document
            
        
        if media_entity:
            # Agar cache miss/expired tha, toh fresh size aur title update karo
            if file_size == 0 and hasattr(media_entity, 'size'):
                file_size = media_entity.size
                
                # File title Document/Video attributes se nikalna
                if hasattr(media_entity, 'attributes'):
                    for attr in media_entity.attributes:
                        if hasattr(attr, 'file_name'):
                            file_title = attr.file_name
                            break
            
            # CRITICAL: File Reference current client ke liye fresh hona anivarya hai
            if not hasattr(media_entity, 'file_reference') or not media_entity.file_reference:
                 logging.error(f"Metadata FAILED: File reference missing for message {file_id_int}. Bot cannot download.")
                 raise HTTPException(status_code=500, detail="Internal error: File reference missing.")
            
            # Current client ke liye fresh entity set karo
            file_entity_for_download = media_entity
            
            # Cache ko sirf size/title/timestamp ke liye update karo
            FILE_METADATA_CACHE[file_id_int] = {
                'size': file_size,
                'title': file_title,
                'timestamp': current_time
            }
            logging.info(f"Fresh File Reference fetched by @{bot_client_name}. Size: {file_size}")
        else:
            logging.error(f"Metadata FAILED: Message {file_id_int} not found or no suitable media.")
            raise HTTPException(status_code=404, detail="File not found in the specified channel or is not a streamable media type.")

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"METADATA RESOLUTION ERROR (@{bot_client_name}): {type(e).__name__}: {e}.")
        FILE_METADATA_CACHE.pop(file_id_int, None) 
        raise HTTPException(status_code=500, detail="Internal error resolving Telegram file metadata.")
          # 6. Range Handling aur Headers (Remaining logic same rahega)
    range_header = request.headers.get("range")
    
    content_type = "video/mp4" 
    if file_title.lower().endswith(".mkv"): content_type = "video/x-matroska"
    elif file_title.lower().endswith(".mp4"): content_type = "video/mp4"
    elif file_title.lower().endswith(".webm"): content_type = "video/webm"
    elif file_title.lower().endswith(".avi"): content_type = "video/x-msvideo"
    elif file_title.lower().endswith((".jpg", ".jpeg")): content_type = "image/jpeg"
    elif file_title.lower().endswith(".png"): content_type = "image/png"


    # StreamingResponse (Ab file_entity_for_download mein hamesha current bot ka valid reference hai)
    if range_header:
        # Partial Content (206) response
        try:
            start_str = range_header.split('=')[1].split('-')[0]
            start_range = int(start_str) if start_str else 0
        except:
            start_range = 0
            
        content_length = file_size - start_range
        
         # Ab hum 'file_size - 1' tak hi jayenge, na ki file_size tak.
        # HTTP range 0-based indexing use karta hai (e.g., 0-999 for 1000 bytes).
        end_range_for_header = file_size - 1 
        
        headers = { 
            "Content-Type": content_type,
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "Content-Range": f"bytes {start_range}-{end_range_for_header}/{file_size}",
            "Content-Disposition": f"inline; filename=\"{file_title}\"",
            "Connection": "keep-alive"
        }
        return StreamingResponse(
            file_iterator(download_client, file_entity_for_download, file_id_int, file_size, range_header, request), 
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
            file_iterator(download_client, file_entity_for_download, file_id_int, file_size, None, request),
            headers=headers
        )
