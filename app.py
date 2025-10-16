# --- TELEGRAM STREAMING SERVER (CLOUD READY - ASYNC BUFFERING & LAZY CACHING) ---

# Import necessary libraries
# StringSession import rakha hai, agar aap bhavishya mein Base64 use karna chahein.
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

# --- GLOBAL CONFIGURATION (USER SESSIONS) ---
# WARNING: Yeh aapki private data hai. Hamesha surakshit rakhein!

# Ab yeh session files ka path hai (jo aapne commit ki hain)
SESSION_FILE_OWNER = "owner.session"
SESSION_FILE_ADMIN = "admin.session"

CONFIGS = [
    {
        'api_id': 23692613,                                # Owner Account ki API ID
        'api_hash': "8bb69956d38a8226433186a199695f57",    # Owner Account ka API Hash
        'session_file': SESSION_FILE_OWNER, 
        'name': 'owner_client'
    },
    {
        'api_id': 29243403,                                # Admin Account ki Doosri API ID 
        'api_hash': "1adc479f6027488cdd13259028056b73",    # Admin Account ka Doosra API Hash 
        'session_file': SESSION_FILE_ADMIN,
        'name': 'admin_client'
    },
]

# Global pool aur counter
client_pool: list[TelegramClient] = []
global_client_counter = 0

# --- GLOBAL VARIABLES HATA DIYE GAYE HAIN ---
# 🚨 resolved_channel_entity: Optional[Any] = None 
# 🚨 entity_resolve_lock = asyncio.Lock()
# -------------------------------------------

# --- Helper function to select client (Round-Robin) ---
def get_next_client() -> Optional[TelegramClient]:
    """Round-Robin tarike se agla client chunnta hai।"""
    global global_client_counter
    if not client_pool:
        return None
        
    client_index = global_client_counter % len(client_pool)
    selected_client = client_pool[client_index]
    global_client_counter += 1 
    
    if global_client_counter >= len(client_pool) * 1000:
        global_client_counter = 0

    return selected_client
# --------------------------------------------------------

# --- CONFIGURATION (LOW RAM & CACHING OPTIMIZATION) ---
# --- CONFIGURATION (LOW RAM & CACHING OPTIMIZATION) ---
# FINAL FIX: Private channel ko Invite Link Hash se access karein
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

app = FastAPI(title="Telethon Async Streaming Proxy (User Session Pool)")

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
# UPDATED startup_event function (Using DIRECT SQLite Session Files)
# ----------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    global client_pool
    logging.info("Attempting to connect Telegram User Session Pool using SQLite Files...") 

    asyncio.create_task(keep_alive_pinger())
    logging.info("PINGER: Keep-alive task scheduled.")

    for config in CONFIGS:
        session_file_path = config.get('session_file')
        client_name = config.get('name')
        
        # 1. File Existence Check
        if not os.path.exists(session_file_path):
             logging.critical(f"FATAL CONNECTION ERROR for {client_name}: Session file {session_file_path} NOT FOUND in repo. Did you commit it?")
             continue

        # 2. Telethon से connect करो
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
                 client_pool.append(client_instance)
            else:
                 logging.error(f"User Session ({client_name}) failed to authorize. (Session may be expired. Regenerate the {session_file_path} file.)")

        except FloodWaitError as e:
             logging.error(f"FATAL CONNECTION ERROR for {client_name}: FloodWaitError: Too many connection attempts. Wait {e.seconds} seconds.")
             continue
        except Exception as e:
            logging.error(f"FATAL CONNECTION ERROR for {client_name}: {type(e).__name__}: {e}. Client will remain disconnected.")
            continue
    
    if not client_pool:
         logging.critical("CRITICAL: No Telegram User Sessions could connect. Service will be unavailable.")


@app.on_event("shutdown")
async def shutdown_event():
    global client_pool
    for client_instance in client_pool:
        if client_instance:
            try:
                name = client_instance.session.filename if hasattr(client_instance.session, 'filename') else 'session'
            except:
                name = 'session'
                
            logging.info(f"Closing Telegram Client connection for {name}...")
            await client_instance.disconnect()


@app.get("/")
async def root():
    chunk_mb = OPTIMAL_CHUNK_SIZE // (1024 * 1024)
    total_buffer_mb = chunk_mb * BUFFER_CHUNK_COUNT
    
    authorized_clients = 0
    
    for client_instance in client_pool:
        try:
            if client_instance.is_connected() and await client_instance.is_user_authorized():
                authorized_clients += 1
        except Exception:
            pass
            
    if authorized_clients > 0:
        status_msg = f"Streaming Proxy Active. Clients: {authorized_clients}/{len(CONFIGS)}. Target Channel: {TEST_CHANNEL_ENTITY_USERNAME}. Chunk Size: {chunk_mb}MB. Buffer: {BUFFER_CHUNK_COUNT} chunks ({total_buffer_mb}MB). Metadata Cache TTL: {CACHE_TTL//60} mins."
    else:
        status_msg = f"Client Pool is NOT connected/authorized ({len(client_pool)} sessions connected, {authorized_clients} authorized). (503 Service Unavailable)."

    return PlainTextResponse(f"Streaming Proxy Status: {status_msg}")


# 🚨 _get_or_resolve_channel_entity function ko hata diya gaya hai. 🚨


async def download_producer(
    client_instance: TelegramClient, 
    file_entity, 
    start_offset: int, 
    end_offset: int, 
    chunk_size: int, 
    queue: asyncio.Queue
):
    """Background mein Telegram se data download karke queue mein daalta hai (Producer)।"""
    offset = start_offset
    
    try:
        client_name = client_instance.session.filename if hasattr(client_instance.session, 'filename') else 'client'
    except:
        client_name = 'client'
        
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
                 logging.error(f"PRODUCER BREAK ({client_name}): Offset reached {offset}, target was {end_offset}. Stopping.")
                 break 
        
    except (FileReferenceExpiredError, RPCError, TimeoutError, AuthKeyError) as e:
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
# ✅ FINAL FIX: stream_file_by_message_id (Channel Resolution Fix)
# ----------------------------------------------------------------------
@app.get("/api/stream/movie/{message_id}")
async def stream_file_by_message_id(message_id: str, request: Request):
    """Telegram Message ID se file stream karta hai।"""
    
    # --- 1. Client Selection ---
    client_instance = get_next_client()
    if client_instance is None:
        raise HTTPException(status_code=503, detail="Telegram Client Pool is not connected or empty.")
        
    try:
        client_name = client_instance.session.filename if hasattr(client_instance.session, 'filename') else 'client'
    except:
        client_name = 'client'
        
    logging.info(f"Using client: {client_name} for Message ID: {message_id}")
    
    # 🚨 FIX: Channel Resolution ab har client अपने लिए खुद करेगा 🚨
    resolved_entity = None
    try:
        # Client ab seedhe channel entity ko resolve karega
        resolved_entity = await client_instance.get_entity(TEST_CHANNEL_ENTITY_USERNAME)
        logging.info(f"RESOLVE SUCCESS: Channel entity resolved by {client_name}.")
    except Exception as e:
        # Agar resolution fail hota hai to yahan 500 return hoga
        logging.error(f"FATAL RESOLUTION ERROR ({client_name}): Could not resolve target channel entity: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"Could not resolve target channel entity using {client_name}. Error: {type(e).__name__}")
        
    # --- 3. Message ID Check ---
    try:
        file_id_int = int(message_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Message ID must be a valid integer.")

    file_size = 0
    file_title = f"movie_{message_id}.mkv" 
    file_entity_for_download = None 
    
    
    # 🌟 JUGAD 2: Check Caching
    current_time = time.time()
    cached_data = FILE_METADATA_CACHE.get(file_id_int) 
    
    if cached_data and (current_time - cached_data['timestamp']) < CACHE_TTL:
        logging.info(f"Cache HIT for message {file_id_int}. Skipping Telegram API call.")
        file_size = cached_data['size']
        file_title = cached_data['title']
        file_entity_for_download = cached_data['entity']
        
    else:
        # Cache Miss/Expired: API se naya data fetch karo
        logging.info(f"Cache MISS/EXPIRED for message {file_id_int}. Fetching metadata using {client_name}...")
        
        # --- METADATA FETCHING ---
        try:
            # Resolved entity abhi upar resolve ho chuka hai
            message = await client_instance.get_messages(resolved_entity, ids=file_id_int) 
            
            media_entity = None
            if message and message.media:
                if hasattr(message.media, 'document') and message.media.document:
                    media_entity = message.media.document
                elif hasattr(message.media, 'video') and message.media.video:
                    media_entity = message.media.video
            
            if media_entity:
                file_size = media_entity.size
                file_entity_for_download = media_entity
                
                if media_entity.attributes:
                    for attr in media_entity.attributes:
                        if hasattr(attr, 'file_name'):
                            file_title = attr.file_name
                            break
                
                # 🌟 JUGAD 3: Cache the fetched data
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
            # Yehi woh error hai jo admin ke liye aaya tha!
            logging.error(f"METADATA RESOLUTION ERROR ({client_name}): {type(e).__name__}: {e}. (Possible permission/access issue.)")
            FILE_METADATA_CACHE.pop(file_id_int, None) 
            raise HTTPException(status_code=500, detail="Internal error resolving Telegram file metadata.")
        
    
    # 4. Range Handling aur Headers 
    range_header = request.headers.get("range")
    
    content_type = "video/mp4" 
    if file_title.endswith(".mkv"): content_type = "video/x-matroska"
    elif file_title.endswith(".mp4"): content_type = "video/mp4"

    if range_header:
        # Partial Content (206) response
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
            file_iterator(client_instance, file_entity_for_download, file_size, range_header, request), 
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
            file_iterator(client_instance, file_entity_for_download, file_size, None, request),
            headers=headers
                    )
        
