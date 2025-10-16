# --- TELEGRAM STREAMING SERVER (CLOUD READY - ASYNC BUFFERING & LAZY CACHING) ---

# Import necessary libraries
from telethon.sessions import StringSession
import asyncio
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import StreamingResponse, PlainTextResponse
from telethon import TelegramClient
# FloodWaitError handling ke liye FloodWaitError import kiya
from telethon.errors import RPCError, FileReferenceExpiredError, AuthKeyError, FloodWaitError
import logging
import time # Time module cache ke liye
from typing import Dict, Any, Optional
# --- Naya import: Self-ping ke liye httpx ---
import httpx 
# --- Naye User Session ke liye imports ---
import os 
# ------------------------------------------

# Set up logging 
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger('telethon').setLevel(logging.WARNING)

# --- GLOBAL CONFIGURATION (USER SESSIONS) ---
# 🚨 WARNING: Yeh aapki private data hai. Hamesha surakshit rakhein!

# 🌟 FIX: Ab hum sessions ko files se load karenge 🌟
# String ko hataakar file paths use karein:
SESSION_FILE_OWNER = "owner.session"
SESSION_FILE_ADMIN = "admin.session"


# Ye purani hardcoded session strings ki lines hata di gayi hain.

CONFIGS = [
    {
        'api_id': 23692613,                                # Owner Account ki API ID
        'api_hash': "8bb69956d38a8226433186a199695f57",    # Owner Account ka API Hash
        'session_file': SESSION_FILE_OWNER, # File path use karein
        'name': 'owner_client'
    },
    {
        'api_id': 29243403,                                # Admin Account ki Doosri API ID 
        'api_hash': "1adc479f6027488cdd13259028056b73",          # Admin Account ka Doosra API Hash 
        'session_file': SESSION_FILE_ADMIN, # File path use karein
        'name': 'admin_client'
    },
]

# Global pool aur counter (Client pool: pehle 'client' tha, ab list hai)
client_pool: list[TelegramClient] = []
global_client_counter = 0

# --- Helper function to select client (Round-Robin) ---
def get_next_client() -> Optional[TelegramClient]:
    """Round-Robin tarike se agla client chunnta hai."""
    global global_client_counter
    if not client_pool:
        return None
        
    client_index = global_client_counter % len(client_pool)
    selected_client = client_pool[client_index]
    global_client_counter += 1 
    
    # ensure ki counter bahut bada na ho jaye
    if global_client_counter >= len(client_pool) * 1000:
        global_client_counter = 0

    return selected_client
# --------------------------------------------------------

# --- CONFIGURATION (LOW RAM & CACHING OPTIMIZATION) ---
TEST_CHANNEL_ENTITY_USERNAME = '@serverdata00'
# Chunk size 2MB (Low latency)
OPTIMAL_CHUNK_SIZE = 1024 * 1024 * 2 # 2 MB chunk size
# Buffer 4 chunks (Low RAM usage)
BUFFER_CHUNK_COUNT = 4 

# ** NEW: PINGER CONFIGURATION **
PINGER_DELAY_SECONDS = 120 # 2 minutes (Render Free Tier ke liye optimal)

# 🚨 RENDER SELF-PING URL (Aapka Public URL) 🚨
PUBLIC_SELF_PING_URL = "https://telegram-stream-proxy-x63x.onrender.com/"
# 🌟 JUGAD 1: Metadata Caching Setup
FILE_METADATA_CACHE: Dict[int, Dict[str, Any]] = {}
CACHE_TTL = 3600 # 60 minutes tak cache rakhenge
# -------------------------------

app = FastAPI(title="Telethon Async Streaming Proxy (User Session Pool)")
# client: TelegramClient = None # Removed single client
resolved_channel_entity: Optional[Any] = None 
entity_resolve_lock = asyncio.Lock()


async def keep_alive_pinger():
    """
    Render Free Tier ko inactive hone se rokne ke liye har 2 minute mein 
    apne hi PUBLIC endpoint par heartbeat (self-ping) bhejta hai.
    """
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
# UPDATED startup_event function (File Loading Logic)
# --- FIX FOR: ValueError: Not a valid string. ---
# ----------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    global client_pool
    logging.info("Attempting to connect Telegram User Session Pool...")

    asyncio.create_task(keep_alive_pinger())
    logging.info("PINGER: Keep-alive task scheduled.")

    for config in CONFIGS:
        session_file_path = config.get('session_file') # File path nikalenge
        client_name = config.get('name')

        # 1. File se Base64 string load karo
        session_b64_string = None
        try:
            with open(session_file_path, 'r') as f:
                # File se string read karo aur uske hidden characters hata do
                # .strip() aur .replace('\r', '') se sabhi line breaks aur whitespace hat jayenge
                session_b64_string = f.read().strip().replace('\n', '').replace('\r', '')
                
            if not session_b64_string:
                logging.error(f"FATAL CONNECTION ERROR for {client_name}: Session file {session_file_path} is empty.")
                continue

        except FileNotFoundError:
            logging.critical(f"FATAL CONNECTION ERROR for {client_name}: Session file {session_file_path} NOT FOUND in repo. Did you commit it?")
            continue
        except Exception as e:
            logging.critical(f"FATAL CONNECTION ERROR for {client_name}: Error reading file: {e}")
            continue
            
        # 2. Telethon se connect karo
        try:
            # FIX: Clean string ko StringSession object mein badlo
            session_obj = StringSession(session_b64_string)
            
            # Client object banao
            client_instance = TelegramClient(
                session=session_obj,
                api_id=config['api_id'], 
                api_hash=config['api_hash'],
                retry_delay=1
            )
            
            # User Session start
            await client_instance.start()
            
            if await client_instance.is_user_authorized():
                 logging.info(f"User Session ({client_name}) connected and authorized successfully!")
                 client_pool.append(client_instance)
            else:
                 logging.error(f"User Session ({client_name}) failed to authorize. (Check API ID/Hash match)")

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
            # Check kiya ki get_update_info hai ya nahi
            try:
                name = client_instance.session.get_update_info().get('name', 'session')
            except:
                name = 'session'
                
            logging.info(f"Closing Telegram Client connection for {name}...")
            await client_instance.disconnect()


@app.get("/")
async def root():
    chunk_mb = OPTIMAL_CHUNK_SIZE // (1024 * 1024)
    total_buffer_mb = chunk_mb * BUFFER_CHUNK_COUNT
    
    authorized_clients = 0
    
    # Check current status of the pool
    for client_instance in client_pool:
        try:
            if await client_instance.is_user_authorized():
                authorized_clients += 1
        except Exception:
            pass
            
    if authorized_clients > 0:
        status_msg = f"Streaming Proxy Active. Clients: {authorized_clients}/{len(CONFIGS)}. Target Channel: {TEST_CHANNEL_ENTITY_USERNAME}. Chunk Size: {chunk_mb}MB. Buffer: {BUFFER_CHUNK_COUNT} chunks ({total_buffer_mb}MB). Metadata Cache TTL: {CACHE_TTL//60} mins."
    else:
        status_msg = f"Client Pool is NOT connected/authorized ({len(client_pool)} sessions connected, {authorized_clients} authorized). (503 Service Unavailable)."

    return PlainTextResponse(f"Streaming Proxy Status: {status_msg}")


async def _get_or_resolve_channel_entity(client_instance: TelegramClient):
    """Channel entity ko resolve karta hai aur global variable mein cache karta hai (Lazy Caching)।"""
    global resolved_channel_entity
    
    # 1. Global cache hit check (All clients share the same resolved entity)
    if resolved_channel_entity:
        return resolved_channel_entity

    # 2. Lazy Resolve
    async with entity_resolve_lock:
        # Double check after acquiring lock
        if resolved_channel_entity:
            return resolved_channel_entity
            
        # Check kiya ki session object mein get_update_info hai ya nahi
        try:
            client_name = client_instance.session.get_update_info().get('name', 'client')
        except:
            client_name = 'client'
            
        logging.info(f"LAZY RESOLVE: Resolving channel entity for {TEST_CHANNEL_ENTITY_USERNAME} using {client_name}...")
        try:
            resolved_channel_entity = await client_instance.get_entity(TEST_CHANNEL_ENTITY_USERNAME)
            logging.info(f"LAZY RESOLVE SUCCESS: Channel resolved and cached.")
            return resolved_channel_entity
        except Exception as e:
            logging.error(f"LAZY RESOLVE FAILED: Could not resolve target channel entity: {e}")
            raise HTTPException(status_code=500, detail="Could not resolve target channel entity.")


async def download_producer(
    client_instance: TelegramClient, # Selected client instance
    file_entity, 
    start_offset: int, 
    end_offset: int, 
    chunk_size: int, 
    queue: asyncio.Queue
):
    """
    Background mein Telegram se data download karke queue mein daalta hai (Producer)।
    """
    offset = start_offset
    
    # Check kiya ki session object mein get_update_info hai ya nahi
    try:
        client_name = client_instance.session.get_update_info().get('name', 'client')
    except:
        client_name = 'client'
        
    try:
        while offset <= end_offset:
            limit = min(chunk_size, end_offset - offset + 1)
            
            # Telethon se download ki request
            async for chunk in client_instance.iter_download( # client_instance use kiya
                file_entity, 
                offset=offset,
                limit=limit,
                chunk_size=chunk_size
            ):
                # Chunk ko queue mein daalo
                await queue.put(chunk)
                offset += len(chunk)

            if offset <= end_offset:
                 # Agar loop break ho gaya aur target nahi pahucha, toh error log karein
                 logging.error(f"PRODUCER BREAK ({client_name}): Offset reached {offset}, target was {end_offset}. Stopping.")
                 break 
        
    except (FileReferenceExpiredError, RPCError, TimeoutError, AuthKeyError) as e:
        logging.error(f"PRODUCER CRITICAL ERROR ({client_name}): {type(e).__name__} during download.")
    except Exception as e:
        logging.error(f"PRODUCER UNHANDLED EXCEPTION ({client_name}): {e}")
    
    finally:
        # Download poora hone par Sentinel (None) daal do
        await queue.put(None)


async def file_iterator(client_instance_for_download, file_entity_for_download, file_size, range_header, request: Request):
    """
    Queue se chunks nikalta hai aur FastAPI ko stream karta hai (Consumer)।
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
    
    # Producer task ko background mein chalao (Ab client_instance_for_download pass hoga)
    producer_task = asyncio.create_task(
        download_producer(client_instance_for_download, file_entity_for_download, start, end, OPTIMAL_CHUNK_SIZE, queue)
    )
    
    try:
        # Consumer loop: Queue se chunks nikal kar yield karo
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


@app.get("/api/stream/movie/{message_id}")
async def stream_file_by_message_id(message_id: str, request: Request):
    """Telegram Message ID se file stream karta hai।"""
    
    # --- 1. Client Selection ---
    client_instance = get_next_client() # Round-Robin se client select karo
    if client_instance is None:
        raise HTTPException(status_code=503, detail="Telegram Client Pool is not connected or empty.")
        
    # Check kiya ki session object mein get_update_info hai ya nahi
    try:
        client_name = client_instance.session.get_update_info().get('name', 'client')
    except:
        client_name = 'client'
        
    logging.info(f"Using client: {client_name} for Message ID: {message_id}")
    
    # --- 2. Channel Resolution ---
    # Resolved entity ab client_instance ka upyog karega
    resolved_entity = await _get_or_resolve_channel_entity(client_instance) 
        
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
            # client_instance ka upyog karke message fetch karo
            message = await client_instance.get_messages(resolved_entity, ids=file_id_int) 
            
            media_entity = None
            if message and message.media:
                if hasattr(message.media, 'document') and message.media.document:
                    media_entity = message.media.document
                elif hasattr(message.media, 'video') and message.media.video:
                    media_entity = message.media.video
            
            if media_entity:
                file_size = media_entity.size
                file_entity_for_download = media_entity # Direct object pass
                
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
                    'timestamp': current_time # Kab cache kiya gaya
                }
                logging.info(f"Metadata fetched and CACHED successfully for {file_id_int}.")
            else:
                logging.error(f"Metadata FAILED: Message {file_id_int} not found or no suitable media.")
                raise HTTPException(status_code=404, detail="File not found in the specified channel or is not a streamable media type.")

        except HTTPException:
            raise
        except Exception as e:
            logging.error(f"METADATA RESOLUTION ERROR ({client_name}): {type(e).__name__}: {e}")
            FILE_METADATA_CACHE.pop(file_id_int, None) 
            raise HTTPException(status_code=500, detail="Internal error resolving Telegram file metadata.")
        
    
    # 3. Range Handling aur Headers (file_iterator mein client_instance pass hoga)
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
            # client_instance pass kiya
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
            # client_instance pass kiya
            file_iterator(client_instance, file_entity_for_download, file_size, None, request),
            headers=headers
        )
                    
