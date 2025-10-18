# --- TELEGRAM STREAMING SERVER (CLOUD READY - MTPROTO HACK) ---

# Import necessary libraries
from telethon.sessions import StringSession 
import asyncio
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import StreamingResponse, PlainTextResponse
from telethon import TelegramClient
# MTProto Hack ke liye naya import
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputDocumentFileLocation, InputFileLocation
from telethon.errors import RPCError, FileReferenceExpiredError, FloodWaitError
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
OPTIMAL_CHUNK_SIZE = 1024 * 1024 * 2 # 2 MB chunk size 
BUFFER_CHUNK_COUNT = 4 

# ** PINGER CONFIGURATION **
PINGER_DELAY_SECONDS = 120
PUBLIC_SELF_PING_URL = "https://telegram-stream-proxy-x63x.onrender.com/" 
# Metadata Caching Setup (Sirf size aur title cache karega)
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


# 🚨 UPDATED DOWNLOAD PRODUCER WITH FRESH REFERENCE FETCH 🚨
async def download_producer(
    client_instance: TelegramClient, # Initial client (Bot)
    file_id_int: int, # Message ID integer
    channel_entity_username: str, # Channel username
    start_offset: int, 
    end_offset: int, 
    chunk_size: int, 
    queue: asyncio.Queue
):
    """
    Producer ab खुद Download शुरू करने से ठीक पहले File Reference Fetch करता है।
    यह File Reference Expiration के कारण होने वाली 0.00KB समस्या को ठीक करता है।
    """
    offset = start_offset
    MAX_RETRIES = 3 # Download RPC ke liye max retry attempts
    
    try:
        client_name = (await client_instance.get_me()).username
    except:
        client_name = 'bot_worker'
        
    logging.info(f"PRODUCER START (@{client_name}): Streaming range {start_offset} - {end_offset} for file {file_id_int}. Fetching FRESH File Reference...")
    
    # --- STEP 1: FRESH FILE REFERENCE FETCH ---
    file_entity = None
    try:
        # Channel entity resolve karna (producer ke andar)
        resolved_entity = await client_instance.get_entity(channel_entity_username)
        
        # Fresh message fetch karo
        messages = await client_instance.get_messages(resolved_entity, ids=file_id_int)
        
        if messages and messages[0] and messages[0].media:
            # Document ya Video entity nikalna
            file_entity = messages[0].media.document or messages[0].media.video
            
        if not file_entity or not hasattr(file_entity, 'file_reference') or not file_entity.file_reference:
            logging.error(f"PRODUCER FATAL: Message {file_id_int} is missing media or a valid File Reference.")
            await queue.put(None)
            return

        logging.info(f"PRODUCER REFERENCE SUCCESS: Fresh File Reference fetched by @{client_name}.")

    except Exception as e:
        logging.error(f"PRODUCER REFERENCE ERROR (@{client_name}): Could not fetch initial File Entity: {type(e).__name__}: {e}.")
        await queue.put(None)
        return

    # --- STEP 2: DOWNLOAD LOOP (USING FRESH REFERENCE) ---
    retry_count = 0
    while offset <= end_offset:
        
        limit = min(chunk_size, end_offset - offset + 1)
        
        # Location hamesha current, fresh file_entity se banegi
        location = InputDocumentFileLocation(
            id=file_entity.id,
            access_hash=file_entity.access_hash,
            file_reference=file_entity.file_reference
        )
        
        try:
            # --- MTProto Hack: Raw upload.GetFileRequest Call ---
            result = await client_instance(
                GetFileRequest(
                    location=location, 
                    offset=offset, 
                    limit=limit
                )
            )
            
            chunk = result.bytes 
            
            if not chunk and limit > 0:
                # 💥 SILENT FAILURE CATCH (0.00KB issue)
                if retry_count < MAX_RETRIES - 1:
                    # Retry karo, ho sakta hai network brief time ke liye hung ho
                    logging.warning(f"PRODUCER RETRY ({client_name}): Empty chunk received at offset {offset}. Retrying Download RPC... ({retry_count + 1}/{MAX_RETRIES})")
                    retry_count += 1
                    await asyncio.sleep(1.5) 
                    continue
                else:
                    logging.error(f"PRODUCER BREAK ({client_name}): Empty chunk received after max download retries. Stopping stream.")
                    break
            
            # --- Success ---
            await queue.put(chunk)
            offset += len(chunk)
            retry_count = 0 # Success, reset retry counter
        
        # 💥 FileReferenceExpiredError (Explicit Error)
        except FileReferenceExpiredError as e:
             # Agar download ke beech mein expire hota hai, toh yeh fatal hai
            logging.error(f"PRODUCER CRITICAL ERROR (@{client_name}): FILE REFERENCE EXPIRED DURING STREAMING. Stopping. {e}")
            break
            
        except RPCError as e:
            # Other RPC Errors (like FloodWait)
            logging.error(f"PRODUCER CRITICAL RPC ERROR (@{client_name}): Code={e.code}, Name={e.name}, Message={e.message}. Stopping stream.")
            break
            
        # 💥 Catch all other exceptions 
        except Exception as e:
            logging.error(f"PRODUCER UNHANDLED FATAL EXCEPTION (@{client_name}): {type(e).__name__}: {e}. Stopping stream.")
            break
    
    # FINAL: Queue ko hamesha band karo
    logging.info(f"PRODUCER END ({client_name}): Download loop finished.")
    await queue.put(None)


async def file_iterator(client_instance_for_download, file_id_int, file_size, range_header, request: Request):
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
    
    # PRODUCER अब केवल ID और Channel Username लेता है
    producer_task = asyncio.create_task(
        download_producer(client_instance_for_download, file_id_int, TEST_CHANNEL_ENTITY_USERNAME, start, end, OPTIMAL_CHUNK_SIZE, queue)
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
# 🔥 stream_file_by_message_id (Metadata Fetching ONLY for HTTP Headers)
# ----------------------------------------------------------------------
@app.get("/api/stream/movie/{message_id}")
async def stream_file_by_message_id(message_id: str, request: Request):
    """
    Message ID से file size और title fetch करता है (Headers के लिए)।
    File Reference fetching का काम अब Producer को दे दिया गया है।
    """
    
    global owner_client, bot_client_pool
    
    # --- 1. Client Check aur Selection ---
    if not bot_client_pool:
        raise HTTPException(status_code=503, detail="Bot Download Pool is empty.")

    # Metadata Fetching के लिए client select किया गया
    fetch_client = get_next_bot_client() 
    
    if fetch_client is None:
        raise HTTPException(status_code=503, detail="Bot Download Pool is empty.")
        
    try:
        bot_client_name = (await fetch_client.get_me()).username
    except:
        bot_client_name = 'bot_worker'

    logging.info(f"Metadata client: @{bot_client_name} for Message ID: {message_id}")
    
    # --- 2. Message ID Check ---
    try:
        file_id_int = int(message_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Message ID must be a valid integer.")

    file_size = 0
    file_title = f"movie_{message_id}.mkv" 
    
    # --- 3. METADATA FETCHING (ONLY SIZE/TITLE) ---
    logging.info(f"Fetching File Size/Title using @{bot_client_name}...")
    
    try:
        # BOT client से सिर्फ साइज़/टाइटल निकालने के लिए
        resolved_entity = await fetch_client.get_entity(TEST_CHANNEL_ENTITY_USERNAME)
        message = await fetch_client.get_messages(resolved_entity, ids=file_id_int) 
        
        media_entity = None
        if message and message.media:
            media_entity = message.media.document or message.media.video
        
        if media_entity and hasattr(media_entity, 'size'):
            file_size = media_entity.size
            if hasattr(media_entity, 'attributes'):
                for attr in media_entity.attributes:
                    if hasattr(attr, 'file_name'):
                        file_title = attr.file_name
                        break
            logging.info(f"Size/Title fetched successfully. Size: {file_size}")
        else:
            logging.error(f"Metadata FAILED: Message {file_id_int} not found or no suitable media.")
            raise HTTPException(status_code=404, detail="File not found in the specified channel.")

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"METADATA RESOLUTION ERROR (@{bot_client_name}): {type(e).__name__}: {e}.")
        raise HTTPException(status_code=500, detail="Internal error resolving Telegram file size/title.")
        
    
    # --- 4. Range Handling aur Headers ---
    range_header = request.headers.get("range")
    
    content_type = "video/mp4" 
    if file_title.lower().endswith(".mkv"): content_type = "video/x-matroska"
    elif file_title.lower().endswith(".mp4"): content_type = "video/mp4"
    # ... other content types ...


    # StreamingResponse
    if range_header:
        # Partial Content (206) response
        try:
            start_str = range_header.split('=')[1].split('-')[0]
            start_range = int(start_str) if start_str else 0
        except:
            start_range = 0
            
        content_length = file_size - start_range
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
            # Producer को अब सिर्फ ID और Channel Name पास किया गया है
            file_iterator(fetch_client, file_id_int, file_size, range_header, request), 
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
             # Producer को अब सिर्फ ID और Channel Name पास किया गया है
            file_iterator(fetch_client, file_id_int, file_size, range_header, request), 
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
            # Producer को अब सिर्फ ID और Channel Name पास किया गया है
            file_iterator(fetch_client, file_id_int, file_size, None, request),
            headers=headers
        )
