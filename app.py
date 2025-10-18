# --- TELEGRAM REDIRECT SERVER (CLOUD READY - ZERO LOAD) ---

# Import necessary libraries
from telethon.sessions import StringSession 
import asyncio
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import RedirectResponse, PlainTextResponse
from telethon import TelegramClient
# Ab hume CdnRedirectError ko pakadna hoga!
from telethon.errors import RPCError, FileReferenceExpiredError, AuthKeyError, FloodWaitError, BotMethodInvalidError
from telethon.errors.rpcerrorlist import CdnRedirectError 
from telethon.tl.functions.upload import GetFileRequest # RPC call ke liye
from telethon.tl.types import InputDocumentFileLocation, InputWebFileLocation, InputEncryptedFileLocation, InputFileLocation
import logging
import time
from typing import Dict, Any, Optional
import httpx 
import os 
from urllib.parse import urlencode, urlparse, parse_qs

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
    "8075063062:AAH8lWaA7yk6ucGn77N5F_U87nR9FRwKv98", # Bot 1 (Example)
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
    
    active_clients = [c for c in bot_client_pool if c and c.is_connected()]
    
    if not active_clients:
        return None
        
    client_index = global_bot_counter % len(active_clients)
    selected_client = active_clients[client_index]
    global_bot_counter += 1 
    
    if global_bot_counter >= len(active_clients) * 1000:
        global_bot_counter = 0

    return selected_client
# --------------------------------------------------------

# --- CONFIGURATION (CACHING & PINGER) ---
TEST_CHANNEL_ENTITY_USERNAME = 'https://t.me/hPquYFblYHkxYzg1' 
# Pinger config same
PINGER_DELAY_SECONDS = 120
PUBLIC_SELF_PING_URL = "https://telegram-stream-proxy-x63x.onrender.com/"
# Metadata Caching Setup
FILE_METADATA_CACHE: Dict[int, Dict[str, Any]] = {}
CACHE_TTL = 3600 # 60 minutes tak cache rakhenge

app = FastAPI(title="Telethon Async Redirect Proxy (Zero Load)")

# --- Helper function: Keep Alive Pinger (SAME AS BEFORE) ---
async def keep_alive_pinger():
    """Render Free Tier ko inactive hone se rokne ke liye self-ping karta hai।"""
    logging.info(f"PINGER: Background keep-alive task started (interval: {PINGER_DELAY_SECONDS}s).")
    async with httpx.AsyncClient(timeout=10) as client_http: 
        while True:
            await asyncio.sleep(PINGER_DELAY_SECONDS)
            try:
                response = await client_http.get(PUBLIC_SELF_PING_URL) 
                if response.status_code == 200:
                    logging.info(f"PINGER: Self-Heartbeat sent successfully.")
                else:
                    logging.warning(f"PINGER: Self-Heartbeat FAILED (Status: {response.status_code}).")
            except httpx.HTTPError as e:
                 logging.error(f"PINGER: Self-Heartbeat HTTP Error: {type(e).__name__} during request.")
            except Exception as e:
                 logging.error(f"PINGER: Self-Heartbeat General Error: {type(e).__name__}: {e}")

# --- UPDATED startup_event and shutdown_event (SAME AS BEFORE) ---
@app.on_event("startup")
async def startup_event():
    # ... (Connection logic - SAME AS BEFORE) ...
    global owner_client, bot_client_pool
    logging.info("Attempting to connect Telegram Clients (User Session & Bot Pool)...") 
    asyncio.create_task(keep_alive_pinger())
    # ... (Owner client connection) ...
    config = USER_SESSION_CONFIG
    session_file_path = config.get('session_file')
    client_name = config.get('name')
    if not os.path.exists(session_file_path):
         logging.critical(f"FATAL CONNECTION ERROR for {client_name}: Session file {session_file_path} NOT FOUND.")
    else:
        try:
            client_instance = TelegramClient(
                session=session_file_path, api_id=config['api_id'], api_hash=config['api_hash'], retry_delay=1
            )
            await client_instance.start()
            if await client_instance.is_user_authorized():
                 logging.info(f"User Session ({client_name}) connected and authorized successfully!")
                 owner_client = client_instance
            else:
                 logging.error(f"User Session ({client_name}) failed to authorize.")
        except Exception as e:
            logging.error(f"FATAL CONNECTION ERROR for {client_name}: {type(e).__name__}: {e}.")

    # ... (Bot pool connection) ...
    for token in BOT_TOKENS:
        if token.startswith("---") and token.endswith("---"):
            logging.warning("Skipping connection attempt: Found placeholder token.")
            continue
        try:
            bot_client = await TelegramClient(None, 
                                              api_id=USER_SESSION_CONFIG['api_id'], 
                                              api_hash=USER_SESSION_CONFIG['api_hash']).start(bot_token=token)
            bot_info = await bot_client.get_me()
            logging.info(f"Bot Session (@{bot_info.username}) connected successfully!")
            bot_client_pool.append(bot_client)
        except Exception as e:
            logging.error(f"FATAL BOT CONNECTION ERROR: {type(e).__name__}: {e}. Bot will remain disconnected.")
    
    if not owner_client: logging.critical("CRITICAL: Owner User Session could not connect.")
    if not bot_client_pool: logging.critical("CRITICAL: No Bot Clients could connect. Redirection will fail.")

@app.on_event("shutdown")
async def shutdown_event():
    # ... (Disconnection logic - SAME AS BEFORE) ...
    global owner_client, bot_client_pool
    if owner_client: await owner_client.disconnect()
    for bot_client in bot_client_pool:
        if bot_client:
            try: name = (await bot_client.get_me()).username
            except: name = 'Bot'
            logging.info(f"Closing Telegram Bot Client connection for @{name}...")
            await bot_client.disconnect()

# --- Root endpoint (SAME AS BEFORE) ---
@app.get("/")
async def root():
    owner_status = False
    if owner_client and owner_client.is_connected() and await owner_client.is_user_authorized():
        owner_status = True
    authorized_bots = sum(1 for bot_client in bot_client_pool if bot_client.is_connected())
    status_msg = f"Redirect Proxy Active. Owner: {owner_status}. Bots: {authorized_bots}/{len(BOT_TOKENS)}. Target Channel: {TEST_CHANNEL_ENTITY_USERNAME}. Metadata Cache TTL: {CACHE_TTL//60} mins."
    if authorized_bots == 0:
         status_msg += " (503 Service Unavailable)."
    return PlainTextResponse(f"Redirect Proxy Status: {status_msg}")

# ----------------------------------------------------------------------
# 🔥 NEW CORE LOGIC: CDN URL GENERATOR (Redirect)
# ----------------------------------------------------------------------

# Helper to construct the CDN URL (SIMULATED/EDUCATIONAL - MAY NEED ADJUSTMENT)
def construct_cdn_url(dc_id: int, file_token: bytes, file_reference: bytes, offset: int = 0, limit: int = 1048576) -> str:
    """
    CdnRedirectError se mili values ka istemal karke CDN URL banata hai.
    NOTE: CDN URL format Telegram ke internal logic par nirbhar karta hai.
    """
    
    # ⚠️ Yeh ek aasan/simulated format hai. Real format me auth_key, limit aur offset ka use hota hai.
    # Repos se mili jaankari ke aadhaar par isko badla ja sakta hai.
    
    # Example format (may be outdated/incorrect): 
    # https://tlgd.me/file/{file_reference}/{file_token}?dc={dc_id}...
    
    file_ref_hex = file_reference.hex()
    file_token_hex = file_token.hex()
    
    # Using the standard CDN URL structure (as researched from clients)
    base_url = f"https://cdn{dc_id}.telegram.org/file/{file_token_hex}/{file_ref_hex}"

    # Query parameters mein offset aur limit jaise parameters ki zaroorat pad sakti hai 
    # Agar hum range header support karna chahte hain toh.
    
    # Simple URL for full redirect (No range header support at redirect level)
    # Most players will handle the redirect and then request the file from the new location.
    
    return base_url 

# --- CORE FUNCTION: GENERATE REDIRECT URL ---
async def generate_redirect_url(client: TelegramClient, file_entity, start_offset: int = 0) -> Optional[str]:
    """
    Attempt to get a CDN URL by forcing a CDN_REDIRECT error and handling the response.
    """
    if not isinstance(file_entity.location, (InputDocumentFileLocation, InputWebFileLocation, InputEncryptedFileLocation, InputFileLocation)):
        logging.error("File entity location not suitable for GetFileRequest.")
        return None

    # Hamein sirf file location chahiye, koi data download nahi karna. Limit 1 rakhte hain.
    try:
        await client(GetFileRequest(
            location=file_entity.location, 
            offset=start_offset, # agar range header hai toh start_offset bhejenge
            limit=1,
            # 'cdn_supported' flag True hona chahiye (Telethon by default karta hai)
        ))
        
        # Agar yahan tak pahunche aur error nahi aayi, toh redirect nahi hoga.
        logging.warning("GetFileRequest did not trigger CDN_REDIRECT. Direct URL generation impossible.")
        return None 

    except CdnRedirectError as e:
        # 🥳 SUCCESS: CDN redirect error mil gayi! Yahi woh chaabi hai!
        logging.info(f"CDN_REDIRECT caught. DC ID: {e.new_dc_id}. File Token received.")
        
        # NOTE: e.file_token is the crucial signed token for the CDN.
        
        # CDN URL construct karo
        # File reference hamein original file_entity se milega
        
        if not hasattr(file_entity.location, 'file_reference') or not file_entity.location.file_reference:
             logging.error("File reference missing after CDN_REDIRECT.")
             return None
             
        # CDN URL construct karne ke liye e.file_token, e.new_dc_id, aur file_entity.location.file_reference ka istemal
        try:
             # Final URL construction
             final_cdn_url = construct_cdn_url(
                 dc_id=e.new_dc_id, 
                 file_token=e.file_token, 
                 file_reference=file_entity.location.file_reference,
                 offset=start_offset
             )
             return final_cdn_url
             
        except Exception as url_e:
             logging.error(f"Error constructing CDN URL: {url_e}")
             return None

    except Exception as e:
        logging.error(f"Unhandled error during redirect attempt: {type(e).__name__}: {e}")
        return None

# ----------------------------------------------------------------------
# UPDATED stream_file_by_message_id (Redirect Response)
# ----------------------------------------------------------------------
@app.get("/api/stream/movie/{message_id}")
async def stream_file_by_message_id(message_id: str, request: Request):
    
    global owner_client, bot_client_pool
    
    # 1. Client Check aur Selection
    download_client = get_next_bot_client() 
    if download_client is None:
        raise HTTPException(status_code=503, detail="Bot Download Pool is empty or disconnected.")
        
    try:
        bot_client_name = (await download_client.get_me()).username
    except:
        bot_client_name = 'bot_worker'

    logging.info(f"Redirect client: @{bot_client_name} for Message ID: {message_id}")
    
    # 2. Channel Resolution & Message ID Check (SAME AS BEFORE)
    try:
        resolved_entity = await download_client.get_entity(TEST_CHANNEL_ENTITY_USERNAME)
        file_id_int = int(message_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error resolving channel/message ID.")
        
    # 3. METADATA FETCHING (Always fresh entity for current bot)
    file_entity_for_download = None 
    
    try:
        # BOT client se fresh reference aur metadata le rahe hain
        message = await download_client.get_messages(resolved_entity, ids=file_id_int) 
        
        if message and message.media:
            # Document/Video entity find karo
            media_entity = message.media.document or message.media.video
        
            if media_entity:
                file_entity_for_download = media_entity
                file_size = media_entity.size
                file_title = next((attr.file_name for attr in media_entity.attributes if hasattr(attr, 'file_name')), f"movie_{message_id}.mkv")
                
                logging.info(f"Metadata OK. Attempting Redirect...")
            else:
                raise HTTPException(status_code=404, detail="File not found or is not streamable media.")
        else:
             raise HTTPException(status_code=404, detail="Message not found.")

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"METADATA RESOLUTION ERROR: {type(e).__name__}: {e}.")
        raise HTTPException(status_code=500, detail="Internal error resolving Telegram file metadata.")
        
    
    # 4. Range Handling (Partial Content)
    start_offset = 0
    range_header = request.headers.get("range")
    if range_header:
        try:
            start_str = range_header.split('=')[1].split('-')[0]
            start_offset = int(start_str) if start_str else 0
        except Exception:
            # Invalid range header, full stream/redirect
            pass 
    
    # 5. CORE LOGIC: Generate Redirect URL
    redirect_url = await generate_redirect_url(download_client, file_entity_for_download, start_offset)
    
    if redirect_url:
        logging.info(f"REDIRECT SUCCESS: Sending 302 to CDN.")
        # Client ko naye CDN URL par bhej do
        
        # Range header ko yahan se pass nahi kiya ja sakta, 
        # Client khud naye URL par Range request bhejega.
        
        # Content-Range header ko handle karna zaroori nahi hai kyunki hum redirect kar rahe hain
        
        # Use 307 (Temporary Redirect) for better compliance with HTTP standards for non-GET requests, 
        # but 302 is often used for simplicity. We'll use 302 as it's common for media links.
        
        return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)
    
    else:
         # Fallback: Agar CDN URL generate nahi ho paya, toh service fail (ya aap yahan 
        # purana Streaming Proxy logic wapas daal sakte hain)
        logging.critical("CRITICAL: Failed to generate CDN redirect URL. Zero Load solution FAILED.")
        raise HTTPException(status_code=503, detail="Service Unavailable: Could not generate direct download link.")
