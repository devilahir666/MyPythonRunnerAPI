# --- TELEGRAM STREAMING SERVER (CLOUD READY - HARDCODED KEYS) ---

# Import necessary libraries
import asyncio
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import StreamingResponse, PlainTextResponse
from telethon import TelegramClient
from telethon.errors import RPCError, FileReferenceExpiredError, AuthKeyError
import logging
import pprint 

# Set up logging 
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger('telethon').setLevel(logging.WARNING)

# --- TELEGRAM CREDENTIALS (HARDCODED) ---
API_ID = 23692613
API_HASH = "8bb69956d38a8226433186a199695f57" 
BOT_TOKEN = "8075063062:AAH8lWaA7yk6ucGnV7N5F_U87nR9FRwKv98" 
SESSION_NAME = None 
# ------------------------------------------

# --- CONFIGURATION ---
TEST_CHANNEL_ENTITY_USERNAME = '@serverdata00'
# 16 MB chunk size set kiya gaya hai for maximum speed.
OPTIMAL_CHUNK_SIZE = 1024 * 1024 * 16 
# -------------------------------

app = FastAPI(title="Telethon Hardcoded Key Streaming Proxy")
client: TelegramClient = None
resolved_channel_entity = None 

@app.on_event("startup")
async def startup_event():
    global client, resolved_channel_entity
    logging.info("Attempting to connect Telegram Client...")
    
    try:
        client_instance = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        await client_instance.start(bot_token=BOT_TOKEN)
        client = client_instance
        
        if client and await client.is_user_authorized():
             logging.info("Telegram Client connected and authorized successfully!")
             
             logging.info(f"Resolving channel entity for {TEST_CHANNEL_ENTITY_USERNAME}...")
             resolved_channel_entity = await client.get_entity(TEST_CHANNEL_ENTITY_USERNAME)
             logging.info(f"Channel resolved successfully! Type: {type(resolved_channel_entity).__name__}")
             
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
    if client and await client.is_user_authorized():
        status_msg = f"Streaming Proxy Active. Streaming from channel: {TEST_CHANNEL_ENTITY_USERNAME}"
    else:
        status_msg = "Client is NOT connected/authorized. (503 Service Unavailable)."

    return PlainTextResponse(f"Streaming Proxy Status: {status_msg}")

async def file_iterator(file_entity_for_download, file_size, range_header, request: Request):
    """File ko Telegram se chunko mein download karta hai aur stream karta hai."""
    
    start = 0
    end = file_size - 1
    
    if range_header:
        try:
            range_value = range_header.split('=')[1]
            if '-' in range_value:
                start_str, end_str = range_value.split('-')
                start = int(start_str) if start_str else 0
                end = int(end_str) if end_str else file_size - 1
            logging.info(f"Streaming Range: bytes={start}-{end}")
        except Exception as e:
            logging.warning(f"Invalid Range header format: {e}")
            start = 0
            end = file_size - 1
    
    # Ab humne global constant use kiya
    chunk_size = OPTIMAL_CHUNK_SIZE 
    offset = start

    try:
        while offset <= end:
            limit = min(chunk_size, end - offset + 1)
            
            # Working logic: Direct Document/Video object use karna
            async for chunk in client.iter_download(
                file_entity_for_download, 
                offset=offset,
                limit=limit,
                chunk_size=chunk_size
            ):
                if await request.is_disconnected():
                    logging.info("Client disconnected during stream (Terminating iterator).")
                    return 
                
                yield chunk
                offset += len(chunk)

            if offset <= end:
                 logging.error(f"STREAM BREAK: Offset reached {offset}, target was {end}. Stopping.")
                 break 

    except (FileReferenceExpiredError, RPCError, TimeoutError, asyncio.CancelledError, AuthKeyError) as e:
        logging.error(f"CRITICAL STREAMING ERROR CAUGHT: {type(e).__name__} during download.")
        return 
    except Exception as e:
        logging.error(f"UNHANDLED EXCEPTION IN ITERATOR: {e}")
        return 

@app.get("/api/stream/movie/{message_id}")
async def stream_file_by_message_id(message_id: str, request: Request):
    """Telegram Message ID se file stream karta hai."""
    global client, resolved_channel_entity
    if client is None or resolved_channel_entity is None:
        raise HTTPException(status_code=503, detail="Telegram Client not connected or channel not resolved.")
        
    logging.info(f"Request received for Channel '{TEST_CHANNEL_ENTITY_USERNAME}', Message ID: {message_id}")
    
    try:
        file_id_int = int(message_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Message ID must be a valid integer.")

    file_entity_for_download = None 
    file_size = 0
    file_title = f"movie_{message_id}.mkv" # Default title

    # --- METADATA FETCHING: Working logic se Document ya Video reference nikalo ---
    try:
        logging.info(f"Fetching metadata for message {file_id_int} from resolved entity.")
        
        message = await client.get_messages(resolved_channel_entity, ids=file_id_int) 
        
        media_entity = None
        media_type = None
        
        if message and message.media:
            if hasattr(message.media, 'document') and message.media.document:
                media_entity = message.media.document
                media_type = "Document"
            elif hasattr(message.media, 'video') and message.media.video:
                media_entity = message.media.video
                media_type = "Video"
        
        if media_entity:
            file_size = media_entity.size
            file_entity_for_download = media_entity # Direct object pass
            
            # File title extraction
            if media_entity.attributes:
                for attr in media_entity.attributes:
                    if hasattr(attr, 'file_name'):
                        file_title = attr.file_name
                        break
            
            logging.info(f"Metadata SUCCESS via {media_type}: Title='{file_title}', Size={file_size} bytes.")
        else:
            logging.error(f"Metadata FAILED: Message {file_id_int} not found or no suitable media (Document/Video).")
            if message:
                logging.error("--- DIAGNOSTIC DUMP: MESSAGE OBJECT DETAILS ---")
                logging.error(pprint.pformat(message.to_dict()))
                logging.error("--- END DUMP ---")
            raise HTTPException(status_code=404, detail="File not found in the specified channel or is not a streamable media type.")

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"METADATA RESOLUTION ERROR: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Internal error resolving Telegram file metadata.")
    
    
    # 2. Range Handling aur Headers
    range_header = request.headers.get("range")
    
    content_type = "video/mp4" 
    if file_title.endswith(".mkv"): content_type = "video/x-matroska"
    elif file_title.endswith(".mp4"): content_type = "video/mp4"

    if range_header:
        try:
            start_range = int(range_header.split('=')[1].split('-')[0])
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
        # Full content request
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
