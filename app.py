# --- TELEGRAM STREAMING SERVER (CLOUD READY - ASYNC BUFFERING) ---

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
BOT_TOKEN = "8075063062:AAH8lWaA7yk6ucGnU7N5F_U87nR9FRwKv98" 
SESSION_NAME = None 
# ------------------------------------------

# --- CONFIGURATION ---
TEST_CHANNEL_ENTITY_USERNAME = '@serverdata00'
OPTIMAL_CHUNK_SIZE = 1024 * 1024 * 16 # 16 MB chunk size
# Buffer mein 2 chunks (32MB) aage se download hoke ready rahenge
BUFFER_CHUNK_COUNT = 2 
# -------------------------------

app = FastAPI(title="Telethon Async Streaming Proxy")
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


async def download_producer(
    client_instance: TelegramClient,
    file_entity, 
    start_offset: int, 
    end_offset: int, 
    chunk_size: int, 
    queue: asyncio.Queue
):
    """
    Background mein Telegram se data download karke queue mein daalta hai.
    Yeh producer task consumer (file_iterator) se alag chalta hai.
    """
    offset = start_offset
    
    try:
        while offset <= end_offset:
            limit = min(chunk_size, end_offset - offset + 1)
            
            # Telethon se download ki request (working logic)
            async for chunk in client_instance.iter_download(
                file_entity, 
                offset=offset,
                limit=limit,
                chunk_size=chunk_size
            ):
                # Chunk ko queue mein daalo (Jahan se consumer uthayega)
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
        # Download poora hone par Sentinel (None) daal do
        await queue.put(None)


async def file_iterator(file_entity_for_download, file_size, range_header, request: Request):
    """
    Queue se chunks nikalta hai aur FastAPI ko stream karta hai.
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
            logging.info(f"Streaming Range: bytes={start}-{end}")
        except Exception as e:
            logging.warning(f"Invalid Range header format: {e}")
            start = 0
            end = file_size - 1

    # Buffer queue banao
    queue = asyncio.Queue(maxsize=BUFFER_CHUNK_COUNT)
    
    # Producer task ko background mein chalao
    producer_task = asyncio.create_task(
        download_producer(client, file_entity_for_download, start, end, OPTIMAL_CHUNK_SIZE, queue)
    )
    
    try:
        # Consumer loop: Queue se chunks nikal kar yield karo
        while True:
            # wait for data from the producer
            chunk = await queue.get()
            
            # None sentinel means the download is complete or failed
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
        # Ensure the background producer task is cleaned up
        if not producer_task.done():
            producer_task.cancel()
        
        # Ab koi error nahi aayega, kyunki producer abhi bhi chal raha hoga
        # humne ensure kar liya ki producer_task.cancel() call ho
        if not producer_task.cancelled():
             await asyncio.gather(producer_task, return_exceptions=True)


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

    # --- METADATA FETCHING: Document ya Video reference nikalo ---
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

    # Baaki headers same rahenge, bas iterator change hoga
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
