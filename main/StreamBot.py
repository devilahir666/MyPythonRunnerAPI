from pyrogram import Client
from main.vars import Var

StreamBot = Client(
    "stream_bot",
    api_id=Var.API_ID,
    api_hash=Var.API_HASH,
    bot_token=Var.BOT_TOKEN
)
