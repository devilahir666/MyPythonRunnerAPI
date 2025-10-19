# This file is a part of TG-Direct-Link-Generator


from ..vars import Var
from pyrogram import Client
from os import getcwd

StreamBot = Client(
    # ****** Yahan session_name ko name se badla gaya hai ******
    name="main",
    api_id=Var.API_ID,
    api_hash=Var.API_HASH,
    # workdir="./" fix barkaraar hai
    workdir="./", 
    plugins={"root": "main/bot/plugins"},
    bot_token=Var.BOT_TOKEN,
    sleep_threshold=Var.SLEEP_THRESHOLD,
    workers=Var.WORKERS,
)

multi_clients = {}
work_loads = {}
