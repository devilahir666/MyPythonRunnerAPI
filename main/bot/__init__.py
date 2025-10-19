# This file is a part of TG-Direct-Link-Generator


from ..vars import Var
from pyrogram import Client
from os import getcwd

StreamBot = Client(
    # Pyrogram v1.x के लिए 'session_name' का उपयोग करें
    session_name="main",  
    api_id=Var.API_ID,
    api_hash=Var.API_HASH,
    # v1.x में 'storage_directory' की जगह 'workdir' का उपयोग होता है
    workdir="main", 
    plugins={"root": "main/bot/plugins"},
    bot_token=Var.BOT_TOKEN,
    sleep_threshold=Var.SLEEP_THRESHOLD,
    # workers को भी कमेंट रहने दें
)

multi_clients = {}
work_loads = {}
