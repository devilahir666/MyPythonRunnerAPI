# This file is a part of TG-Direct-Link-Generator


from ..vars import Var
from pyrogram import Client
from os import getcwd

StreamBot = Client(
    # v2.0 में 'name' पहला positional argument है।
    "main",  
    api_id=Var.API_ID,
    api_hash=Var.API_HASH,
    # v2.0 में 'workdir' की जगह 'storage_directory' का उपयोग होता है
    storage_directory="main", 
    plugins={"root": "main/bot/plugins"},
    bot_token=Var.BOT_TOKEN,
    sleep_threshold=Var.SLEEP_THRESHOLD,
    # 'workers' v2.0 में Client.__init__ का हिस्सा नहीं है।
    # इसलिए इसे हटा दें।
)

multi_clients = {}
work_loads = {}
