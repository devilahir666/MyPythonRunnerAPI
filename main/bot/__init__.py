# This file is a part of TG-Direct-Link-Generator


from ..vars import Var
from pyrogram import Client
from os import getcwd

StreamBot = Client(
    # Pyrogram v2.0 के लिए पहला आर्गुमेंट (नाम) ज़रूरी है।
    "main",  
    api_id=Var.API_ID,
    api_hash=Var.API_HASH,
    # v2.0 में 'workdir' की जगह 'storage_directory' का उपयोग होता है
    storage_directory="main", 
    plugins={"root": "main/bot/plugins"},
    bot_token=Var.BOT_TOKEN,
    sleep_threshold=Var.SLEEP_THRESHOLD,
    # 'workers' अब Client.__init__ में नहीं, बल्कि .run() या .start() में होता है।
    # इसलिए इसे कमेंट रहने दें।
)

multi_clients = {}
work_loads = {}
