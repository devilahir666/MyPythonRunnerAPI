# This file is a part of TG-Direct-Link-Generator

import sys
import asyncio
import logging
import signal
from aiohttp import web
from pyrogram import idle

from main.vars import Var
from main import utils
from main.server import web_server
from main.bot.clients import initialize_clients
from main.StreamBot import StreamBot  # updated import

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    datefmt="%d/%m/%Y %H:%M:%S",
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(stream=sys.stdout),
              logging.FileHandler("streambot.log", mode="a", encoding="utf-8")]
)
logging.getLogger("aiohttp").setLevel(logging.ERROR)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)

# Web server
server = web.AppRunner(web_server())

# Graceful shutdown on SIGTERM (Render)
def sigterm_handler():
    print("SIGTERM received. Shutting down bot and server...")
    asyncio.create_task(cleanup())

signal.signal(signal.SIGTERM, lambda s,f: sigterm_handler())

async def start_services():
    print("\n-------------------- Initializing Telegram Bot --------------------")
    await StreamBot.start()
    bot_info = await StreamBot.get_me()
    StreamBot.username = bot_info.username
    print("------------------------------ DONE ------------------------------\n")

    print("---------------------- Initializing Clients ----------------------")
    await initialize_clients()
    print("------------------------------ DONE ------------------------------")

    if Var.ON_HEROKU:
        print("------------------ Starting Keep Alive Service ------------------\n")
        asyncio.create_task(utils.ping_server())

    print("--------------------- Initializing Web Server ---------------------")
    await server.setup()
    bind_address = "0.0.0.0" if Var.ON_HEROKU else Var.BIND_ADDRESS
    await web.TCPSite(server, bind_address, Var.PORT).start()
    print("------------------------------ DONE ------------------------------\n")

    print("------------------------- Service Started -------------------------")
    print(f"                        bot =>> {bot_info.first_name}")
    if bot_info.dc_id:
        print(f"                        DC ID =>> {bot_info.dc_id}")
    print(f"                        server ip =>> {bind_address}:{Var.PORT}")
    if Var.ON_HEROKU:
        print(f"                        app running on =>> {Var.FQDN}")
    print("------------------------------------------------------------------\n")

    print("""
 _____________________________________________   
|                                             |  
|          Deployed Successfully              |  
|              Join @TechZBots                |
|_____________________________________________|
    """)

    await idle()


async def cleanup():
    print("-------------------- Cleaning up --------------------")
    try:
        await StreamBot.stop()
    except Exception as e:
        logging.error(f"Error stopping bot: {e}")
    try:
        await server.cleanup()
    except Exception as e:
        logging.error(f"Error stopping server: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(start_services())  # <- updated
    except KeyboardInterrupt:
        pass
    except Exception as err:
        logging.error(err)
    finally:
        print("------------------------ Stopped Services ------------------------")
