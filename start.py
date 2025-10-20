import sys
import asyncio
import logging
from .vars import Var
from aiohttp import web
from pyrogram import idle
from main import utils
from main import StreamBot
from main.server import web_server
from main.bot.clients import initialize_clients

logging.basicConfig(
    level=logging.INFO,
    datefmt="%d/%m/%Y %H:%M:%S",
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(stream=sys.stdout),
              logging.FileHandler("streambot.log", mode="a", encoding="utf-8")],
)

logging.getLogger("aiohttp").setLevel(logging.ERROR)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)

server = web.AppRunner(web_server())

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

async def start_services():
    print("\n-------------------- Initializing Telegram Bot --------------------")
    await StreamBot.start()
    bot_info = await StreamBot.get_me()
    StreamBot.username = bot_info.username
    print("------------------------------ DONE ------------------------------\n")

    print("---------------------- Initializing Clients ----------------------")
    await initialize_clients()
    print("------------------------------ DONE ------------------------------\n")

    if Var.ON_HEROKU:
        print("------------------ Starting Keep Alive Service ------------------")
        asyncio.create_task(utils.ping_server())

    print("--------------------- Initializing Web Server ---------------------")
    await server.setup()
    bind_address = "0.0.0.0" if Var.ON_HEROKU else Var.BIND_ADDRESS
    await web.TCPSite(server, bind_address, Var.PORT).start()
    print("------------------------------ DONE ------------------------------\n")

    print("------------------------- Service Started -------------------------")
    print(f"bot =>> {bot_info.first_name}")
    if bot_info.dc_id:
        print(f"DC ID =>> {bot_info.dc_id}")
    print(f"server ip =>> {bind_address}:{Var.PORT}")
    if Var.ON_HEROKU:
        print(f"app running on =>> {Var.FQDN}")
    print("------------------------------------------------------------------\n")

    await idle()

async def cleanup():
    await server.cleanup()
    await StreamBot.stop()

if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except KeyboardInterrupt:
        pass
    except Exception as err:
        logging.error(err)
    finally:
        loop.run_until_complete(cleanup())
        loop.stop()
        print("------------------------ Stopped Services ------------------------")
