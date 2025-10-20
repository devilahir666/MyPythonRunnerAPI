import asyncio
import aiohttp
from main.vars import Var

async def ping_server():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                await session.get(Var.URL)
        except Exception:
            pass
        await asyncio.sleep(Var.PING_INTERVAL)
