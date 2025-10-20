from pyrogram import Client
from main.vars import Var

clients = []

async def initialize_clients():
    for i, token in enumerate(Var.MULTI_TOKENS):
        client = Client(
            f"multi_client_{i+1}",
            api_id=Var.API_ID,
            api_hash=Var.API_HASH,
            bot_token=token
        )
        await client.start()
        clients.append(client)
