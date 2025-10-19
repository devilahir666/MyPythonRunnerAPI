# Taken from megadlbot_oss <https://github.com/eyaadh/megadlbot_oss/blob/master/mega/webserver/__init__.py>
# Thanks to Eyaadh <https://github.com/eyaadh>
# This file is a part of TG-Direct-Link-Generator

from aiohttp import web
from .stream_routes import routes


# Naya function: Sirf OK response dene ke liye
async def ping_handler(request):
    return web.Response(text="OK")


def web_server():
    web_app = web.Application(client_max_size=30000000)
    
    # FIX: Yahan naya handler add kiya gaya hai
    # Yeh Webhook aur Render health check ko turant 200 OK dega.
    # Note: add_get/add_post se HEAD conflict ho raha tha, isliye isko rakhna zaroori hai.
    web_app.router.add_get('/', ping_handler)
    web_app.router.add_post('/', ping_handler)
    
    web_app.add_routes(routes)
    return web_app
    
