from aiohttp import web

def web_server():
    app = web.Application()
    async def home(request):
        return web.Response(text="TG Direct Link Generator Bot is running!")
    app.router.add_get("/", home)
    return app
