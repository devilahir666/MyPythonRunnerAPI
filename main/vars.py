from os import environ
from dotenv import load_dotenv

load_dotenv()


class Var(object):
    MULTI_CLIENT = True if environ.get("MULTI_CLIENT", "False").lower() == "true" else False
    API_ID = int(environ.get("API_ID"))
    API_HASH = str(environ.get("API_HASH"))
    BOT_TOKEN = str(environ.get("BOT_TOKEN"))
    MULTI_TOKENS = [
        str(environ.get(f"MULTI_TOKEN{i}")) for i in range(1, 6) if environ.get(f"MULTI_TOKEN{i}")
    ]
    SLEEP_THRESHOLD = int(environ.get("SLEEP_THRESHOLD", "60"))
    WORKERS = int(environ.get("WORKERS", "6"))
    BIN_CHANNEL = int(environ.get("BIN_CHANNEL", None))
    PORT = int(environ.get("PORT", 8080))
    BIND_ADDRESS = str(environ.get("WEB_SERVER_BIND_ADDRESS", "0.0.0.0"))
    PING_INTERVAL = int(environ.get("PING_INTERVAL", "1200"))
    HAS_SSL = str(environ.get("HAS_SSL", "False")).lower() == "true"
    NO_PORT = str(environ.get("NO_PORT", "False")).lower() == "true"

    ON_HEROKU = "DYNO" in environ
    APP_NAME = str(environ.get("APP_NAME")) if ON_HEROKU else None

    FQDN = (
        str(environ.get("FQDN", BIND_ADDRESS))
        if not ON_HEROKU or environ.get("FQDN")
        else APP_NAME + ".herokuapp.com"
    )
    URL = f"https://{FQDN}/" if HAS_SSL or ON_HEROKU else f"http://{FQDN}:{PORT}/"

    UPDATES_CHANNEL = "TechZBots"
    OWNER_ID = int(environ.get('OWNER_ID', '777000'))

    BANNED_CHANNELS = list(set(int(x) for x in str(environ.get("BANNED_CHANNELS", "-1001296894100")).split()))
    BANNED_USERS = list(set(int(x) for x in str(environ.get("BANNED_USERS","5275470552 5287015877")).split()))
