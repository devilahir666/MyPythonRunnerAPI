# This file is a part of TG-Direct-Link-Generator


from main.bot import StreamBot
from main.vars import Var
from pyrogram import filters, Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton


START_TEXT = """**Hello {}**,
I'm a Telegram Files to Link Generator Bot.
"""


@StreamBot.on_message(filters.command(["start", "help"]) & filters.private)
async def start_handler(c: Client, m: Message):
    if m.from_user.id not in Var.BANNED_USERS:
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("Support Group 🧑‍💻", url="https://t.me/Devilahir_Bots")]])
        await m.reply_text(
            text=START_TEXT.format(m.from_user.first_name),
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )

# Yahan koi changes nahi kiye gaye, lekin aapke code mein woh upar waali line galat thi.
