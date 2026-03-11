from telegram.ext import CallbackQueryHandler, CommandHandler, MessageHandler, filters

from bot.callbacks import handle_callback
from bot.conversations import (
    cmd_claimoff,
    cmd_claimphoff,
    cmd_claimspecialoff,
    cmd_clockoff,
    cmd_clockphoff,
    cmd_clockspecialoff,
    cmd_history,
    cmd_newuser,
    cmd_startadmin,
    handle_message,
)
from constants import HELP_TEXT, START_TEXT
from services.sheets_repo import healthcheck, try_get_worksheet_title


async def cmd_start(update, context):
    await update.message.reply_text(START_TEXT)


async def cmd_help(update, context):
    await update.message.reply_text(HELP_TEXT)


async def cmd_ping(update, context):
    await update.message.reply_text("pong")


async def cmd_checksheet(update, context):
    ok, message = healthcheck()
    prefix = "✅" if ok else "❌"
    await update.message.reply_text(f"{prefix} {message}")


async def cmd_sheetinfo(update, context):
    title = try_get_worksheet_title()
    if title:
        await update.message.reply_text(f"Connected sheet: {title}")
    else:
        await update.message.reply_text("Sheet not ready.")


async def cmd_summary(update, context):
    await update.message.reply_text("Summary is temporarily under rebuild in v2.")


async def cmd_overview(update, context):
    await update.message.reply_text("Overview is temporarily under rebuild in v2.")


def register_handlers(application):
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("checksheet", cmd_checksheet))
    application.add_handler(CommandHandler("sheetinfo", cmd_sheetinfo))

    application.add_handler(CommandHandler("startadmin", cmd_startadmin))
    application.add_handler(CommandHandler("history", cmd_history))

    application.add_handler(CommandHandler("clockoff", cmd_clockoff))
    application.add_handler(CommandHandler("claimoff", cmd_claimoff))
    application.add_handler(CommandHandler("clockphoff", cmd_clockphoff))
    application.add_handler(CommandHandler("claimphoff", cmd_claimphoff))
    application.add_handler(CommandHandler("clockspecialoff", cmd_clockspecialoff))
    application.add_handler(CommandHandler("claimspecialoff", cmd_claimspecialoff))
    application.add_handler(CommandHandler("newuser", cmd_newuser))

    application.add_handler(CommandHandler("summary", cmd_summary))
    application.add_handler(CommandHandler("overview", cmd_overview))

    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
