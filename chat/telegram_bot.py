import logging
import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

TELEGRAM_TOKEN = "8226866379:AAFQP-WJaQYX5BoR35uZBOtVsl_hhVJVO6g"
API_BASE_URL = "http://127.0.0.1:8000"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Welcome to PulseBoard!\n\n"
        "I track real-time sentiment for 20 top tech companies.\n\n"
        "Try asking me:\n"
        "• What is the sentiment on OpenAI?\n"
        "• Show me the leaderboard\n"
        "• How is Anthropic trending?\n\n"
        "Commands:\n"
        "/leaderboard — top trending companies\n"
        "/company anthropic — specific company stats"
    )

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        response = requests.get(f"{API_BASE_URL}/api/leaderboard", timeout=15)
        data = response.json()
        companies = data["leaderboard"]
        text = "PulseBoard Leaderboard\n\n"
        for i, company in enumerate(companies):
            sentiment = company["avg_sentiment"]
            emoji = "🟢" if sentiment >= 0.05 else "🔴" if sentiment <= -0.05 else "🟡"
            text += (
                f"{i+1}. {emoji} {company['company']}\n"
                f"   Sentiment: {sentiment:.3f} | "
                f"Mentions: {company['total_mentions']} | "
                f"Score: {company['trending_score']:.2f}\n\n"
            )
        await update.message.reply_text(text)
    except Exception as e:
        await update.message.reply_text(f"Error fetching leaderboard: {e}")

async def company_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /company anthropic")
        return
    company_name = " ".join(context.args)
    try:
        response = requests.get(
            f"{API_BASE_URL}/api/company/{company_name}/sentiment",
            timeout=15
        )
        if response.status_code == 404:
            await update.message.reply_text(f"Company '{company_name}' not found.")
            return
        data = response.json()
        sentiment = data["avg_sentiment"]
        emoji = "🟢" if sentiment >= 0.05 else "🔴" if sentiment <= -0.05 else "🟡"
        text = (
            f"{emoji} {data['company']}\n\n"
            f"Sentiment: {sentiment:.3f} ({data['sentiment_label']})\n"
            f"Total Mentions: {data['total_mentions']}\n"
            f"Trending Score: {data['trending_score']:.2f}\n"
            f"Last Updated: {data['last_updated'][:19]}"
        )
        await update.message.reply_text(text)
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text
    try:
        response = requests.post(
            f"{API_BASE_URL}/api/chat",
            json={"query": query},
            timeout=15
        )
        data = response.json()
        await update.message.reply_text(data["response"])
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

def main():
    print("Starting PulseBoard Telegram bot...")
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("company", company_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print("Bot is running!")
    app.run_polling(allowed_updates=["message"])

if __name__ == "__main__":
    main()