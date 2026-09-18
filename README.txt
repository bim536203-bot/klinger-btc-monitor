Klinger BTC Server v1
Environment variables required:
TELEGRAM_BOT_TOKEN = your bot token (keep secret)
TELEGRAM_CHAT_ID = your Telegram chat id
Start command: uvicorn main:app --host 0.0.0.0 --port $PORT
Endpoints: GET /status, POST /start, POST /stop
The /start endpoint runs one 30-minute monitoring session.
