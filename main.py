# main.py
from webhook_handler import app

if __name__ == "__main__":
    import asyncio
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
