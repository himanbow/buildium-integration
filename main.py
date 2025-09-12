"""Entry point for running the Quart webhook service locally."""

from webhook_handler import app

if __name__ == "__main__":
    import asyncio
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
