"""Entry point for running the Quart webhook service locally."""

import os

from webhook_handler import app

if __name__ == "__main__":
    import asyncio
    import uvicorn

    worker_count = int(os.getenv("WEB_CONCURRENCY", "1"))
    uvicorn.run(app, host="0.0.0.0", port=8080, workers=worker_count)
