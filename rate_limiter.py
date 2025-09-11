"""Shared concurrency limiter for Buildium API calls."""

import asyncio

semaphore = asyncio.Semaphore(2)

