"""Global controls for Buildium API rate limiting.

The defaults can be overridden with the environment variables
``BUILDIUM_MAX_CONCURRENT_REQUESTS`` (concurrency) and
``BUILDIUM_REQS_PER_SEC`` (token bucket rate).
"""

import asyncio
import os
from aiolimiter import AsyncLimiter

MAX_CONCURRENT_REQUESTS = int(os.getenv("BUILDIUM_MAX_CONCURRENT_REQUESTS", "9"))
TOKENS_PER_SECOND = float(os.getenv("BUILDIUM_REQS_PER_SEC", "9"))

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
throttle = AsyncLimiter(TOKENS_PER_SECOND, time_period=1)


