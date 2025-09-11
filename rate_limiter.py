"""Global semaphore for Buildium API rate limiting.

The limit defaults to 10 concurrent requests but can be overridden by
setting the ``BUILDIUM_MAX_CONCURRENT_REQUESTS`` environment variable.
"""

import asyncio
import os

MAX_CONCURRENT_REQUESTS = int(os.getenv("BUILDIUM_MAX_CONCURRENT_REQUESTS", "10"))
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

