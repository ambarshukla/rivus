"""Entry point: python -m rivus_collect venues/<venue>.yaml"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys

from prometheus_client import start_http_server

from .collector import Collector
from .config import load_config
from .sink import KafkaSink


async def main(config_path: str) -> None:
    config = load_config(config_path)
    sink = KafkaSink(os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"), config.venue)
    start_http_server(int(os.environ.get("METRICS_PORT", "8000")))

    task = asyncio.create_task(Collector(config, sink).run())
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):  # not available on Windows
            loop.add_signal_handler(sig, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        logging.getLogger(__name__).info("%s: stopping", config.venue)
    finally:
        sink.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python -m rivus_collect <venue config file>")
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(main(sys.argv[1]))
