import logging
import os
from fetcher import run_fetch
from db import get_client, get_processed_tickers, upsert_result

_log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(_log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(_log_dir, "pipeline.log"), encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def run_pipeline(limit=25):
    client = get_client()
    processed = get_processed_tickers(client)
    logger.info("Starting pipeline: %d already processed, fetching next %d", len(processed), limit)

    results = run_fetch(limit, exclude=processed)

    logger.info("Inserting %d companies into Supabase", len(results))
    for result in results:
        ticker = result["company"].ticker
        try:
            upsert_result(client, result)
            logger.info("Inserted %s", ticker)
        except Exception as e:
            logger.error("Failed to insert %s: %s", ticker, e)

    logger.info("Pipeline complete")


if __name__ == "__main__":
    run_pipeline(limit=25)
