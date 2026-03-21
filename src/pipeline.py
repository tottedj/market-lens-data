import logging
from fetcher import run_fetch
from db import get_client, insert_result

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline(limit=100):
    logger.info("Starting pipeline for %d companies", limit)

    client = get_client()
    results = run_fetch(limit)

    logger.info("Inserting %d companies into Supabase", len(results))
    for result in results:
        ticker = result["company"].ticker
        try:
            insert_result(client, result)
            logger.info("Inserted %s", ticker)
        except Exception as e:
            logger.error("Failed to insert %s: %s", ticker, e)

    logger.info("Pipeline complete")


if __name__ == "__main__":
    run_pipeline(limit=10)
