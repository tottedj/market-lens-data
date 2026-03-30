import logging
import time
from datetime import datetime
from pathlib import Path

import click
from dotenv import load_dotenv

from .fetcher import run_fetch
from .db import get_client, get_processed_tickers, upsert_result

logger = logging.getLogger(__name__)


def _setup_logging():
    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


@click.command()
@click.option("--limit", default=25, help="Number of tickers to process.")
def main(limit):
    """Fetch financial data from SEC/Yahoo Finance and store in Supabase."""
    load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")
    _setup_logging()

    t_start = time.monotonic()
    client = get_client()
    processed = get_processed_tickers(client)
    logger.info("Starting pipeline: %d already processed, fetching next %d", len(processed), limit)

    results = run_fetch(limit, exclude=processed)

    if not results:
        logger.warning("No results to insert — check fetch logs above")
        return

    logger.info("Inserting %d companies into Supabase", len(results))
    inserted = 0
    insert_failed = 0
    failed_tickers = []
    for result in results:
        ticker = result["company"].ticker
        try:
            upsert_result(client, result)
            inserted += 1
            logger.info("Inserted %s", ticker)
        except Exception as e:
            insert_failed += 1
            failed_tickers.append(ticker)
            logger.error("Failed to insert %s (%s): %s", ticker, type(e).__name__, e)

    elapsed = time.monotonic() - t_start
    logger.info("Pipeline complete in %.1fs — %d fetched, %d inserted, %d insert failures",
                elapsed, len(results), inserted, insert_failed)
    if failed_tickers:
        logger.warning("Failed to insert: %s", ", ".join(failed_tickers))


if __name__ == "__main__":
    main()
