import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import click
from dotenv import load_dotenv

from .fetcher import run_fetch
from .db import get_client, get_processed_tickers, get_skipped_tickers, insert_skipped_tickers, upsert_result

logger = logging.getLogger(__name__)


def _cleanup_old_logs(log_dir: Path, max_logs: int = 30):
    logs = sorted(log_dir.glob("pipeline_*.log"), key=lambda f: f.stat().st_mtime)
    for f in logs[:-max_logs]:
        f.unlink()


def _setup_logging():
    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    _cleanup_old_logs(log_dir)
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
    skipped = get_skipped_tickers(client)
    exclude = processed | skipped
    logger.info("Starting pipeline: %d already processed, %d skipped, fetching next %d",
                len(processed), len(skipped), limit)

    results, newly_skipped = run_fetch(limit, exclude=exclude)

    if newly_skipped:
        insert_skipped_tickers(client, newly_skipped, reason="no valid annual rows")
        logger.info("Saved %d newly skipped tickers: %s", len(newly_skipped), ", ".join(newly_skipped))

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
