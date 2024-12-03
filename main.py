import subprocess
import time
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    filename="scraper_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Directory containing the scraper scripts
SCRAPER_DIR = Path("./scrapers")

# List of scraper filenames
SCRAPERS = [
    "biunsinnorden.py",
    "eventbrite.py",
    "eventim.py",
    "kiel-sailing-city.py",
    "live_gigs.py",
    "rausgegangen.py",
    "sh-tourismus.py",
    "unser_luebeck.py",
]

# Delay between scrapers (in seconds)
DELAY = 5

import sys

# Get the current Python interpreter
PYTHON_EXECUTABLE = sys.executable

def run_scraper(scraper):
    """Runs a scraper script using subprocess."""
    scraper_path = SCRAPER_DIR / scraper
    if not scraper_path.exists():
        logging.error(f"Scraper script not found: {scraper_path}")
        return

    try:
        logging.info(f"Starting scraper: {scraper}")
        result = subprocess.run(
            [PYTHON_EXECUTABLE, str(scraper_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        logging.info(f"Scraper finished successfully: {scraper}")
        logging.debug(f"Scraper output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Scraper failed: {scraper}")
        logging.debug(f"Error output: {e.stderr}")


def main():
    logging.info("Starting scraper orchestration...")
    for scraper in SCRAPERS:
        run_scraper(scraper)
        logging.info(f"Waiting {DELAY} seconds before the next scraper...")
        time.sleep(DELAY)
    logging.info("All scrapers completed.")

if __name__ == "__main__":
    main()
