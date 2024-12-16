import subprocess
import time
import logging
import pandas as pd
from pathlib import Path
import os

# Setup logging to log to both file and terminal
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("scraper_log.log"),
        logging.StreamHandler(),  # Logs to terminal
    ],
)

# Directory containing the scraper scripts
SCRAPER_DIR = Path("./finalized_scrapers")

# Update the OUTPUT_DIR path to the finalized_scrapers directory
OUTPUT_DIR = Path("./finalized_scrapers")
OUTPUT_DIR.mkdir(exist_ok=True)

# List of scraper filenames
SCRAPERS = [
    "biunsinnorden.py",
    "eventbrite.py",
    #"eventim.py",
    "hamburg_de.py",
    "our_neumuenster_py.py",
    "kiel-sailing-city.py",
    "live_gigs.py",
    "sh-tourismus.py",
    "rausgegangen.py",
    "unser_luebeck.py",
    "kiel-magazin.py",
    "meine_stadt.py",
    "wasgeht.py",
]

# Delay between scrapers (in seconds)
DELAY = 5

def run_scraper(scraper):
    """Runs a scraper script using subprocess."""
    scraper_path = SCRAPER_DIR / scraper
    if not scraper_path.exists():
        logging.error(f"Scraper script not found: {scraper_path}")
        return

    try:
        logging.info(f"Starting scraper: {scraper}")
        result = subprocess.run(
            ["python", str(scraper_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        logging.info(f"Scraper finished successfully: {scraper}")
        logging.info(f"Scraper output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Scraper failed: {scraper}")
        logging.error(f"Error output:\n{e.stderr}")
    except Exception as ex:
        logging.error(f"Unexpected error running scraper {scraper}: {ex}")

def merge_csvs(output_file):
    """Merges all valid and non-empty CSV files in the output directory."""
    all_csv_files = list(Path(".").glob("*.csv"))
    logging.info(f"Looking for CSV files in: {OUTPUT_DIR.resolve()}")
    logging.info(f"Found files: {[str(file) for file in OUTPUT_DIR.glob('*')]}")

    if not OUTPUT_DIR.exists():
        logging.error(f"Directory does not exist: {OUTPUT_DIR.resolve()}")
    else:
        logging.info(f"Directory exists: {OUTPUT_DIR.resolve()}")


    df_list = []
    columns_set = None

    for file in all_csv_files:
        try:
            # Skip empty files
            if file.stat().st_size == 0:
                logging.warning(f"Skipping empty file: {file}")
                continue

            df = pd.read_csv(file)

            # Check for consistent columns
            if columns_set is None:
                columns_set = set(df.columns)
            elif set(df.columns) != columns_set:
                logging.error(f"Column mismatch in file: {file}. Skipping...")
                continue

            df_list.append(df)
            logging.info(f"Successfully loaded: {file}")

        except Exception as e:
            logging.error(f"Failed to read {file}: {e}")

    if df_list:
        merged_df = pd.concat(df_list, ignore_index=True)
        merged_df.to_csv(output_file, index=False)
        logging.info(f"Merged data saved to {output_file}.")
    else:
        logging.warning("No valid CSV files found for merging.")

def orchestrate_scrapers():
    """Runs all scrapers, merges CSVs, and triggers BigQuery upload."""
    logging.info("Starting scraper orchestration...")
    for scraper in SCRAPERS:
        run_scraper(scraper)
        logging.info(f"Waiting {DELAY} seconds before the next scraper...")
        time.sleep(DELAY)

    # Merge all CSVs after scrapers are done
    merged_file = OUTPUT_DIR / "merged_data.csv"
    merge_csvs(merged_file)

    # Trigger the BigQuery upload script
    try:
        logging.info("Triggering push_to_bigquery.py script...")
        subprocess.run(["python", "push_to_bigquery.py"], check=True)
        logging.info("Data upload to BigQuery completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to trigger BigQuery upload: {e.stderr}")
    except Exception as ex:
        logging.error(f"Unexpected error while triggering BigQuery upload: {ex}")

if __name__ == "__main__":
    orchestrate_scrapers()
