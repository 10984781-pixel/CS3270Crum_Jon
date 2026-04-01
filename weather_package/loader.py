"""CSV loading utilities."""

from __future__ import annotations

import asyncio
import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class WeatherDataFetcher:
    """Fetch and store weather data from CSV files."""

    def __init__(self, file_path):
        self.file_path = Path(file_path)
        self.rows = []
        self.dataframe = None

    def iter_rows(self):
        """Yield CSV rows one at a time using the built-in csv module."""
        try:
            with self.file_path.open(mode="r", newline="", encoding="utf-8") as file:
                reader = csv.reader(file)
                for row in reader:
                    yield row
        except FileNotFoundError:
            logger.error("CSV file not found: %s", self.file_path)
            raise
        except PermissionError:
            logger.error("Permission denied reading CSV: %s", self.file_path)
            raise
        except UnicodeDecodeError:
            logger.error("Unable to decode CSV file as UTF-8: %s", self.file_path)
            raise
        except csv.Error:
            logger.error("CSV parsing error in file: %s", self.file_path)
            raise

    def load_csv_builtin(self):
        """Load a CSV file with the built-in csv module and store rows."""
        self.rows = list(self.iter_rows())
        return self.rows

    async def load_csv_builtin_async(self):
        """Load CSV rows asynchronously by offloading I/O to a worker thread."""
        return await asyncio.to_thread(self.load_csv_builtin)

    def load_csv_pandas(self):
        """Load a CSV file with pandas and store the DataFrame."""
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("pandas is required for load_csv_pandas") from exc

        try:
            self.dataframe = pd.read_csv(self.file_path)
            return self.dataframe
        except FileNotFoundError:
            logger.error("CSV file not found: %s", self.file_path)
            raise
        except PermissionError:
            logger.error("Permission denied reading CSV: %s", self.file_path)
            raise
        except UnicodeDecodeError:
            logger.error("Unable to decode CSV file as UTF-8: %s", self.file_path)
            raise
        except Exception:
            logger.error("Unexpected error reading CSV with pandas: %s", self.file_path)
            raise

    async def load_csv_pandas_async(self):
        """Load pandas DataFrame asynchronously by offloading I/O to a worker thread."""
        return await asyncio.to_thread(self.load_csv_pandas)
