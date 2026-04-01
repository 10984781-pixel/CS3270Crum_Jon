import builtins
import csv
import os
import sys
import types
import unittest
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import mock_open, patch

from weather_package.loader import WeatherDataFetcher


class TestWeatherDataFetcher(unittest.TestCase):
    def _write_temp_csv(self, rows):
        temp_file = NamedTemporaryFile(mode="w", newline="", delete=False)
        self.addCleanup(lambda: os.unlink(temp_file.name))
        writer = csv.writer(temp_file)
        writer.writerows(rows)
        temp_file.close()
        return temp_file.name

    def test_iter_rows_reads_rows(self):
        path = self._write_temp_csv([["a", "b"], ["1", "2"]])
        fetcher = WeatherDataFetcher(path)
        rows = list(fetcher.iter_rows())
        self.assertEqual(rows, [["a", "b"], ["1", "2"]])

    def test_iter_rows_file_not_found(self):
        fetcher = WeatherDataFetcher("missing-file.csv")
        with self.assertRaises(FileNotFoundError):
            next(fetcher.iter_rows())

    def test_iter_rows_permission_error(self):
        fetcher = WeatherDataFetcher("blocked.csv")
        with patch("weather_package.loader.Path.open", side_effect=PermissionError):
            with self.assertRaises(PermissionError):
                next(fetcher.iter_rows())

    def test_iter_rows_unicode_error(self):
        fetcher = WeatherDataFetcher("bad-encoding.csv")
        decode_error = UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid")
        with patch("weather_package.loader.Path.open", side_effect=decode_error):
            with self.assertRaises(UnicodeDecodeError):
                next(fetcher.iter_rows())

    def test_iter_rows_csv_error(self):
        fetcher = WeatherDataFetcher("bad.csv")
        with patch("weather_package.loader.Path.open", mock_open(read_data="a,b\n")):
            with patch(
                "weather_package.loader.csv.reader",
                side_effect=csv.Error("bad csv"),
            ):
                with self.assertRaises(csv.Error):
                    next(fetcher.iter_rows())

    def test_load_csv_builtin_uses_generator(self):
        path = self._write_temp_csv([["x", "y"], ["3", "4"]])
        fetcher = WeatherDataFetcher(path)
        rows = fetcher.load_csv_builtin()
        self.assertEqual(rows, [["x", "y"], ["3", "4"]])

    def test_load_csv_pandas_import_error(self):
        fetcher = WeatherDataFetcher("data.csv")

        original_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pandas":
                raise ImportError("no pandas")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(ImportError):
                fetcher.load_csv_pandas()

    def test_load_csv_pandas_success(self):
        fetcher = WeatherDataFetcher("data.csv")
        fake_pd = types.ModuleType("pandas")
        fake_pd.read_csv = lambda path: {"rows": 1}
        sys.modules["pandas"] = fake_pd
        self.addCleanup(lambda: sys.modules.pop("pandas", None))

        df = fetcher.load_csv_pandas()
        self.assertEqual(df, {"rows": 1})
        self.assertEqual(fetcher.dataframe, {"rows": 1})

    def test_load_csv_pandas_file_not_found(self):
        fetcher = WeatherDataFetcher("missing.csv")
        fake_pd = types.ModuleType("pandas")

        def raise_not_found(*args, **kwargs):
            raise FileNotFoundError("missing")

        fake_pd.read_csv = raise_not_found
        sys.modules["pandas"] = fake_pd
        self.addCleanup(lambda: sys.modules.pop("pandas", None))

        with self.assertRaises(FileNotFoundError):
            fetcher.load_csv_pandas()

    def test_load_csv_pandas_permission_error(self):
        fetcher = WeatherDataFetcher("blocked.csv")
        fake_pd = types.ModuleType("pandas")

        def raise_permission(*args, **kwargs):
            raise PermissionError("blocked")

        fake_pd.read_csv = raise_permission
        sys.modules["pandas"] = fake_pd
        self.addCleanup(lambda: sys.modules.pop("pandas", None))

        with self.assertRaises(PermissionError):
            fetcher.load_csv_pandas()

    def test_load_csv_pandas_unicode_error(self):
        fetcher = WeatherDataFetcher("bad-encoding.csv")
        fake_pd = types.ModuleType("pandas")

        def raise_unicode(*args, **kwargs):
            raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid")

        fake_pd.read_csv = raise_unicode
        sys.modules["pandas"] = fake_pd
        self.addCleanup(lambda: sys.modules.pop("pandas", None))

        with self.assertRaises(UnicodeDecodeError):
            fetcher.load_csv_pandas()

    def test_load_csv_pandas_unexpected_error(self):
        fetcher = WeatherDataFetcher("bad.csv")
        fake_pd = types.ModuleType("pandas")

        def raise_unexpected(*args, **kwargs):
            raise ValueError("boom")

        fake_pd.read_csv = raise_unexpected
        sys.modules["pandas"] = fake_pd
        self.addCleanup(lambda: sys.modules.pop("pandas", None))

        with self.assertRaises(ValueError):
            fetcher.load_csv_pandas()


class TestWeatherDataFetcherAsync(unittest.IsolatedAsyncioTestCase):
    async def test_load_csv_builtin_async_delegates_to_sync(self):
        fetcher = WeatherDataFetcher("data.csv")
        with patch.object(fetcher, "load_csv_builtin", return_value=[["x", "y"]]) as mocked:
            rows = await fetcher.load_csv_builtin_async()
        self.assertEqual(rows, [["x", "y"]])
        mocked.assert_called_once_with()

    async def test_load_csv_pandas_async_delegates_to_sync(self):
        fetcher = WeatherDataFetcher("data.csv")
        fake_df = {"rows": 3}
        with patch.object(fetcher, "load_csv_pandas", return_value=fake_df) as mocked:
            df = await fetcher.load_csv_pandas_async()
        self.assertEqual(df, fake_df)
        mocked.assert_called_once_with()
