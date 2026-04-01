import csv
import os
import unittest
from tempfile import NamedTemporaryFile

import Module2


class FakePlot:
    def __init__(self):
        self.calls = []

    def figure(self, *args, **kwargs):
        self.calls.append("figure")

    def hist(self, *args, **kwargs):
        self.calls.append("hist")

    def bar(self, *args, **kwargs):
        self.calls.append("bar")

    def title(self, *args, **kwargs):
        self.calls.append("title")

    def xlabel(self, *args, **kwargs):
        self.calls.append("xlabel")

    def ylabel(self, *args, **kwargs):
        self.calls.append("ylabel")

    def xticks(self, *args, **kwargs):
        self.calls.append("xticks")

    def tight_layout(self):
        self.calls.append("tight_layout")


class TestFunctionalModule(unittest.TestCase):
    def _write_temp_csv(self, rows):
        temp_file = NamedTemporaryFile(mode="w", newline="", delete=False)
        self.addCleanup(lambda: os.unlink(temp_file.name))
        writer = csv.writer(temp_file)
        writer.writerows(rows)
        temp_file.close()
        return temp_file.name

    def test_iter_weather_records(self):
        path = self._write_temp_csv(
            [
                ["Location", "RainToday", "Rainfall", "MaxTemp"],
                ["A", "Yes", "1.5", "20"],
            ]
        )
        records = list(Module2.iter_weather_records(path))
        self.assertEqual(records[0]["Location"], "A")

    def test_filter_rainy_days(self):
        records = [
            {"RainToday": "Yes"},
            {"RainToday": "No"},
        ]
        rainy = list(Module2.filter_rainy_days(records))
        self.assertEqual(len(rainy), 1)

    def test_rainfall_stats(self):
        records = [
            {"Rainfall": "1.0"},
            {"Rainfall": ""},
            {"Rainfall": "2.0"},
        ]
        values, total, avg = Module2.rainfall_stats(records)
        self.assertEqual(values, [1.0, 2.0])
        self.assertAlmostEqual(total, 3.0)
        self.assertAlmostEqual(avg, 1.5)

    def test_average_max_temp_by_location(self):
        records = [
            {"Location": "A", "MaxTemp": "10"},
            {"Location": "A", "MaxTemp": "20"},
            {"Location": "B", "MaxTemp": "30"},
        ]
        top = Module2.average_max_temp_by_location(records, top_n=1)
        self.assertEqual(top[0][0], "B")

    def test_plot_helpers(self):
        fake = FakePlot()
        Module2.plot_rainfall_hist([1.0, 2.0], plt=fake)
        Module2.plot_top_locations_bar([("A", 1.0)], plt=fake)
        self.assertIn("hist", fake.calls)
        self.assertIn("bar", fake.calls)
