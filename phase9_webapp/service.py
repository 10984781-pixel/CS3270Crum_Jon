"""Service tier for weather query and summary logic."""

from __future__ import annotations

import csv
from pathlib import Path

from .data import QueryHistoryRepository


class WeatherQueryError(ValueError):
    """Raised for validation or user-input query problems."""


class WeatherService:
    """Business logic for weather searches and stored query history."""

    def __init__(self, csv_path: str | Path, history_repo: QueryHistoryRepository):
        self.csv_path = Path(csv_path)
        self.history_repo = history_repo
        self._records_cache: list[dict[str, str]] | None = None
        self._locations_cache: list[str] | None = None

    def available_locations(self) -> list[str]:
        self._ensure_records_loaded()
        return self._locations_cache or []

    def query_weather(self, location: str, rainy_only: bool, limit: int = 25) -> dict:
        cleaned_location = (location or "").strip()
        if not cleaned_location:
            raise WeatherQueryError("Please select a location.")

        self._ensure_records_loaded()
        assert self._records_cache is not None

        matching_rows = [
            row
            for row in self._records_cache
            if (row.get("Location") or "").strip().lower() == cleaned_location.lower()
        ]

        if rainy_only:
            matching_rows = [row for row in matching_rows if row.get("RainToday") == "Yes"]

        total_matches = len(matching_rows)
        preview_rows = matching_rows[:limit]

        max_temps = [
            parsed
            for parsed in (self._to_float(row.get("MaxTemp")) for row in matching_rows)
            if parsed is not None
        ]
        avg_max_temp = round(sum(max_temps) / len(max_temps), 2) if max_temps else None

        rainy_matches = sum(1 for row in matching_rows if row.get("RainToday") == "Yes")

        self.history_repo.add_query(
            location=cleaned_location,
            rainy_only=rainy_only,
            result_count=total_matches,
        )

        return {
            "location": cleaned_location,
            "rainy_only": rainy_only,
            "total_matches": total_matches,
            "rainy_matches": rainy_matches,
            "avg_max_temp": avg_max_temp,
            "rows": [self._project_row(row) for row in preview_rows],
        }

    def recent_query_history(self, limit: int = 10) -> list[dict]:
        items = self.history_repo.recent_queries(limit=limit)
        return [
            {
                "id": item.id,
                "location": item.location,
                "rainy_only": item.rainy_only,
                "result_count": item.result_count,
                "created_at": item.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
            for item in items
        ]

    def _ensure_records_loaded(self) -> None:
        if self._records_cache is not None:
            return

        if not self.csv_path.exists():
            raise FileNotFoundError(f"Dataset not found: {self.csv_path}")

        with self.csv_path.open(mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            self._records_cache = list(reader)

        self._locations_cache = sorted(
            {
                (row.get("Location") or "").strip()
                for row in self._records_cache
                if (row.get("Location") or "").strip()
            }
        )

    @staticmethod
    def _to_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _project_row(row: dict[str, str]) -> dict[str, str]:
        return {
            "row_id": row.get("row ID", ""),
            "location": row.get("Location", ""),
            "min_temp": row.get("MinTemp", ""),
            "max_temp": row.get("MaxTemp", ""),
            "rainfall": row.get("Rainfall", ""),
            "rain_today": row.get("RainToday", ""),
            "rain_tomorrow": row.get("RainTomorrow", ""),
        }
