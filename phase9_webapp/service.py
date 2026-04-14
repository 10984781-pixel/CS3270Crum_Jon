"""Service tier for weather query and summary logic."""

from __future__ import annotations

import csv
from pathlib import Path

from .data import QueryHistoryRepository

_SKLEARN_IMPORT_ERROR: str | None = None

try:
    from sklearn.metrics import accuracy_score
    from sklearn.model_selection import train_test_split
    from sklearn.tree import DecisionTreeClassifier
except Exception as exc:
    accuracy_score = None
    train_test_split = None
    DecisionTreeClassifier = None
    summary = str(exc).strip().splitlines()
    detail = summary[0] if summary else type(exc).__name__
    _SKLEARN_IMPORT_ERROR = f"{type(exc).__name__}: {detail}"


class WeatherQueryError(ValueError):
    """Raised for validation or user-input query problems."""


class WeatherService:
    """Business logic for weather searches and stored query history."""

    def __init__(self, csv_path: str | Path, history_repo: QueryHistoryRepository):
        self.csv_path = Path(csv_path)
        self.history_repo = history_repo
        self._records_cache: list[dict[str, str]] | None = None
        self._locations_cache: list[str] | None = None
        self._prediction_model = None
        self._model_algorithm = "DecisionTreeClassifier"
        self._model_accuracy: float | None = None
        self._model_training_rows = 0
        self._model_error: str | None = None

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
        predictions = self._predict_rain_tomorrow_batch(matching_rows)
        preview_rows = list(zip(matching_rows, predictions))[:limit]

        max_temps = [
            parsed
            for parsed in (self._to_float(row.get("MaxTemp")) for row in matching_rows)
            if parsed is not None
        ]
        avg_max_temp = round(sum(max_temps) / len(max_temps), 2) if max_temps else None

        rainy_matches = sum(1 for row in matching_rows if row.get("RainToday") == "Yes")
        predictable_rows = sum(1 for prediction in predictions if prediction is not None)
        predicted_rain_tomorrow = sum(1 for prediction in predictions if prediction == "Yes")
        predicted_rain_pct = (
            round((predicted_rain_tomorrow / predictable_rows) * 100, 1)
            if predictable_rows
            else None
        )

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
            "rows": [
                self._project_row(row, predicted_rain_tomorrow=prediction)
                for row, prediction in preview_rows
            ],
            "predictions_available": self._prediction_model is not None,
            "predictable_rows": predictable_rows,
            "predicted_rain_tomorrow": predicted_rain_tomorrow,
            "predicted_rain_pct": predicted_rain_pct,
            "model_algorithm": self._model_algorithm,
            "model_validation_accuracy": self._model_accuracy,
            "model_training_rows": self._model_training_rows,
            "model_error": self._model_error,
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
        self._train_prediction_model()

    def _train_prediction_model(self) -> None:
        if self._prediction_model is not None or self._model_error is not None:
            return

        if DecisionTreeClassifier is None or train_test_split is None or accuracy_score is None:
            detail = (
                f" ({_SKLEARN_IMPORT_ERROR})" if _SKLEARN_IMPORT_ERROR else ""
            )
            self._model_error = (
                "scikit-learn is unavailable in this environment."
                f"{detail} Run pip install -r requirements_phase9.txt."
            )
            return

        assert self._records_cache is not None

        features: list[list[float]] = []
        targets: list[int] = []

        for row in self._records_cache:
            feature_vector = self._feature_vector(row)
            target_value = self._target_value(row)
            if feature_vector is None or target_value is None:
                continue
            features.append(feature_vector)
            targets.append(target_value)

        self._model_training_rows = len(features)
        if self._model_training_rows < 200:
            self._model_error = "Not enough rows with complete fields to train the model."
            return

        if len(set(targets)) < 2:
            self._model_error = "Training data only contains one target class."
            return

        try:
            x_train, x_test, y_train, y_test = train_test_split(
                features,
                targets,
                test_size=0.2,
                random_state=42,
                stratify=targets,
            )
            model = DecisionTreeClassifier(max_depth=6, random_state=42)
            model.fit(x_train, y_train)
            predicted = model.predict(x_test)
            self._model_accuracy = round(float(accuracy_score(y_test, predicted)), 3)
            self._prediction_model = model
        except ValueError as exc:
            self._model_error = f"Model training failed: {exc}"

    def _predict_rain_tomorrow_batch(self, rows: list[dict[str, str]]) -> list[str | None]:
        if self._prediction_model is None:
            return [None for _ in rows]

        prediction_inputs: list[list[float]] = []
        prediction_indexes: list[int] = []
        labels: list[str | None] = [None for _ in rows]

        for index, row in enumerate(rows):
            feature_vector = self._feature_vector(row)
            if feature_vector is None:
                continue
            prediction_inputs.append(feature_vector)
            prediction_indexes.append(index)

        if not prediction_inputs:
            return labels

        raw_predictions = self._prediction_model.predict(prediction_inputs)
        for index, prediction in zip(prediction_indexes, raw_predictions):
            labels[index] = "Yes" if int(prediction) == 1 else "No"

        return labels

    @staticmethod
    def _to_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _feature_vector(row: dict[str, str]) -> list[float] | None:
        min_temp = WeatherService._to_float(row.get("MinTemp"))
        max_temp = WeatherService._to_float(row.get("MaxTemp"))
        rainfall = WeatherService._to_float(row.get("Rainfall"))
        humidity_3pm = WeatherService._to_float(row.get("Humidity3pm"))
        pressure_3pm = WeatherService._to_float(row.get("Pressure3pm"))

        rain_today_raw = (row.get("RainToday") or "").strip().lower()
        if rain_today_raw == "yes":
            rain_today = 1.0
        elif rain_today_raw == "no":
            rain_today = 0.0
        else:
            return None

        numeric_values = [min_temp, max_temp, rainfall, humidity_3pm, pressure_3pm]
        if any(value is None for value in numeric_values):
            return None

        return [*numeric_values, rain_today]

    @staticmethod
    def _target_value(row: dict[str, str]) -> int | None:
        target_raw = (row.get("RainTomorrow") or "").strip().lower()
        if target_raw in {"1", "yes"}:
            return 1
        if target_raw in {"0", "no"}:
            return 0
        return None

    @staticmethod
    def _project_row(
        row: dict[str, str],
        predicted_rain_tomorrow: str | None = None,
    ) -> dict[str, str]:
        return {
            "row_id": row.get("row ID", ""),
            "location": row.get("Location", ""),
            "min_temp": row.get("MinTemp", ""),
            "max_temp": row.get("MaxTemp", ""),
            "rainfall": row.get("Rainfall", ""),
            "rain_today": row.get("RainToday", ""),
            "rain_tomorrow": row.get("RainTomorrow", ""),
            "predicted_rain_tomorrow": predicted_rain_tomorrow or "N/A",
        }
