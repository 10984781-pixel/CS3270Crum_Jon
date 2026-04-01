"""Presentation tier (Flask routes + Jinja templates)."""

from __future__ import annotations

from pathlib import Path

from flask import Flask, render_template, request

from .data import Database, QueryHistoryRepository
from .service import WeatherQueryError, WeatherService

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CSV_PATH = BASE_DIR / "AustraliaWeatherData" / "Weather Training Data.csv"
DEFAULT_DB_PATH = BASE_DIR / "weather_queries.db"


def create_app(
    csv_path: str | Path = DEFAULT_CSV_PATH,
    db_path: str | Path = DEFAULT_DB_PATH,
) -> Flask:
    app = Flask(__name__)

    database = Database(db_path)
    database.create_tables()
    history_repo = QueryHistoryRepository(database.session_factory)
    weather_service = WeatherService(csv_path, history_repo)

    @app.route("/", methods=["GET", "POST"])
    def index():
        context = {
            "locations": [],
            "history": weather_service.recent_query_history(),
            "result": None,
            "error": None,
            "selected_location": "",
            "rainy_only": False,
        }

        try:
            context["locations"] = weather_service.available_locations()
        except FileNotFoundError:
            context["error"] = (
                "Dataset file was not found. Verify the CSV file exists at "
                f"{Path(csv_path)}"
            )
            return render_template("index.html", **context)

        if request.method == "POST":
            selected_location = (request.form.get("location") or "").strip()
            rainy_only = request.form.get("rainy_only") == "on"
            context["selected_location"] = selected_location
            context["rainy_only"] = rainy_only

            try:
                context["result"] = weather_service.query_weather(
                    location=selected_location,
                    rainy_only=rainy_only,
                    limit=25,
                )
            except WeatherQueryError as exc:
                context["error"] = str(exc)

            context["history"] = weather_service.recent_query_history()

        return render_template("index.html", **context)

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
