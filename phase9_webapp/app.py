"""Flask routes for Module 11 final project."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

from flask import Flask, current_app, jsonify, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename

from .analytics import build_analysis
from .auth import login_required, verify_credentials
from .csv_data import filter_by_city, get_available_cities, load_weather_data
from .storage import session_data_store


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-this-secret")
    app.config["UPLOAD_EXTENSIONS"] = {".csv"}
    app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024

    @app.get("/")
    def login_page():
        if session.get("authenticated"):
            return redirect(url_for("dashboard"))
        return render_template("login.html", error=None)

    @app.post("/login")
    def login():
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if verify_credentials(username, password):
            session.clear()
            session["authenticated"] = True
            session["username"] = username
            session["session_id"] = str(uuid.uuid4())
            return redirect(url_for("dashboard"))

        return render_template("login.html", error="Invalid username or password.")

    @app.post("/logout")
    @login_required
    def logout():
        session_id = session.get("session_id")
        if session_id:
            session_data_store.clear(session_id)
        session.clear()
        return redirect(url_for("login_page"))

    @app.get("/dashboard")
    @login_required
    def dashboard():
        return render_template("dashboard.html", username=session.get("username", ""))

    @app.post("/api/upload")
    @login_required
    def upload_csv():
        uploaded_file = request.files.get("file")
        if uploaded_file is None or uploaded_file.filename == "":
            return jsonify({"ok": False, "error": "Please choose a CSV file."}), 400

        filename = secure_filename(uploaded_file.filename)
        extension = Path(filename).suffix.lower()
        if extension not in current_app.config["UPLOAD_EXTENSIONS"]:
            return jsonify({"ok": False, "error": "Only CSV files are allowed."}), 400

        try:
            dataframe = load_weather_data(uploaded_file)
        except Exception as exc:
            return jsonify({"ok": False, "error": f"Could not process CSV: {exc}"}), 400

        session_data_store.set_dataframe(session["session_id"], dataframe)
        cities = get_available_cities(dataframe)

        return jsonify({"ok": True, "rows": len(dataframe), "cities": cities})

    @app.get("/api/cities")
    @login_required
    def get_cities():
        dataframe = session_data_store.get_dataframe(session["session_id"])
        if dataframe is None:
            return jsonify({"ok": False, "error": "Upload a CSV file first."}), 400

        return jsonify({"ok": True, "cities": get_available_cities(dataframe)})

    @app.get("/api/analysis")
    @login_required
    def get_analysis():
        category = request.args.get("category", "").strip().lower()
        city = request.args.get("city", "").strip()

        if category not in {"temperature", "rainfall", "extreme"}:
            return jsonify({"ok": False, "error": "Invalid category."}), 400
        if not city:
            return jsonify({"ok": False, "error": "City is required."}), 400

        dataframe = session_data_store.get_dataframe(session["session_id"])
        if dataframe is None:
            return jsonify({"ok": False, "error": "Upload a CSV file first."}), 400

        city_df = filter_by_city(dataframe, city)
        analysis = build_analysis(city_df, category, city)
        return jsonify({"ok": True, **analysis})

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
