"""Convenience runner for the Phase 9 Flask web app."""

from phase9_webapp.app import app


if __name__ == "__main__":
    app.run(debug=True)
