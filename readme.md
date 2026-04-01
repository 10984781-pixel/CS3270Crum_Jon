# Phase 9: 3-Tier Weather Web Application

The app lets a user select an Australia weather location, request filered data, and view results in a browser. It also stores useful query history in SQLite.

## What I Implemented

### Presntation Tier (UI)
- Browserbased UI built with Flask + Jinja templates.
- Form inputs:
  - Location selector
  - Optional `Rainy days only` filter
- Output:
  - Summary stas for the request
  - Preview table of weather rows
  - Query history table from SQLite

### Business Tie
- Validates user input.
- Loads and filters weather CSV records.
- Computes simple summary values:
  - total matchs
  - rainy matches
  - average max temperature
- Saves each user request to the database.

File:
- `phase9_webapp/service.py`

### Data Tier 
- Stores useful appliction data:
  - `location`
  - `rainy_only`
  - `result_count`
  - `created_at`

## Quick Run Instructions

### 1. Create/activate a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies
```bash
python -m pip instll -r requirements_phase9.txt
```

### 3. Run the Flask app
```bash
python run_phase9.py
```

### 4. Open in browser
Go to:
- `http://127.0.0.1:5000`


