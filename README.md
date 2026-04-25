# CS3270 Final Project Module 11 jon crum

## Purpose 
This project is a Python web application for Australian weather analytics
Core features:
- Login screen with secure password verification
- Authenticated dashboard access only
- CSV upload for Australian weather data
- Single-page  dashboard
- Three analysis buttons:
  - Temperature Trends
  - Rainfall Patterns
  - Extreme Indicators
- Dynamic city dropdown populated from uploaded file
- Autupdating chart and text summary based on category 

Dashboard flow:
1. User logs in.
2. User uploads a CSV file.
3. User selects one of 3 category buttons.
4. City dropdown appears with values from uploaded CSV.
5. City selection triggers chart + summary update on same page.

## CSV Upload and Processing
- CSV file is uploaded through `/api/upload`
- File extension is validated csv only
- CSV is parsed
- City columns are seperated
- Parsed data is stored by user session

## Interactive Buttons, Dropdown, Charts
- Clicking any category button sets it
- City is populated from available cities
- Changing city or category calls backend API (`/api/analysis`)

## Development Phases and Iteration Summary
### Phase 1: Authentication Foundation
- Added login/logout and session navigation if user is not logged in

### Phase 2: Data Upload and Processing
- Added CSV upload 

### Phase 3: Interactive Dashboard
- 3 category buttons, city dropdown
- Added chart + summary rendering without page navigation

### Phase 4: Testing
- Added tests for data loading
- Simplified UI for reliabilityy

## Setup and Run easily!
1. Create and activate a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:
```bash
python3 -m pip install -r requirements_phase9.txt
```

3. Run tests:
```bash
pytest -q
```

4. Start app:
```bash
python3 run_phase9.py
```

5. Open browser:
- `http://127.0.0.1:5000`

## Demo Login
- Username: `student`
- Password: `CS3270!`
