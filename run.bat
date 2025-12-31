@echo off
setlocal

set MONGO_URI=mongodb://localhost:27017
set SCRAPE_GITHUB_TOKEN=

echo === Starting Windows Setup ^& Execution ===

if not exist "venv" (
    echo Creating virtual environment...
    python -m venv venv
)

call venv\Scripts\activate

echo Installing dependencies from requirements.txt...
python -m pip install --upgrade pip
pip install -r requirements.txt

echo Initializing Database Schema...
python db_schemas.py

echo Ready to start the AI Client.
echo Note: Make sure your MongoDB service is running.

python ai_query_client.py

pause