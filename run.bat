@echo off
setlocal

:: --- CONFIGURATION ---
:: Set your environment variables here
set MONGO_URI=mongodb://localhost:27017
set SCRAPE_GITHUB_TOKEN=

echo === Starting Windows Setup ^& Execution ===

:: 1. Create Virtual Environment if it doesn't exist
if not exist "venv" (
    echo Creating virtual environment...
    python -m venv venv
)

:: 2. Activate Virtual Environment
call venv\Scripts\activate

:: 3. Install/Update Dependencies
echo Installing dependencies from requirements.txt...
python -m pip install --upgrade pip
pip install -r requirements.txt

:: 4. Initialize Database Schema
echo Initializing Database Schema...
python db_schemas.py

:: 5. Ready to start
echo Ready to start the AI Client.
echo Note: Make sure your MongoDB service is running.

:: 6. Run the AI Query Client
python ai_query_client.py

pause