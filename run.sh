#!/bin/bash

export MONGO_URI="mongodb://localhost:27017"
export SCRAPE_GITHUB_TOKEN=""

echo "=== Starting Linux Setup & Execution ==="

if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate

echo "Installing dependencies from requirements.txt..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Initializing Database Schema..."
python3 db_schemas.py

echo "Ready to start the AI Client."
echo "Note: Make sure your MongoDB is running."

python3 ai_query_client.py