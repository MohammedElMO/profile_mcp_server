#!/bin/bash

# --- CONFIGURATION ---
# Set your environment variables here
export MONGO_URI="mongodb://localhost:27017"
export SCRAPE_GITHUB_TOKEN="" # Add your token if available

echo "=== Starting Linux Setup & Execution ==="

# 1. Create Virtual Environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# 2. Activate Virtual Environment
source venv/bin/activate

# 3. Install/Update Dependencies
echo "Installing dependencies from requirements.txt..."
pip install --upgrade pip
pip install -r requirements.txt

# 4. Initialize Database Schema
echo "Initializing Database Schema..."
python3 init_db.py

# 5. Check if we have data (Optional: Prompt to run scraper)
echo "Ready to start the AI Client."
echo "Note: Make sure your MongoDB is running."

# 6. Run the AI Query Client (which imports the MCP server logic)
python3 ai_query_client.py  