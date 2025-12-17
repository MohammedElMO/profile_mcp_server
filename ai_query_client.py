import requests
import time
import json
import asyncio
from mcp_server import search_profiles, get_platform_stats, get_top_contributors
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
# --- Gemini API Configuration ---
MODEL_NAME = "gemini-2.5-flash-preview-09-2025"
API_KEY = os.getenv("GEMINI_API_KEY")  # Handled by environment


def call_gemini(prompt, tools_definitions=None):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_NAME}:generateContent?key={API_KEY}"

    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }],
        "tools": [{
            "function_declarations": [
                {
                    "name": "search_profiles",
                    "description": "Search for profiles in the database by name, headline, skills, or location.",
                    "parameters": {
                        "type": "OBJECT",
                        "properties": {
                            "query": {"type": "STRING"},
                            "limit": {"type": "NUMBER"}
                        },
                        "required": ["query"]
                    }
                },
                {
                    "name": "get_platform_stats",
                    "description": "Returns count of profiles grouped by source platform."
                },
                {
                    "name": "get_top_contributors",
                    "description": "Find top profiles on a platform based on a metric.",
                    "parameters": {
                        "type": "OBJECT",
                        "properties": {
                            "platform": {"type": "STRING"},
                            "metric": {"type": "STRING"},
                            "limit": {"type": "NUMBER"}
                        },
                        "required": ["platform", "metric"]
                    }
                }
            ]
        }]
    }

    # Exponential Backoff for API Calls
    for attempt in range(6):
        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                return response.json()
            if response.status_code == 429:  # Rate limit
                time.sleep(2**attempt)
                continue
            break
        except Exception:
            time.sleep(2**attempt)

    return None


def handle_tool_calls(response_json):
    """Parses Gemini response and executes the matching local MCP tools."""
    candidate = response_json.get('candidates', [{}])[0]
    parts = candidate.get('content', {}).get('parts', [])

    for part in parts:
        if 'functionCall' in part:
            call = part['functionCall']
            name = call['name']
            args = call['args']

            print(f"AI is calling Tool: {name} with args: {args}")

            if name == "search_profiles":
                return search_profiles(**args)
            if name == "get_platform_stats":
                return get_platform_stats()
            if name == "get_top_contributors":
                return get_top_contributors(**args)

    return parts[0].get('text') if parts else "No answer."


async def chat_loop():
    print("=== AI Profile Assistant (MCP Connected) ===")
    print("Ask about your scraped data (e.g., 'Find Python devs in NYC' or 'Who has the most followers on GitHub?')")

    while True:
        user_input = input("\nYou: ")
        if user_input.lower() in ['exit', 'quit']:
            break

        # 1. Ask Gemini
        response = call_gemini(user_input)
        if not response:
            print("Error connecting to Gemini.")
            continue

        # 2. Handle Tool Call (The AI decides which DB tool to use)
        tool_result = handle_tool_calls(response)

        # 3. Send data back to Gemini for the final "Contextual" answer
        if isinstance(tool_result, (list, dict)):
            follow_up_prompt = f"Based on this database data: {json.dumps(tool_result)}, answer the user question: {user_input}"
            final_response = call_gemini(follow_up_prompt)
            answer = final_response['candidates'][0]['content']['parts'][0]['text']
            print(f"\nAI: {answer}")
        else:
            print(f"\nAI: {tool_result}")

if __name__ == "__main__":
    asyncio.run(chat_loop())
