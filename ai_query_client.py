from dotenv import load_dotenv
import requests
import time
import json
import asyncio
from mcp_server import search_profiles, find_top_experts, get_geo_density, get_skill_distribution
import os

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")

MODEL_NAME = "gemini-2.5-flash-preview-09-2025"


def call_gemini(prompt, include_tools=True):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_NAME}:generateContent?key={API_KEY}"

    payload = {
        "contents": [{"parts": [{"text": prompt}]}]
    }

    if include_tools:
        payload["tools"] = [{
            "function_declarations": [
                {
                    "name": "search_profiles",
                    "description": "Find specific people by text query.",
                    "parameters": {"type": "OBJECT", "properties": {"query": {"type": "STRING"}}, "required": ["query"]}
                },
                {
                    "name": "find_top_experts",
                    "description": "Identifies highly-qualified professionals for a specific tech skill.",
                    "parameters": {"type": "OBJECT", "properties": {"skill": {"type": "STRING"}}, "required": ["skill"]}
                },
                {
                    "name": "get_geo_density",
                    "description": "Analyzes tech talent concentration in a location.",
                    "parameters": {"type": "OBJECT", "properties": {"location": {"type": "STRING"}}, "required": ["location"]}
                },
                {
                    "name": "get_skill_distribution",
                    "description": "Returns most common skills across the entire database."
                }
            ]
        }]

    for attempt in range(5):
        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                return response.json()
            if response.status_code == 429:
                time.sleep(2**attempt)
        except:
            time.sleep(1)
    return None


def extract_text(resp):
    try:
        return resp['candidates'][0]['content']['parts'][0]['text']
    except:
        return None


def process_tool_calls(resp):
    parts = resp.get('candidates', [{}])[0].get('content', {}).get('parts', [])
    for part in parts:
        if 'functionCall' in part:
            fc = part['functionCall']
            name, args = fc['name'], fc['args']
            print(f"AI executing tool: {name}")

            if name == "search_profiles":
                return search_profiles(**args)
            if name == "find_top_experts":
                return find_top_experts(**args)
            if name == "get_geo_density":
                return get_geo_density(**args)
            if name == "get_skill_distribution":
                return get_skill_distribution()
    return extract_text(resp)


async def chat_loop():
    print("=== AI Profile Analyst  ===")
    print("Ready to analyze 10,000+ tech profiles.")

    while True:
        user_input = input("\nWhat would You Like to know About The Profiles: ")
        if user_input.lower() in ['exit', 'quit']:
            break

        resp = call_gemini(user_input, include_tools=True)
        data_result = process_tool_calls(resp)

        if isinstance(data_result, (list, dict)):
            final_prompt = (
                f"I found this data in the profiles database:\n{json.dumps(data_result, indent=2)}\n\n"
                f"Use this data to answer: {user_input}"
            )
            summary_resp = call_gemini(final_prompt, include_tools=False)
            print(f"\nAI: {extract_text(summary_resp)}")
        else:
            print(f"\nAI: {data_result}")

if __name__ == "__main__":
    asyncio.run(chat_loop())
