
import os
from mcp.server.fastmcp import FastMCP
from pymongo import MongoClient
from bson import json_util
import json

# Initialize FastMCP server
mcp = FastMCP("ProfileScraperDB")

# Database Connection


def get_db():
    # Reusing your logic: connect to the profiles collection
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    # Update to your actual DB name
    db = client['profile_scraper']
    return db['profiles']


@mcp.tool()
def search_profiles(query: str, limit: int = 5):
    """
    Search for profiles in the database by name, headline, skills, or location.
    Args:
        query: The search term (e.g., 'Python developer' or 'San Francisco')
        limit: Number of results to return
    """
    col = get_db()

    # Simple text search across multiple fields
    search_filter = {
        "$or": [
            {"basics.name": {"$regex": query, "$options": "i"}},
            {"basics.headline": {"$regex": query, "$options": "i"}},
            {"basics.location": {"$regex": query, "$options": "i"}},
            {"skills": {"$regex": query, "$options": "i"}},
            {"source_platform": {"$regex": query, "$options": "i"}}
        ]
    }

    results = list(col.find(search_filter).limit(limit))
    # Convert MongoDB BSON to JSON string
    return json.loads(json_util.dumps(results))


@mcp.tool()
def get_platform_stats():
    """Returns a count of profiles grouped by source platform (GitHub, Kaggle, etc.)"""
    col = get_db()
    pipeline = [
        {"$group": {"_id": "$source_platform", "count": {"$sum": 1}}}
    ]
    stats = list(col.aggregate(pipeline))
    return stats


@mcp.tool()
def get_top_contributors(platform: str, metric: str, limit: int = 5):
    """
    Find top profiles on a platform based on a metric.
    Args:
        platform: 'GitHub', 'StackOverflow', 'Kaggle', or 'ORCID'
        metric: 'followers', 'reputation_score', 'contribution_count'
        limit: Number of profiles to return
    """
    col = get_db()
    sort_key = f"metrics.{metric}"
    results = list(col.find({"source_platform": platform}).sort(
        sort_key, -1).limit(limit))
    return json.loads(json_util.dumps(results))


if __name__ == "__main__":
    # Start the server using stdio (standard for MCP)
    mcp.run()
