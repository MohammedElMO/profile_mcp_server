import os
from mcp.server.fastmcp import FastMCP
from pymongo import MongoClient
from bson import json_util
import json

mcp = FastMCP("TechProfileAnalytics")


def get_db():
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    # Ensure this matches your actual database name used in DBManager
    db = client['profiles_database']
    return db['profiles']


@mcp.tool()
def search_profiles(query: str, limit: int = 5):
    """General search for profiles based on text in name, headline, or location."""
    col = get_db()
    search_filter = {
        "$or": [
            {"basics.name": {"$regex": query, "$options": "i"}},
            {"basics.headline": {"$regex": query, "$options": "i"}},
            {"basics.location": {"$regex": query, "$options": "i"}},
            {"skills": {"$regex": query, "$options": "i"}}
        ]
    }
    results = list(col.find(search_filter).limit(limit))
    return json.loads(json_util.dumps(results))


@mcp.tool()
def find_top_experts(skill: str, limit: int = 5):
    """
    Finds the highest-ranked professionals for a specific skill.
    Uses reputation scores and contribution counts for ranking.
    """
    col = get_db()
    query = {
        "$or": [
            {"skills": {"$regex": skill, "$options": "i"}},
            {"basics.headline": {"$regex": skill, "$options": "i"}}
        ]
    }
    # Ranking logic: High reputation, high contribution, high followers
    results = list(col.find(query).sort([
        ("metrics.reputation_score", -1),
        ("metrics.contribution_count", -1),
        ("metrics.followers", -1)
    ]).limit(limit))
    return json.loads(json_util.dumps(results))


@mcp.tool()
def get_geo_density(location: str):
    """
    Analyzes the concentration of tech talent in a specific city or region.
    Returns counts per platform and avg reputation scores.
    """
    col = get_db()
    pipeline = [
        {"$match": {"basics.location": {"$regex": location, "$options": "i"}}},
        {"$group": {
            "_id": "$source_platform",
            "total_count": {"$sum": 1},
            "avg_reputation": {"$avg": "$metrics.reputation_score"}
        }}
    ]
    return list(col.aggregate(pipeline))


@mcp.tool()
def get_skill_distribution():
    """Identifies the most common tech skills appearing in the database."""
    col = get_db()
    pipeline = [
        {"$unwind": "$skills"},
        {"$group": {"_id": "$skills", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20}
    ]
    return list(col.aggregate(pipeline))


if __name__ == "__main__":
    mcp.run()
