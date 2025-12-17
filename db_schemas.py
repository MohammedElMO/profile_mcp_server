from pymongo import ASCENDING, TEXT
from pymongo.errors import OperationFailure
from db_manager import DBManager


def create_validation_schemas(db):
    """
    Applies strict JSON Schema validation to collections.
    This acts as the gatekeeper for data quality.
    """

    # --- 1. ORGANIZATIONS COLLECTION ---
    # Stores unique company data to avoid duplication in profiles.
    try:
        db.create_collection("organizations", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name", "domain_hash"],
                "properties": {
                    "name": {
                        "bsonType": "string",
                        "description": "Canonical name of the company"
                    },
                    "domain_hash": {
                        "bsonType": "string",
                        "description": "Unique hash of website domain for deduplication"
                    },
                    "industry": {"bsonType": "string"},
                    "location": {
                        "bsonType": "object",
                        "properties": {
                            "city": {"bsonType": "string"},
                            "country": {"bsonType": "string"}
                        }
                    }
                }
            }
        })
        print("✅ 'organizations' collection created.")
    except OperationFailure:
        print("ℹ️ 'organizations' collection already exists (skipping creation).")

    # --- 2. PROFILES COLLECTION ---
    # The main document. Notice the 'work' array embeds specific job details
    # but references the 'organization_id' for company details.
    try:
        db.create_collection("profiles", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["basics", "source_id"],
                "properties": {
                    "basics": {
                        "bsonType": "object",
                        "required": ["name", "email"],
                        "properties": {
                            "name": {"bsonType": "string"},
                            "email": {
                                "bsonType": "string",
                                "pattern": "^.+@.+$"  # Regex validation for email
                            },
                            "headline": {"bsonType": "string"},
                            "location": {"bsonType": "string"}
                        }
                    },
                    "work_history": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "object",
                            "required": ["title", "start_date"],
                            "properties": {
                                "title": {"bsonType": "string"},
                                # RELATIONSHIP: Link to organizations collection
                                "org_id": {"bsonType": "objectId"},
                                # Used if org_id is not yet known
                                "org_name_fallback": {"bsonType": "string"},
                                "start_date": {"bsonType": "string"},
                                "end_date": {"bsonType": ["string", "null"]},
                                "is_current": {"bsonType": "bool"}
                            }
                        }
                    },
                    "skills": {
                        "bsonType": "array",
                        "items": {"bsonType": "string"}
                    },
                    # Provenance: Where did this come from?
                    "source_id": {"bsonType": "string"}
                }
            }
        })
        print("✅ 'profiles' collection created.")
    except OperationFailure:
        print("ℹ️ 'profiles' collection already exists.")

    # --- 3. LOGS COLLECTION ---
    # No strict validation needed here, just a flexible bucket for errors/stats
    if "extraction_logs" not in db.list_collection_names():
        db.create_collection("extraction_logs")
        print("✅ 'extraction_logs' collection created.")


def create_indexes(db):
    """
    Creates indexes to ensure query speed and data uniqueness.
    """
    # PROFILES: Unique Email to prevent duplicates
    db.profiles.create_index([("basics.email", ASCENDING)], unique=True)

    # PROFILES: Text index for searching skills and headlines
    db.profiles.create_index([
        ("basics.headline", TEXT),
        ("skills", TEXT)
    ], name="search_index")

    # ORGANIZATIONS: Unique Domain Hash
    db.organizations.create_index([("domain_hash", ASCENDING)], unique=True)

    print("✅ Indexes applied successfully.")


if __name__ == "__main__":
    manager = DBManager()
    db = manager.connect()

    print("--- STARTING DATABASE SETUP ---")
    create_validation_schemas(db)
    create_indexes(db)
    print("--- SETUP COMPLETE ---")
