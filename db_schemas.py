from pymongo import ASCENDING, TEXT
from pymongo.errors import OperationFailure
from db_manager import DBManager


def create_validation_schemas(db):
    """
    Applies strict JSON Schema validation to the profiles collection.
    """
    # --- PROFILES COLLECTION ---
    try:
        db.create_collection("profiles", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["basics", "source_id", "source_platform"],
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
                    "skills": {
                        "bsonType": "array",
                        "items": {"bsonType": "string"}
                    },
                    "metrics": {"bsonType": "object"},
                    "source_id": {"bsonType": "string"},
                    "source_platform": {"bsonType": "string"}
                }
            }
        })
        print("✅ 'profiles' collection created with JSON schema validation.")
    except OperationFailure:
        print("ℹ️ 'profiles' collection already exists (applying validation update).")
        # Update validator for existing collection
        db.command("collMod", "profiles", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["basics", "source_id", "source_platform"],
                "properties": {
                    "basics": {
                        "bsonType": "object",
                        "required": ["name", "email"],
                        "properties": {
                            "name": {"bsonType": "string"},
                            "email": {"bsonType": "string", "pattern": "^.+@.+$"},
                            "headline": {"bsonType": "string"},
                            "location": {"bsonType": "string"}
                        }
                    }
                }
            }
        })


def create_indexes(db):
    """
    Creates indexes to ensure query speed and data uniqueness.
    """
    # 1. Unique index on source_platform + source_id to prevent duplicates across runs
    db.profiles.create_index(
        [("source_platform", ASCENDING), ("source_id", ASCENDING)],
        unique=True
    )

    # 2. Unique Email index
    try:
        db.profiles.create_index([("basics.email", ASCENDING)], unique=True)
    except OperationFailure as e:
        print(
            f"⚠️ Unique email index exists or conflict: {e.details.get('errmsg')}")

    # 3. Text index for searching (Handled for conflicts)
    try:
        print("Applying 'search_index'...")
        db.profiles.create_index([
            ("basics.headline", TEXT),
            ("skills", TEXT),
            ("basics.name", TEXT)
        ], name="search_index")
    except OperationFailure as e:
        # Code 85 is IndexOptionsConflict: happens when index exists with different fields
        if e.code == 85:
            print(
                "🔄 Index conflict detected. Dropping old 'search_index' and recreating...")
            db.profiles.drop_index("search_index")
            db.profiles.create_index([
                ("basics.headline", TEXT),
                ("skills", TEXT),
                ("basics.name", TEXT)
            ], name="search_index")
            print("✅ 'search_index' updated successfully.")
        else:
            print(f"❌ Failed to create text index: {e}")

    print("✅ All indexes verified and applied.")


if __name__ == "__main__":
    # Ensure DBManager is configured correctly in your project
    manager = DBManager()
    db = manager.connect()

    print("--- STARTING DATABASE SETUP ---")
    create_validation_schemas(db)
    create_indexes(db)
    print("--- SETUP COMPLETE ---")
