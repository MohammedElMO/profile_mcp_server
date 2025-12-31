from pymongo import ASCENDING, TEXT
from pymongo.errors import OperationFailure
from db_manager import DBManager


def create_validation_schemas(db):
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
                                "pattern": "^.+@.+$"
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
    except:
        pass


def create_indexes(db):
    db.profiles.create_index(
        [("source_platform", ASCENDING), ("source_id", ASCENDING)],
        unique=True
    )

    try:
        db.profiles.create_index([("basics.email", ASCENDING)], unique=True)
    except OperationFailure as e:
        print(
            f"Unique email index exists or conflict: {e.details.get('errmsg')}")

    try:
        print("Applying 'search_index'...")
        db.profiles.create_index([
            ("basics.headline", TEXT),
            ("skills", TEXT),
            ("basics.name", TEXT)
        ], name="search_index")
    except OperationFailure as e:
        if e.code == 85:
            print(
                "🔄 Index conflict detected. Dropping old 'search_index' and recreating...")
            db.profiles.drop_index("search_index")
            db.profiles.create_index([
                ("basics.headline", TEXT),
                ("skills", TEXT),
                ("basics.name", TEXT)
            ], name="search_index")
            print("'search_index' updated successfully.")
        else:
            print(f"Failed to create text index: {e}")

    print("All indexes verified and applied.")


if __name__ == "__main__":
    manager = DBManager()
    db = manager.connect()

    print("--- STARTING DATABASE SETUP ---")
    create_validation_schemas(db)
    create_indexes(db)
    print("--- SETUP COMPLETE ---")
