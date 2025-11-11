from fastapi import APIRouter, Query, HTTPException
from services.schema_client import Schema
from services.statistics import Statistics
from services.relationships import get_profile
from services.sizing import Sizer
from pathlib import Path

router = APIRouter()

# Define the valid DB signatures
VALID_DB_SIGNATURES = [f"DB{i}" for i in range(6)]  # ["DB0", "DB1", ..., "DB5"]

@router.get("/bytesCalculator")
async def calculate_bytes(
    db_signature: str = Query(
        "DB4",
        description="Database signature (choose from DB0-DB5)",
        enum=VALID_DB_SIGNATURES
    )
):
    # Ensure the signature is valid (redundant with enum but safe)
    if db_signature not in VALID_DB_SIGNATURES:
        raise HTTPException(status_code=400, detail=f"Invalid DB signature. Must be one of {VALID_DB_SIGNATURES}")

    # Dynamically build path to the right JSON schema
    schema_path = Path(__file__).resolve().parents[2] / 'services' / 'JSON_schema' / f'json-schema-{db_signature}.json'
    
    if schema_path.exists():
        try:
            schema_text = schema_path.read_text(encoding='utf-8')
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read schema for {db_signature}: {e}")
    else:
        raise HTTPException(status_code=404, detail=f"Schema file not found for {db_signature}")

    # Build core objects
    schema = Schema(schema_text)
    stats = Statistics()
    profile = get_profile(db_signature)  # relationship & collections profile

    # Optional debug output
    stats.describe()
    schema.print_entities_and_relations()

    # Relationship-aware sizing
    sizer = Sizer(schema, profile, stats)
    sizes = sizer.compute_collection_sizes()

    # Debug: print type counts per entity
    try:
        type_counts = sizer.debug_type_counts()
        print("\n=== TYPE COUNTS (per collection) ===")
        for coll, counts in type_counts.items():
            compact = ", ".join(f"{k}:{v}" for k, v in counts.items() if v)
            print(f"{coll}: {compact}")
    except Exception as e:
        print(f"Type-count debug failed: {e}")

    return {
        "message": "Byte calculation successful!",
        "db_signature": db_signature,
        "database_total": sizes["Database_Total"]["total_human"],
        "collections": sizes
    }