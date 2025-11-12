from fastapi import APIRouter, Query, HTTPException
from services.schema_client import Schema
from services.statistics import Statistics
from services.sizing import Sizer
from pathlib import Path

router = APIRouter()

DB_SIGNATURES = {
    "DB0": "DB0: Prod, Cat, Supp, St, Wa, OL, Cl",
    "DB1": "DB1: Prod{[Cat],Supp}, St, Wa, OL, Cl",
    "DB2": "DB2: Prod{[Cat],Supp,[St]}, Wa, OL, Cl",
    "DB3": "DB3: St{Prod{[Cat],Supp}}, Wa, OL, Cl",
    "DB4": "DB4: St, Wa, OL{Prod{[Cat],Supp}},Cl",
    "DB5": "DB5: Prod{[Cat],Supp,[OL]}, St, Wa, Cl"
}

VALID_DB_SIGNATURES = list(DB_SIGNATURES.keys())


# ============================================================
# MAIN BYTES CALCULATOR ENDPOINT
# ============================================================
@router.get("/bytesCalculator")
async def calculate_bytes(
    db_signature: str = Query(
        "DB4",
        description="Select a database signature:\n\n"
                    + "\n".join([f"- **{k}** → {v}" for k, v in DB_SIGNATURES.items()]),
        enum=VALID_DB_SIGNATURES
    )
):
    print(f"Selected profile: {DB_SIGNATURES[db_signature]}")
    
    if db_signature not in VALID_DB_SIGNATURES:
        raise HTTPException(status_code=400, detail=f"Invalid DB signature. Must be one of {VALID_DB_SIGNATURES}")

    # Load schema
    schema_path = Path(__file__).resolve().parents[2] / 'services' / 'JSON_schema' / f'json-schema-{db_signature}.json'
    if not schema_path.exists():
        raise HTTPException(status_code=404, detail=f"Schema file not found for {db_signature}")

    try:
        schema_text = schema_path.read_text(encoding='utf-8')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read schema for {db_signature}: {e}")

    schema = Schema(schema_text)
    stats = Statistics()

    # Optional debug info
    stats.describe()
    schema.print_entities_and_relations()

    # Compute collection sizes
    sizer = Sizer(schema, stats)
    sizes = sizer.compute_collection_sizes()

    return {
        "message": "Byte calculation successful!",
        "db_signature": db_signature,
        "database_total": sizes["Database_Total"]["total_size_human"],
        "collections": sizes
    }


# ============================================================
# SHARDING DISTRIBUTION STATISTICS
# ============================================================
@router.get("/shardingStats")
async def sharding_statistics():
    """
    Compute and return sharding distribution statistics for all
    (collection, shard key) pairs defined in the exercise.
    Does not depend on DB signature.
    """
    stats = Statistics()
    sharding_data = stats.compute_sharding_stats()
    return {
        "message": "Sharding distribution computed successfully.",
        "total_servers": stats.nb_servers,
        "distributions": sharding_data
    }