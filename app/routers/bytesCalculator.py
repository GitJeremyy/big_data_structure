from fastapi import APIRouter, Query, HTTPException
from services.schema_client import Schema
from services.statistics import Statistics
from services.sizing import Sizer
from pathlib import Path
import json

router = APIRouter(prefix="/TD1", tags=["TD1 - Storage Analysis"])

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
# HELPER FUNCTION TO SAVE RESULTS
# ============================================================
def save_results_to_json(db_signature: str, sizes: dict):
    """
    Save the computed collection sizes to results_TD1.json.
    Updates or creates the file with results for each DB signature.
    """
    results_path = Path(__file__).resolve().parents[2] / 'services' / 'results_TD1.json'
    
    # Load existing results if file exists
    if results_path.exists():
        try:
            with open(results_path, 'r', encoding='utf-8') as f:
                all_results = json.load(f)
        except json.JSONDecodeError:
            all_results = {}
    else:
        all_results = {}
    
    # Prepare the collection data for this DB signature
    collections_data = []
    for coll_name, coll_info in sizes.items():
        if coll_name == "Database_Total":
            continue  # Skip the total, we'll add it separately
        
        # Extract the collection size in human-readable format
        total_size_human = coll_info.get("total_size_human", "0 B")
        # Remove the collection name from the human-readable size if present
        size_only = total_size_human.split(" (")[0] if " (" in total_size_human else total_size_human
        
        collections_data.append({
            "collection": coll_name,
            "doc_size_bytes": coll_info.get("avg_doc_bytes", 0),
            "num_docs": coll_info.get("nb_docs", 0),
            "collection_size": size_only
        })
    
    # Update the results for this DB signature
    all_results[db_signature] = {
        "description": DB_SIGNATURES[db_signature],
        "database_total": sizes["Database_Total"]["total_size_human"].split(" (")[0],
        "collections": collections_data
    }
    
    # Save back to file
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Results saved to {results_path}")


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
    # Optional: can pass manual_counts here if needed
    # manual_counts = {
    #     "Product": {"integer": 5, "string": 4, "longstring": 2, ...},
    #     ...
    # }
    sizer = Sizer(schema, stats)
    sizes = sizer.compute_collection_sizes()

    # Save results to JSON file
    save_results_to_json(db_signature, sizes)

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