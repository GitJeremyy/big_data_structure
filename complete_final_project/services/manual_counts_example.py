"""
Manual field counts for document size calculation.

This module provides manual field counts that match the spreadsheet exactly.
When provided to Sizer or QueryCostCalculator, these override automatic counting.

Format:
{
    "CollectionName": {
        "integer": int,           # Number of integer fields
        "string": int,             # Number of string fields
        "date": int,               # Number of date fields
        "longstring": int,         # Number of longstring fields
        "array_int": int,          # Number of integer arrays
        "array_string": int,       # Number of string arrays
        "array_date": int,         # Number of date arrays
        "array_longstring": int,   # Number of longstring arrays
        "avg_array_length": int,   # Average array length
        "keys": int                # Total number of keys (including nested)
    }
}

Usage:
    from services.sizing import Sizer
    from services.schema_client import Schema
    from services.statistics import Statistics
    from services.manual_counts_example import MANUAL_COUNTS_DB1
    
    schema = Schema(schema_data)
    stats = Statistics()
    sizer = Sizer(schema, stats, manual_counts=MANUAL_COUNTS_DB1)
    size = sizer.estimate_document_size(product_entity)  # Uses manual counts if available
"""

# Manual counts for DB1 (Prod{[Cat], Supp})
MANUAL_COUNTS_DB1 = {
    "Stock": {
        "integer": 3,
        "string": 1,
        "date": 0,
        "longstring": 0,
        "array_int": 0,
        "array_string": 0,
        "array_date": 0,
        "array_longstring": 0,
        "avg_array_length": 0,
        "keys": 4
    },
    "Product": {
        "integer": 5,
        "string": 4,
        "date": 0,
        "longstring": 2,
        "array_int": 0,
        "array_string": 1,
        "array_date": 0,
        "array_longstring": 0,
        "avg_array_length": 2,
        "keys": 13
    },
    "Warehouse": {
        "integer": 2,
        "string": 1,
        "date": 0,
        "longstring": 0,
        "array_int": 0,
        "array_string": 0,
        "array_date": 0,
        "array_longstring": 0,
        "avg_array_length": 0,
        "keys": 3
    }
}

# Manual counts for DB2 (Prod{[Cat], Supp, [St]})
MANUAL_COUNTS_DB2 = {
    "Product": {
        "integer": 5,
        "string": 10,
        "date": 0,
        "longstring": 1,
        "array_int": 2,
        "array_string": 1,
        "array_date": 0,
        "array_longstring": 0,
        "avg_array_length": 200,
        "keys": 616
    }
}

# Manual counts for DB3 (St{Prod{[Cat],Supp}})
MANUAL_COUNTS_DB3 = {
    "Stock": {
        "integer": 8,
        "string": 5,
        "date": 0,
        "longstring": 2,
        "array_int": 0,
        "array_string": 1,
        "array_date": 0,
        "array_longstring": 0,
        "avg_array_length": 2,
        "keys": 17
    }
}

# Manual counts for DB4 (OL{Prod{[Cat],Supp}})
MANUAL_COUNTS_DB4 = {
    "OrderLine": {
        "integer": 9,
        "string": 4,
        "date": 2,
        "longstring": 3,
        "array_int": 0,
        "array_string": 1,
        "array_date": 0,
        "array_longstring": 0,
        "avg_array_length": 2,
        "keys": 20
    }
}

# Manual counts for DB5 (Prod{[Cat], Supp, [OL]})
MANUAL_COUNTS_DB5 = {
    "Product": {
        "integer": 5,
        "string": 4,
        "date": 0,
        "longstring": 2,
        "array_int": 3,
        "array_string": 0,
        "array_date": 2,
        "array_longstring": 1,
        "avg_array_length": 40000,  # 4.00E+04
        "keys": 240011
    }
}

# Additional collections (common across DBs)
MANUAL_COUNTS_COMMON = {
    "OrderLine": {
        "integer": 4,
        "string": 0,
        "date": 2,
        "longstring": 1,
        "array_int": 0,
        "array_string": 0,
        "array_date": 0,
        "array_longstring": 0,
        "avg_array_length": 0,
        "keys": 7
    },
    "Client": {
        "integer": 1,
        "string": 5,
        "date": 1,
        "longstring": 0,
        "array_int": 0,
        "array_string": 0,
        "array_date": 0,
        "array_longstring": 0,
        "avg_array_length": 0,
        "keys": 7
    }
}

# Helper function to get manual counts for a specific DB
def get_manual_counts_for_db(db_signature: str) -> dict:
    """
    Get manual counts for a specific database signature.
    
    Args:
        db_signature: Database signature (DB1, DB2, DB3, DB4, DB5)
    
    Returns:
        Dict of manual counts for that DB, merged with common collections
    """
    db_counts = {
        "DB1": MANUAL_COUNTS_DB1,
        "DB2": MANUAL_COUNTS_DB2,
        "DB3": MANUAL_COUNTS_DB3,
        "DB4": MANUAL_COUNTS_DB4,
        "DB5": MANUAL_COUNTS_DB5,
    }.get(db_signature, {})
    
    # Merge with common collections
    result = MANUAL_COUNTS_COMMON.copy()
    result.update(db_counts)
    return result

