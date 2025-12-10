"""
Generate all query cost results and save them to the queries/ folder
"""
import json
from pathlib import Path
from services.query_cost import QueryCostCalculator
from services.query_parser import parse_query

# Create queries directory if it doesn't exist
queries_dir = Path("queries")
queries_dir.mkdir(exist_ok=True)

# Define all queries to generate
queries = []

# Q1: Stock query (IDP=1, IDW=2) - Good sharding: IDP, Bad sharding: IDC
q1_query = "SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = 1 AND S.IDW = 2"
for db in ["DB0", "DB1", "DB2", "DB3", "DB4", "DB5"]:
    queries.append({
        "filename": f"Q1_{db}_IDP.json",
        "db_design": db,
        "query": q1_query,
        "sharding_key": "IDP",
        "description": f"Q1 on {db} with IDP sharding (good)"
    })
    queries.append({
        "filename": f"Q1_{db}_IDC.json",
        "db_design": db,
        "query": q1_query,
        "sharding_key": "IDC",
        "description": f"Q1 on {db} with IDC sharding (bad)"
    })

# Q2: Product query (brand='Apple') - Good sharding: IDP, Bad sharding: IDC
q2_query = "SELECT P.name, P.price FROM Product P WHERE P.brand = 'Apple'"
for db in ["DB0", "DB1", "DB2", "DB3", "DB4", "DB5"]:
    queries.append({
        "filename": f"Q2_{db}_IDP.json",
        "db_design": db,
        "query": q2_query,
        "sharding_key": "IDP",
        "description": f"Q2 on {db} with IDP sharding (good)"
    })
    queries.append({
        "filename": f"Q2_{db}_IDC.json",
        "db_design": db,
        "query": q2_query,
        "sharding_key": "IDC",
        "description": f"Q2 on {db} with IDC sharding (bad)"
    })

# Q3: OrderLine query (date='2024-01-01') - Good sharding: date, Bad sharding: IDC
q3_query = "SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = '2024-01-01'"
for db in ["DB0", "DB1", "DB2", "DB3", "DB4", "DB5"]:
    queries.append({
        "filename": f"Q3_{db}_date.json",
        "db_design": db,
        "query": q3_query,
        "sharding_key": "date",
        "description": f"Q3 on {db} with date sharding (good)"
    })
    queries.append({
        "filename": f"Q3_{db}_IDC.json",
        "db_design": db,
        "query": q3_query,
        "sharding_key": "IDC",
        "description": f"Q3 on {db} with IDC sharding (bad)"
    })


print("=" * 80)
print("GENERATING ALL QUERY COST RESULTS")
print("=" * 80)

for query_def in queries:
    print(f"\n{query_def['description']}")
    print(f"  DB: {query_def['db_design']}")
    print(f"  File: {query_def['filename']}")
    
    try:
        # Initialize calculator for this DB design
        calc = QueryCostCalculator(db_signature=query_def["db_design"])
        
        # Parse the query (convert SQL to parsed format)
        parsed = parse_query(query_def["query"], db_signature=query_def["db_design"])
        
        # Build the query dict with sharding_key if specified
        query_dict = {
            "collection": parsed["collection"],
            "filter_fields": parsed["filter_fields"],
            "project_fields": parsed["project_fields"],
            "sharding_key": query_def.get("sharding_key", parsed.get("sharding_key")),
            "has_index": True,
            "index_size": 1000000  # 1MB default
        }
        
        # Calculate costs
        cost_result = calc.calculate_query_cost(query_dict)
        
        # Build the full result object (matching API format)
        result = {
            "message": "Query cost calculated successfully!",
            "sql": query_def["query"],
            "db_signature": query_def["db_design"],
            "collection_size_file": "results_TD1.json",
            "parsed_query": parsed,
            "cost_analysis": cost_result
        }
        
        # Save to file
        output_path = queries_dir / query_def["filename"]
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        # Print summary
        sizes = result["cost_analysis"]["sizes"]
        distribution = result["cost_analysis"]["distribution"]
        
        print(f"  ✓ Saved to {output_path}")
        print(f"    size_input: {sizes['size_input_bytes']}")
        print(f"    size_msg: {sizes['size_msg_bytes']}")
        print(f"    S (servers): {distribution['S_servers']}")
        print(f"    res_q: {distribution['res_q_results']}")
        
    except Exception as e:
        print(f"  ✗ ERROR: {e}")

print("\n" + "=" * 80)
print("GENERATION COMPLETE")
print("=" * 80)
print(f"\nAll query results saved to {queries_dir}/ folder")
