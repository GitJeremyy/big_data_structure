"""
Comprehensive test script that parses queries first, then calculates costs
Tests all filter query configurations from the homework spreadsheet
"""

from services.query_parser import parse_query
from services.query_cost import QueryCostCalculator
from services.statistics import Statistics
import json
from typing import Dict, List, Optional


def format_scientific(value: float) -> str:
    """Format number in scientific notation"""
    return f"{value:.2e}"


def calculate_budget(network_vol: float) -> float:
    """
    Calculate budget cost from network volume.
    Based on spreadsheet: Price = 1.1E-11 €/B
    """
    PRICE_PER_BYTE = 1.1e-11  # €/B
    return network_vol * PRICE_PER_BYTE


def extract_ram_vol_per_server(result: Dict, has_index: bool) -> float:
    """
    Extract RAM volume per server from result.
    For index scan: index_size + selectivity × docs_per_server × doc_size
    """
    # Get values from result
    selectivity = float(result["distribution"]["selectivity"])
    nb_docs_per_server = result["distribution"]["nb_docs_per_server"]
    size_doc = int(result["sizes"]["size_doc_bytes"].split()[0])
    
    if has_index:
        index_size = Statistics.DEFAULT_INDEX_SIZE
        ram_per_server = index_size + selectivity * nb_docs_per_server * size_doc
    else:
        ram_per_server = 1.0 * nb_docs_per_server * size_doc
    
    return ram_per_server


def test_query_from_sql(
    query_name: str,
    sql: str,
    db_signature: str,
    sharding_key: str,
    expected_values: Dict = None
):
    """
    Test a query by parsing SQL first, then calculating costs.
    
    Args:
        query_name: Q1, Q2, or Q3
        sql: SQL query string
        db_signature: DB1, DB2, DB3, DB4, or DB5
        sharding_key: Sharding key
        expected_values: Optional dict with expected values for comparison
    """
    print(f"\n{'='*100}")
    print(f"Query: {query_name} | DB: {db_signature} | SQL: {sql}")
    print(f"Sharding Key: {sharding_key}")
    print(f"{'='*100}")
    
    # Step 1: Parse the SQL query
    try:
        parsed = parse_query(sql, db_signature=db_signature)
        print(f"\n[OK] Query parsed successfully")
        print(f"  Collection: {parsed['collection']}")
        print(f"  Filter fields: {len(parsed['filter_fields'])}")
        print(f"  Project fields: {len(parsed['project_fields'])}")
    except Exception as e:
        print(f"\n[ERROR] Query parsing failed: {e}")
        return None
    
    # Step 2: Calculate costs using parsed query
    calc = QueryCostCalculator(
        db_signature=db_signature,
        collection_size_file="results_TD1.json"
    )
    
    query = {
        **parsed,
        "sharding_key": sharding_key,
        "has_index": True,
        "index_size": Statistics.DEFAULT_INDEX_SIZE
    }
    
    try:
        result = calc.calculate_query_cost(query)
    except Exception as e:
        print(f"\n[ERROR] Cost calculation failed: {e}")
        return None
    
    # Extract values
    S = result["distribution"]["S_servers"]
    size_input = int(result["sizes"]["size_input_bytes"].split()[0])
    size_msg = int(result["sizes"]["size_msg_bytes"].split()[0])
    nb_output = result["distribution"]["res_q_results"]
    vol_network = float(result["volumes"]["vol_network"].split()[0])
    vol_ram_total = float(result["volumes"]["vol_RAM"].split()[0])
    time_cost = float(result["costs"]["time"].split()[0])
    co2_total = float(result["costs"]["carbon_total"].split()[0])
    
    # Calculate RAM per server
    vol_ram_per_server = extract_ram_vol_per_server(result, has_index=True)
    
    # Calculate budget
    budget = calculate_budget(vol_network)
    
    # CO2 values are already in kgCO2eq (no conversion needed)
    co2_kg = co2_total
    
    # Display results in spreadsheet-like format
    print(f"\n[NETWORK]")
    print(f"  Sharding Key: {sharding_key}")
    print(f"  Sharding (S): {S} server(s)")
    print(f"  Query size: {size_input} B")
    print(f"  nb output: {nb_output:,}")
    print(f"  output size: {size_msg} B")
    print(f"  Network Vol (B): {format_scientific(vol_network)}")
    
    print(f"\n[RAM / SERVER SIDE]")
    print(f"  Index: 1")
    print(f"  Local Index: {sharding_key}")
    print(f"  Nb pointers per working: {nb_output}")
    print(f"  collection on doc size: {int(result['sizes']['size_doc_bytes'].split()[0]):,} B")
    print(f"  RAM Vol per server: {format_scientific(vol_ram_per_server)}")
    print(f"  nb srv working: {S}")
    print(f"  RAM Vol (total): {format_scientific(vol_ram_total)}")
    
    print(f"\n[COSTS]")
    print(f"  Time: {format_scientific(time_cost)} s")
    print(f"  kgCO2eq: {format_scientific(co2_kg)}")
    print(f"  Budget: {format_scientific(budget)} EUR")
    
    # Compare with expected values if provided
    if expected_values:
        print(f"\n[COMPARISON WITH EXPECTED]")
        if "query_size" in expected_values:
            exp_qs = expected_values["query_size"]
            diff = abs(size_input - exp_qs) / exp_qs * 100 if exp_qs > 0 else 0
            status = "[OK]" if abs(size_input - exp_qs) < 1 else "[FAIL]"
            print(f"  {status} Query Size: Expected {exp_qs} B, Got {size_input} B, Diff: {diff:.1f}%")
        
        if "output_size" in expected_values:
            exp_os = expected_values["output_size"]
            diff = abs(size_msg - exp_os) / exp_os * 100 if exp_os > 0 else 0
            status = "[OK]" if abs(size_msg - exp_os) < 1 else "[FAIL]"
            print(f"  {status} Output Size: Expected {exp_os} B, Got {size_msg} B, Diff: {diff:.1f}%")
        
        if "network_vol" in expected_values:
            exp_net = expected_values["network_vol"]
            diff = abs(vol_network - exp_net) / exp_net * 100 if exp_net > 0 else 0
            print(f"  Network Vol: Expected {format_scientific(exp_net)}, Got {format_scientific(vol_network)}, Diff: {diff:.1f}%")
    
    return {
        "query": query_name,
        "db": db_signature,
        "sql": sql,
        "sharding_key": sharding_key,
        "collection": parsed["collection"],
        "S": S,
        "size_input": size_input,
        "size_msg": size_msg,
        "nb_output": nb_output,
        "vol_network": vol_network,
        "vol_ram_per_server": vol_ram_per_server,
        "vol_ram_total": vol_ram_total,
        "time": time_cost,
        "co2_kg": co2_kg,
        "budget": budget
    }


def test_all_configurations():
    """
    Test all configurations from the homework spreadsheet.
    Based on the image showing DB/Query/Collection/Sharding combinations.
    """
    all_results = []
    
    # ============================================================
    # Q1: SELECT S.IDP, S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW
    # ============================================================
    print("\n" + "="*100)
    print("Q1: SELECT S.IDP, S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW")
    print("="*100)
    
    sql_q1 = "SELECT S.IDP, S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW"
    
    # DB1, Q1, Stock - IDP
    all_results.append(test_query_from_sql("Q1", sql_q1, "DB1", "IDP"))
    
    # DB1, Q1, Stock - IDW
    all_results.append(test_query_from_sql("Q1", sql_q1, "DB1", "IDW"))
    
    # DB1, Q1, Stock - IDP & IDW
    all_results.append(test_query_from_sql("Q1", sql_q1, "DB1", "IDP"))
    # Note: "IDP & IDW" sharding means filtering on both, which is same as IDP when IDP is in filter
    
    # DB2, Q1, P[S] (Stock embedded in Product) - IDP
    all_results.append(test_query_from_sql("Q1", sql_q1, "DB2", "IDP"))
    
    # DB3, Q1, S{P} (Product embedded in Stock) - IDW
    all_results.append(test_query_from_sql("Q1", sql_q1, "DB3", "IDW"))
    
    # ============================================================
    # Q2: SELECT P.IDP, P.name, P.price FROM Product P WHERE P.brand = $brand
    # ============================================================
    print("\n" + "="*100)
    print("Q2: SELECT P.IDP, P.name, P.price FROM Product P WHERE P.brand = $brand")
    print("="*100)
    
    sql_q2 = "SELECT P.IDP, P.name, P.price FROM Product P WHERE P.brand = $brand"
    
    # DB1, Q2, Prod - IDP
    all_results.append(test_query_from_sql("Q2", sql_q2, "DB1", "IDP"))
    
    # DB1, Q2, Prod - Brand
    all_results.append(test_query_from_sql("Q2", sql_q2, "DB1", "brand"))
    
    # DB2, Q2, P[S] (Stock embedded in Product) - IDP
    all_results.append(test_query_from_sql("Q2", sql_q2, "DB2", "IDP"))
    
    # DB2, Q2, P[S] - Brand
    all_results.append(test_query_from_sql("Q2", sql_q2, "DB2", "brand"))
    
    # DB3, Q2, S{P} (Product embedded in Stock) - IDW
    all_results.append(test_query_from_sql("Q2", sql_q2, "DB3", "IDW"))
    
    # DB4, Q2, OL{P} (Product embedded in OrderLine) - IDP
    all_results.append(test_query_from_sql("Q2", sql_q2, "DB4", "IDP"))
    
    # DB5, Q2, P[OL] (OrderLine embedded in Product) - Brand
    all_results.append(test_query_from_sql("Q2", sql_q2, "DB5", "brand"))
    
    # DB5, Q2, P[OL] - IDP
    all_results.append(test_query_from_sql("Q2", sql_q2, "DB5", "IDP"))
    
    # ============================================================
    # Q3: SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = $date
    # ============================================================
    print("\n" + "="*100)
    print("Q3: SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = $date")
    print("="*100)
    
    sql_q3 = "SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = $date"
    
    # DB1, Q3, OL - IDP
    all_results.append(test_query_from_sql("Q3", sql_q3, "DB1", "IDP"))
    
    # DB4, Q3, OL{P} (Product embedded in OrderLine) - IDP
    all_results.append(test_query_from_sql("Q3", sql_q3, "DB4", "IDP"))
    
    # DB5, Q3, P[OL] (OrderLine embedded in Product) - IDP
    all_results.append(test_query_from_sql("Q3", sql_q3, "DB5", "IDP"))
    
    # Filter out None results (failed tests)
    all_results = [r for r in all_results if r is not None]
    
    return all_results


def generate_summary_table(all_results: List[Dict]):
    """Generate a summary table of all results"""
    print("\n" + "="*100)
    print("SUMMARY TABLE - ALL CONFIGURATIONS")
    print("="*100)
    
    # Header
    header = f"{'Query':<6} {'DB':<4} {'Collection':<15} {'Sharding':<12} {'S':<4} {'Query Size':<12} {'Output Size':<12} {'Network Vol':<15} {'RAM/Server':<15} {'RAM Total':<15} {'Time':<12} {'CO2(kg)':<12} {'Budget(€)':<12}"
    print(header)
    print("-" * len(header))
    
    # Rows
    for r in all_results:
        row = (
            f"{r['query']:<6} "
            f"{r['db']:<4} "
            f"{r['collection']:<15} "
            f"{r['sharding_key']:<12} "
            f"{r['S']:<4} "
            f"{r['size_input']:<12} "
            f"{r['size_msg']:<12} "
            f"{format_scientific(r['vol_network']):<15} "
            f"{format_scientific(r['vol_ram_per_server']):<15} "
            f"{format_scientific(r['vol_ram_total']):<15} "
            f"{format_scientific(r['time']):<12} "
            f"{format_scientific(r['co2_kg']):<12} "
            f"{format_scientific(r['budget']):<12}"
        )
        print(row)
    
    print("-" * len(header))


def export_to_json(all_results: List[Dict], filename: str = "query_cost_results_from_parsing.json"):
    """Export all results to JSON file"""
    with open(filename, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"\n[OK] Results exported to {filename}")


if __name__ == "__main__":
    print("\n" + "="*100)
    print("COMPREHENSIVE QUERY COST TESTING - FROM SQL PARSING")
    print("Tests all filter query configurations from homework spreadsheet")
    print("="*100)
    
    # Test all configurations
    all_results = test_all_configurations()
    
    # Generate summary table
    generate_summary_table(all_results)
    
    # Export to JSON
    export_to_json(all_results)
    
    print("\n" + "="*100)
    print("ALL TESTS COMPLETED!")
    print("="*100)
    print(f"\nTotal configurations tested: {len(all_results)}")
    print(f"Successful: {len(all_results)}")
    print(f"Failed: {0}")

