"""
Comprehensive test script for query cost calculator
Tests all filter query configurations from the spreadsheet:
- Q1, Q2, Q3 queries
- DB1, DB2, DB3, DB4, DB5 configurations
- Different sharding keys
- All cost metrics: Network, RAM, Time, CO2, Budget
"""

from services.query_cost import QueryCostCalculator
from services.statistics import Statistics
import json
from typing import Dict, List


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


def test_configuration(
    query_name: str,
    db_signature: str,
    collection: str,
    filter_fields: List[Dict],
    project_fields: List[Dict],
    sharding_key: str,
    expected_values: Dict = None
):
    """
    Test a single query configuration and display all metrics.
    
    Args:
        query_name: Q1, Q2, or Q3
        db_signature: DB1, DB2, DB3, DB4, or DB5
        collection: Collection name
        filter_fields: List of filter conditions
        project_fields: List of projection fields
        sharding_key: Sharding key
        expected_values: Optional dict with expected values for comparison
    """
    calc = QueryCostCalculator(
        db_signature=db_signature,
        collection_size_file="results_TD1.json"
    )
    
    query = {
        "collection": collection,
        "filter_fields": filter_fields,
        "project_fields": project_fields,
        "sharding_key": sharding_key,
        "has_index": True,
        "index_size": Statistics.DEFAULT_INDEX_SIZE
    }
    
    result = calc.calculate_query_cost(query)
    
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
    
    # Display results in spreadsheet-like format
    print(f"\n{'='*100}")
    print(f"Query: {query_name} | DB: {db_signature} | Collection: {collection} | Sharding: {sharding_key}")
    print(f"{'='*100}")
    
    print(f"\n📊 NETWORK:")
    print(f"  Sharding Key: {sharding_key}")
    print(f"  Sharding (S): {S} server(s)")
    print(f"  Query size: {size_input} B")
    print(f"  nb output: {nb_output:,}")
    print(f"  output size: {size_msg} B")
    print(f"  Network Vol (B): {format_scientific(vol_network)}")
    
    print(f"\n💾 RAM / SERVER SIDE:")
    print(f"  Index: 1")
    print(f"  Local Index: {sharding_key}")
    print(f"  Nb pointers per working: {nb_output}")
    print(f"  collection on doc size: {int(result['sizes']['size_doc_bytes'].split()[0]):,} B")
    print(f"  RAM Vol per server: {format_scientific(vol_ram_per_server)}")
    print(f"  nb srv working: {S}")
    print(f"  RAM Vol (total): {format_scientific(vol_ram_total)}")
    
    print(f"\n💰 COSTS:")
    print(f"  Time: {format_scientific(time_cost)} s")
    print(f"  kgCO2eq: {format_scientific(co2_total)}")
    print(f"  Budget: {format_scientific(budget)} €")
    
    # Compare with expected values if provided
    if expected_values:
        print(f"\n📋 COMPARISON WITH EXPECTED:")
        if "network_vol" in expected_values:
            exp_net = expected_values["network_vol"]
            diff = abs(vol_network - exp_net) / exp_net * 100 if exp_net > 0 else 0
            print(f"  Network Vol: Expected {format_scientific(exp_net)}, Got {format_scientific(vol_network)}, Diff: {diff:.1f}%")
        
        if "ram_vol_total" in expected_values:
            exp_ram = expected_values["ram_vol_total"]
            diff = abs(vol_ram_total - exp_ram) / exp_ram * 100 if exp_ram > 0 else 0
            print(f"  RAM Vol Total: Expected {format_scientific(exp_ram)}, Got {format_scientific(vol_ram_total)}, Diff: {diff:.1f}%")
        
        if "time" in expected_values:
            exp_time = expected_values["time"]
            diff = abs(time_cost - exp_time) / exp_time * 100 if exp_time > 0 else 0
            print(f"  Time: Expected {format_scientific(exp_time)}, Got {format_scientific(time_cost)}, Diff: {diff:.1f}%")
        
        if "co2" in expected_values:
            exp_co2 = expected_values["co2"]
            diff = abs(co2_total - exp_co2) / exp_co2 * 100 if exp_co2 > 0 else 0
            print(f"  CO2 (kg): Expected {format_scientific(exp_co2)}, Got {format_scientific(co2_total)}, Diff: {diff:.1f}%")
    
    return {
        "query": query_name,
        "db": db_signature,
        "collection": collection,
        "sharding_key": sharding_key,
        "S": S,
        "size_input": size_input,
        "size_msg": size_msg,
        "nb_output": nb_output,
        "vol_network": vol_network,
        "vol_ram_per_server": vol_ram_per_server,
        "vol_ram_total": vol_ram_total,
        "time": time_cost,
        "co2_kg": co2_total,
        "budget": budget
    }


def test_all_q1_configurations():
    """Test Q1: SELECT S.IDP, S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW"""
    print("\n" + "="*100)
    print("Q1: SELECT S.IDP, S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW")
    print("="*100)
    
    filter_fields = [
        {"name": "IDP", "type": "integer"},
        {"name": "IDW", "type": "integer"}
    ]
    project_fields = [
        {"name": "IDP", "type": "integer"},
        {"name": "quantity", "type": "integer"},
        {"name": "location", "type": "string"}
    ]
    
    results = []
    
    # Q1 - DB1 - Stock - IDP
    results.append(test_configuration(
        "Q1", "DB1", "Stock",
        filter_fields, project_fields, "IDP"
    ))
    
    # Q1 - DB1 - Stock - IDW
    results.append(test_configuration(
        "Q1", "DB1", "Stock",
        filter_fields, project_fields, "IDW"
    ))
    
    # Q1 - DB2 - Stock (embedded in Product) - IDP
    results.append(test_configuration(
        "Q1", "DB2", "Stock",
        filter_fields, project_fields, "IDP"
    ))
    
    # Q1 - DB2 - Stock (embedded in Product) - IDW
    results.append(test_configuration(
        "Q1", "DB2", "Stock",
        filter_fields, project_fields, "IDW"
    ))
    
    # Q1 - DB3 - Stock (Product embedded in Stock) - IDP
    results.append(test_configuration(
        "Q1", "DB3", "Stock",
        filter_fields, project_fields, "IDP"
    ))
    
    # Q1 - DB3 - Stock (Product embedded in Stock) - IDW
    results.append(test_configuration(
        "Q1", "DB3", "Stock",
        filter_fields, project_fields, "IDW"
    ))
    
    return results


def test_all_q2_configurations():
    """Test Q2: SELECT ID.IDP, P.name, P.price FROM Product P WHERE P.brand = $brand"""
    print("\n" + "="*100)
    print("Q2: SELECT ID.IDP, P.name, P.price FROM Product P WHERE P.brand = $brand")
    print("="*100)
    
    filter_fields = [
        {"name": "brand", "type": "string"}
    ]
    project_fields = [
        {"name": "IDP", "type": "integer"},
        {"name": "name", "type": "string"},
        {"name": "price", "type": "integer"}
    ]
    
    results = []
    
    # Q2 - DB0/DB1 - Product - Brand
    results.append(test_configuration(
        "Q2", "DB0", "Product",
        filter_fields, project_fields, "brand"
    ))
    
    # Q2 - DB0/DB1 - Product - IDP (not filtering on shard key)
    results.append(test_configuration(
        "Q2", "DB0", "Product",
        filter_fields, project_fields, "IDP"
    ))
    
    # Q2 - DB2 - Product - Brand
    results.append(test_configuration(
        "Q2", "DB2", "Product",
        filter_fields, project_fields, "brand"
    ))
    
    # Q2 - DB2 - Product - IDP
    results.append(test_configuration(
        "Q2", "DB2", "Product",
        filter_fields, project_fields, "IDP"
    ))
    
    # Q2 - DB3 - Product (embedded in Stock) - Brand
    results.append(test_configuration(
        "Q2", "DB3", "Product",
        filter_fields, project_fields, "brand"
    ))
    
    # Q2 - DB4 - Product (embedded in OrderLine) - Brand
    results.append(test_configuration(
        "Q2", "DB4", "Product",
        filter_fields, project_fields, "brand"
    ))
    
    # Q2 - DB5 - Product - Brand
    results.append(test_configuration(
        "Q2", "DB5", "Product",
        filter_fields, project_fields, "brand"
    ))
    
    return results


def test_all_q3_configurations():
    """Test Q3: SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = $date"""
    print("\n" + "="*100)
    print("Q3: SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = $date")
    print("="*100)
    
    filter_fields = [
        {"name": "date", "type": "date"}
    ]
    project_fields = [
        {"name": "IDP", "type": "integer"},
        {"name": "quantity", "type": "integer"}
    ]
    
    results = []
    
    # Q3 - DB0/DB1 - OrderLine - date
    results.append(test_configuration(
        "Q3", "DB0", "OrderLine",
        filter_fields, project_fields, "date"
    ))
    
    # Q3 - DB0/DB1 - OrderLine - IDC (not filtering on shard key)
    results.append(test_configuration(
        "Q3", "DB0", "OrderLine",
        filter_fields, project_fields, "IDC"
    ))
    
    # Q3 - DB2 - OrderLine - date
    results.append(test_configuration(
        "Q3", "DB2", "OrderLine",
        filter_fields, project_fields, "date"
    ))
    
    # Q3 - DB3 - OrderLine - date
    results.append(test_configuration(
        "Q3", "DB3", "OrderLine",
        filter_fields, project_fields, "date"
    ))
    
    # Q3 - DB4 - OrderLine (Product embedded) - date
    results.append(test_configuration(
        "Q3", "DB4", "OrderLine",
        filter_fields, project_fields, "date"
    ))
    
    # Q3 - DB5 - OrderLine (embedded in Product) - date
    results.append(test_configuration(
        "Q3", "DB5", "OrderLine",
        filter_fields, project_fields, "date"
    ))
    
    return results


def generate_summary_table(all_results: List[Dict]):
    """Generate a summary table of all results"""
    print("\n" + "="*100)
    print("SUMMARY TABLE - ALL CONFIGURATIONS")
    print("="*100)
    
    # Header
    header = f"{'Query':<6} {'DB':<4} {'Collection':<15} {'Sharding':<12} {'S':<4} {'Network Vol':<15} {'RAM/Server':<15} {'RAM Total':<15} {'Time':<12} {'CO2(kg)':<12} {'Budget(€)':<12}"
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
            f"{format_scientific(r['vol_network']):<15} "
            f"{format_scientific(r['vol_ram_per_server']):<15} "
            f"{format_scientific(r['vol_ram_total']):<15} "
            f"{format_scientific(r['time']):<12} "
            f"{format_scientific(r['co2_kg']):<12} "
            f"{format_scientific(r['budget']):<12}"
        )
        print(row)
    
    print("-" * len(header))


def export_to_json(all_results: List[Dict], filename: str = "query_cost_results.json"):
    """Export all results to JSON file"""
    with open(filename, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"\n✓ Results exported to {filename}")


if __name__ == "__main__":
    print("\n" + "="*100)
    print("COMPREHENSIVE QUERY COST TESTING")
    print("Testing all filter query configurations from spreadsheet")
    print("="*100)
    
    all_results = []
    
    # Test all Q1 configurations
    all_results.extend(test_all_q1_configurations())
    
    # Test all Q2 configurations
    all_results.extend(test_all_q2_configurations())
    
    # Test all Q3 configurations
    all_results.extend(test_all_q3_configurations())
    
    # Generate summary table
    generate_summary_table(all_results)
    
    # Export to JSON
    export_to_json(all_results)
    
    print("\n" + "="*100)
    print("ALL TESTS COMPLETED!")
    print("="*100)
    print(f"\nTotal configurations tested: {len(all_results)}")