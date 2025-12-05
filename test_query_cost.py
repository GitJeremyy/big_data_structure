"""
Test script for query cost calculator
"""

from services.query_cost import QueryCostCalculator
import json

def test_q1():
    """
    Q1: SELECT S.quantity, S.location
        FROM Stock S
        WHERE S.IDP = $IDP AND S.IDW = $IDW
    """
    print("\n" + "="*60)
    print("TESTING Q1: Stock query with IDP and IDW")
    print("="*60)
    
    calc = QueryCostCalculator(db_signature="DB1")
    
    query = {
        "collection": "Stock",
        "filter_fields": [
            {"name": "IDP", "type": "integer"},
            {"name": "IDW", "type": "integer"}
        ],
        "project_fields": [
            {"name": "quantity", "type": "boolean"},
            {"name": "location", "type": "boolean"}
        ],
        "sharding_key": "IDW",  # Stock sharded by warehouse ID
        "has_index": True,
        "index_size": 1_000_000  # 1 MB index
    }
    
    result = calc.calculate_query_cost(query)
    print(json.dumps(result, indent=2))
    
    # Verify size_input from formulas_TD2.tex example
    expected_size_input = 92  # From the LaTeX example
    actual_size_input = result["sizes"]["size_input"]
    print(f"\n✓ Expected size_input: {expected_size_input} B")
    print(f"✓ Actual size_input: {actual_size_input} B")
    print(f"✓ Match: {expected_size_input == actual_size_input}")
    
    # Verify size_msg from formulas_TD2.tex example
    expected_size_msg = 40  # From the LaTeX example
    actual_size_msg = result["sizes"]["size_msg"]
    print(f"\n✓ Expected size_msg: {expected_size_msg} B")
    print(f"✓ Actual size_msg: {actual_size_msg} B")
    print(f"✓ Match: {expected_size_msg == actual_size_msg}")


def test_q2():
    """
    Q2: SELECT P.name, P.price
        FROM Product P
        WHERE P.brand = $brand
    """
    print("\n" + "="*60)
    print("TESTING Q2: Product query by brand")
    print("="*60)
    
    calc = QueryCostCalculator(db_signature="DB0")
    
    query = {
        "collection": "Product",
        "filter_fields": [
            {"name": "brand", "type": "string"}
        ],
        "project_fields": [
            {"name": "name", "type": "boolean"},
            {"name": "price", "type": "boolean"}
        ],
        "sharding_key": "IDP",  # Product sharded by IDP, not brand
        "has_index": True,
        "index_size": 500_000  # 500 KB index on brand
    }
    
    result = calc.calculate_query_cost(query)
    print(json.dumps(result, indent=2))


def test_q3():
    """
    Q3: SELECT O.IDP, O.quantity
        FROM OrderLine O
        WHERE O.date = $date
    """
    print("\n" + "="*60)
    print("TESTING Q3: OrderLine query by date")
    print("="*60)
    
    calc = QueryCostCalculator(db_signature="DB0")
    
    query = {
        "collection": "OrderLine",
        "filter_fields": [
            {"name": "date", "type": "date"}
        ],
        "project_fields": [
            {"name": "IDP", "type": "boolean"},
            {"name": "quantity", "type": "boolean"}
        ],
        "sharding_key": "IDC",  # OrderLine sharded by client ID
        "has_index": True,
        "index_size": 10_000_000  # 10 MB index on date
    }
    
    result = calc.calculate_query_cost(query)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    test_q1()
    test_q2()
    test_q3()
