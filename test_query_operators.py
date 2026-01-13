"""
Test script for query operators (filter and nested loop join)
"""

from services.query_parser import parse_query
from services.query_cost import QueryCostCalculator
import json


def test_filter_with_sharding():
    """Test filter operator with sharding"""
    print("\n" + "="*60)
    print("TEST: Filter with Sharding")
    print("="*60)
    
    calculator = QueryCostCalculator(db_signature="DB1", collection_size_file="results_TD1.json")
    
    # Q1: Stock query with IDP sharding
    result = calculator.filter_with_sharding(
        collection="Stock",
        project_fields=[
            {"name": "quantity", "type": "boolean"},
            {"name": "location", "type": "boolean"}
        ],
        filter_fields=[
            {"name": "IDP", "type": "number"},
            {"name": "IDW", "type": "number"}
        ],
        sharding_key="IDP",
        has_index=True
    )
    
    print(f"\nOperator: {result['operator']}")
    print(f"Collection: {result['collection']}")
    print(f"Output documents: {result['nb_docs']:,}")
    print(f"Total size: {result['total_size_bytes']:,} bytes")
    print(f"Selectivity: {result['selectivity']:.6f}")
    print(f"Servers accessed: {result['S_servers']}")
    print(f"\nCosts:")
    print(f"  Network volume: {result['costs']['vol_network']:.2e} B")
    print(f"  RAM volume: {result['costs']['vol_RAM']:.2e} B")
    print(f"  Time: {result['costs']['time']:.2e} s")
    print(f"  Carbon total: {result['costs']['carbon_total']:.2e} gCO2")
    
    assert result['operator'] == 'filter_with_sharding'
    assert result['S_servers'] == 1  # Filtering on sharding key
    assert result['nb_docs'] > 0
    assert result['total_size_bytes'] > 0
    
    print("\n✓ Filter with sharding test passed!")


def test_filter_without_sharding():
    """Test filter operator without sharding"""
    print("\n" + "="*60)
    print("TEST: Filter without Sharding")
    print("="*60)
    
    calculator = QueryCostCalculator(db_signature="DB0", collection_size_file="results_TD1.json")
    
    # Q2: Product query by brand (no sharding on brand)
    result = calculator.filter_without_sharding(
        collection="Product",
        project_fields=[
            {"name": "name", "type": "boolean"},
            {"name": "price", "type": "boolean"}
        ],
        filter_fields=[
            {"name": "brand", "type": "string"}
        ],
        has_index=True
    )
    
    print(f"\nOperator: {result['operator']}")
    print(f"Collection: {result['collection']}")
    print(f"Output documents: {result['nb_docs']:,}")
    print(f"Total size: {result['total_size_bytes']:,} bytes")
    print(f"Servers accessed: {result['S_servers']}")
    print(f"\nCosts:")
    print(f"  Network volume: {result['costs']['vol_network']:.2e} B")
    print(f"  RAM volume: {result['costs']['vol_RAM']:.2e} B")
    print(f"  Time: {result['costs']['time']:.2e} s")
    
    assert result['operator'] == 'filter_without_sharding'
    assert result['S_servers'] == 1000  # All servers (no sharding)
    assert result['nb_docs'] > 0
    
    print("\n✓ Filter without sharding test passed!")


def test_nested_loop_join_with_sharding():
    """Test nested loop join with sharding"""
    print("\n" + "="*60)
    print("TEST: Nested Loop Join with Sharding")
    print("="*60)
    
    calculator = QueryCostCalculator(db_signature="DB0", collection_size_file="results_TD1.json")
    
    # Q4: Stock JOIN Product with IDW filter
    result = calculator.nested_loop_join_with_sharding(
        collection1="Stock",
        collection2="Product",
        join_conditions=[
            {
                "left_collection": "Stock",
                "left_field": "IDP",
                "right_collection": "Product",
                "right_field": "IDP"
            }
        ],
        project_fields=[
            {"name": "name", "collection": "Product", "type": "boolean"},
            {"name": "quantity", "collection": "Stock", "type": "boolean"}
        ],
        filter_fields=[
            {"name": "IDW", "collection": "Stock", "type": "number"}
        ],
        sharding_key="IDW",  # Sharding on Stock.IDW
        has_index=True
    )
    
    print(f"\nOperator: {result['operator']}")
    print(f"Collections: {result['collections']}")
    print(f"Output documents: {result['nb_docs']:,}")
    print(f"Total size: {result['total_size_bytes']:,} bytes")
    print(f"Selectivity1 (Stock): {result['selectivity1']:.6f}")
    print(f"Selectivity2 (Product): {result['selectivity2']:.6f}")
    print(f"Join selectivity: {result['join_selectivity']:.6f}")
    print(f"Servers (outer): {result['S_servers_outer']}")
    print(f"Servers (inner): {result['S_servers_inner']}")
    print(f"\nCosts:")
    print(f"  Network volume: {result['costs']['vol_network']:.2e} B")
    print(f"  RAM volume: {result['costs']['vol_RAM']:.2e} B")
    print(f"  Time: {result['costs']['time']:.2e} s")
    print(f"  Carbon total: {result['costs']['carbon_total']:.2e} gCO2")
    
    assert result['operator'] == 'nested_loop_join_with_sharding'
    assert result['S_servers_outer'] == 1  # Filtering on sharding key
    assert result['nb_docs'] > 0
    
    print("\n✓ Nested loop join with sharding test passed!")


def test_nested_loop_join_without_sharding():
    """Test nested loop join without sharding"""
    print("\n" + "="*60)
    print("TEST: Nested Loop Join without Sharding")
    print("="*60)
    
    calculator = QueryCostCalculator(db_signature="DB0", collection_size_file="results_TD1.json")
    
    # Q5: Product JOIN Stock with brand filter (no sharding)
    result = calculator.nested_loop_join_without_sharding(
        collection1="Product",
        collection2="Stock",
        join_conditions=[
            {
                "left_collection": "Product",
                "left_field": "IDP",
                "right_collection": "Stock",
                "right_field": "IDP"
            }
        ],
        project_fields=[
            {"name": "name", "collection": "Product", "type": "boolean"},
            {"name": "price", "collection": "Product", "type": "boolean"},
            {"name": "IDW", "collection": "Stock", "type": "boolean"},
            {"name": "quantity", "collection": "Stock", "type": "boolean"}
        ],
        filter_fields=[
            {"name": "brand", "collection": "Product", "type": "string"}
        ],
        has_index=True
    )
    
    print(f"\nOperator: {result['operator']}")
    print(f"Collections: {result['collections']}")
    print(f"Output documents: {result['nb_docs']:,}")
    print(f"Total size: {result['total_size_bytes']:,} bytes")
    print(f"Servers (outer): {result['S_servers_outer']}")
    print(f"Servers (inner): {result['S_servers_inner']}")
    print(f"\nCosts:")
    print(f"  Network volume: {result['costs']['vol_network']:.2e} B")
    print(f"  RAM volume: {result['costs']['vol_RAM']:.2e} B")
    print(f"  Time: {result['costs']['time']:.2e} s")
    
    assert result['operator'] == 'nested_loop_join_without_sharding'
    assert result['S_servers_outer'] == 1000  # All servers (no sharding)
    assert result['nb_docs'] > 0
    
    print("\n✓ Nested loop join without sharding test passed!")


def test_query_cost_with_parsed_queries():
    """Test calculate_query_cost with parsed queries"""
    print("\n" + "="*60)
    print("TEST: Query Cost with Parsed Queries")
    print("="*60)
    
    calculator = QueryCostCalculator(db_signature="DB0", collection_size_file="results_TD1.json")
    
    # Test Q3: Filter query
    sql_q3 = "SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = $date"
    parsed_q3 = parse_query(sql_q3, db_signature="DB0")
    
    query_q3 = {
        **parsed_q3,
        "sharding_key": "date",
        "has_index": True
    }
    
    result_q3 = calculator.calculate_query_cost(query_q3)
    
    print(f"\nQ3 (Filter):")
    print(f"  Collection: {result_q3['query']['collection']}")
    print(f"  Results: {result_q3['distribution']['res_q_results']:,}")
    print(f"  Time: {result_q3['costs']['time']}")
    
    assert result_q3['query']['collection'] == 'OrderLine'
    
    # Test Q4: Join query
    sql_q4 = "SELECT P.name, S.quantity FROM Stock S JOIN Product P ON S.IDP = P.IDP WHERE S.IDW = $IDW"
    parsed_q4 = parse_query(sql_q4, db_signature="DB0")
    
    query_q4 = {
        **parsed_q4,
        "sharding_key": "IDW",
        "has_index": True
    }
    
    result_q4 = calculator.calculate_query_cost(query_q4)
    
    print(f"\nQ4 (Join):")
    print(f"  Collections: {result_q4['query']['collections']}")
    print(f"  Results: {result_q4['output']['nb_docs']:,}")
    print(f"  Time: {result_q4['costs']['time']}")
    
    assert result_q4['query']['query_type'] == 'join'
    assert len(result_q4['query']['collections']) == 2
    
    # Test Q5: Join query
    sql_q5 = 'SELECT P.name, P.price, S.IDW, S.quantity FROM Product P JOIN Stock S ON P.IDP = S.IDP WHERE P.brand = "Apple"'
    parsed_q5 = parse_query(sql_q5, db_signature="DB0")
    
    query_q5 = {
        **parsed_q5,
        "sharding_key": None,  # No sharding
        "has_index": True
    }
    
    result_q5 = calculator.calculate_query_cost(query_q5)
    
    print(f"\nQ5 (Join):")
    print(f"  Collections: {result_q5['query']['collections']}")
    print(f"  Results: {result_q5['output']['nb_docs']:,}")
    print(f"  Time: {result_q5['costs']['time']}")
    
    assert result_q5['query']['query_type'] == 'join'
    
    print("\n✓ Query cost with parsed queries test passed!")


def test_operator_comparison():
    """Compare costs between with/without sharding"""
    print("\n" + "="*60)
    print("TEST: Operator Comparison (Sharding Impact)")
    print("="*60)
    
    calculator = QueryCostCalculator(db_signature="DB1", collection_size_file="results_TD1.json")
    
    filter_fields = [
        {"name": "IDP", "type": "number"},
        {"name": "IDW", "type": "number"}
    ]
    project_fields = [
        {"name": "quantity", "type": "boolean"},
        {"name": "location", "type": "boolean"}
    ]
    
    # With sharding (on IDP)
    result_with = calculator.filter_with_sharding(
        collection="Stock",
        project_fields=project_fields,
        filter_fields=filter_fields,
        sharding_key="IDP",
        has_index=True
    )
    
    # Without sharding
    result_without = calculator.filter_without_sharding(
        collection="Stock",
        project_fields=project_fields,
        filter_fields=filter_fields,
        has_index=True
    )
    
    print(f"\nFilter with sharding (IDP):")
    print(f"  Servers: {result_with['S_servers']}")
    print(f"  Network: {result_with['costs']['vol_network']:.2e} B")
    print(f"  Time: {result_with['costs']['time']:.2e} s")
    
    print(f"\nFilter without sharding:")
    print(f"  Servers: {result_without['S_servers']}")
    print(f"  Network: {result_without['costs']['vol_network']:.2e} B")
    print(f"  Time: {result_without['costs']['time']:.2e} s")
    
    improvement = result_without['costs']['vol_network'] / result_with['costs']['vol_network']
    print(f"\nSharding improvement: {improvement:.1f}x less network traffic")
    
    assert result_with['S_servers'] == 1
    assert result_without['S_servers'] == 1000
    assert result_with['costs']['vol_network'] < result_without['costs']['vol_network']
    
    print("\n✓ Operator comparison test passed!")


if __name__ == "__main__":
    # Test individual operators
    test_filter_with_sharding()
    test_filter_without_sharding()
    test_nested_loop_join_with_sharding()
    test_nested_loop_join_without_sharding()
    
    # Test with parsed queries
    test_query_cost_with_parsed_queries()
    
    # Test comparisons
    test_operator_comparison()
    
    print("\n" + "="*60)
    print("ALL OPERATOR TESTS PASSED!")
    print("="*60)