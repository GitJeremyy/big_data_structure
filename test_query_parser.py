"""
Test script for SQL query parser
"""

from services.query_parser import QueryParser, parse_query
import json


def test_parser_q1():
    """Test parsing Q1: Stock query with IDP and IDW"""
    print("\n" + "="*60)
    print("TESTING PARSER Q1: Stock query")
    print("="*60)
    
    sql = "SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW"
    
    parser = QueryParser(db_signature="DB1")
    result = parser.parse(sql)
    
    print(f"\nOriginal SQL:\n{sql}")
    print(f"\nParsed result:")
    print(json.dumps(result, indent=2))
    
    # Verify
    assert result['collection'] == 'Stock'
    assert len(result['filter_fields']) == 2
    assert result['filter_fields'][0]['name'] == 'IDP'
    assert result['filter_fields'][0]['type'] == 'number'  # From JSON schema
    assert result['filter_fields'][1]['name'] == 'IDW'
    assert result['filter_fields'][1]['type'] == 'number'  # From JSON schema
    assert len(result['project_fields']) == 2
    assert result['project_fields'][0]['name'] == 'quantity'
    assert result['project_fields'][0]['type'] == 'boolean'
    
    print("\n✓ All assertions passed!")


def test_parser_q2():
    """Test parsing Q2: Product query by brand"""
    print("\n" + "="*60)
    print("TESTING PARSER Q2: Product query")
    print("="*60)
    
    sql = "SELECT P.name, P.price FROM Product P WHERE P.brand = $brand"
    
    result = parse_query(sql, db_signature="DB0")
    
    print(f"\nOriginal SQL:\n{sql}")
    print(f"\nParsed result:")
    print(json.dumps(result, indent=2))
    
    # Verify
    assert result['collection'] == 'Product'
    assert len(result['filter_fields']) == 1
    assert result['filter_fields'][0]['name'] == 'brand'
    assert result['filter_fields'][0]['type'] == 'string'  # Auto-inferred
    assert len(result['project_fields']) == 2
    
    print("\n✓ All assertions passed!")


def test_parser_q3():
    """Test parsing Q3: OrderLine query by date"""
    print("\n" + "="*60)
    print("TESTING PARSER Q3: OrderLine query")
    print("="*60)
    
    sql = "SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = $date"
    
    result = parse_query(sql, db_signature="DB0")
    
    print(f"\nOriginal SQL:\n{sql}")
    print(f"\nParsed result:")
    print(json.dumps(result, indent=2))
    
    # Verify
    assert result['collection'] == 'OrderLine'
    assert len(result['filter_fields']) == 1
    assert result['filter_fields'][0]['name'] == 'date'
    assert result['filter_fields'][0]['type'] == 'date'  # Auto-inferred
    assert len(result['project_fields']) == 2
    
    print("\n✓ All assertions passed!")


def test_parser_with_type_override():
    """Test parsing with type overrides"""
    print("\n" + "="*60)
    print("TESTING PARSER: Type overrides")
    print("="*60)
    
    sql = "SELECT name, description FROM Product WHERE custom_field = 123"
    
    # Override the inferred type for custom_field
    result = parse_query(sql, db_signature="DB0", type_overrides={'custom_field': 'longstring'})
    
    print(f"\nOriginal SQL:\n{sql}")
    print(f"\nParsed result (with type override):")
    print(json.dumps(result, indent=2))
    
    assert result['filter_fields'][0]['name'] == 'custom_field'
    assert result['filter_fields'][0]['type'] == 'longstring'  # Overridden
    
    print("\n✓ Type override works!")


def test_parser_simple():
    """Test parsing without table aliases"""
    print("\n" + "="*60)
    print("TESTING PARSER: Simple query without aliases")
    print("="*60)
    
    sql = "SELECT quantity, location FROM Stock WHERE IDP = 1 AND IDW = 2"
    
    result = parse_query(sql, db_signature="DB1")
    
    print(f"\nOriginal SQL:\n{sql}")
    print(f"\nParsed result:")
    print(json.dumps(result, indent=2))
    
    assert result['collection'] == 'Stock'
    assert len(result['filter_fields']) == 2
    assert len(result['project_fields']) == 2
    
    print("\n✓ Simple query parsed successfully!")


if __name__ == "__main__":
    test_parser_q1()
    test_parser_q2()
    test_parser_q3()
    test_parser_with_type_override()
    test_parser_simple()
    
    print("\n" + "="*60)
    print("ALL PARSER TESTS PASSED!")
    print("="*60)
