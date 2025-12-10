"""
SQL Query Parser for TD2
Parses simplified SQL queries to extract collection, filter_fields, and project_fields.
"""

import re
import json
from typing import Dict, List, Optional
from pathlib import Path
from services.schema_client import Schema


class QueryParser:
    """
    Parses simplified SQL SELECT queries into structured format for cost calculation.
    
    Supported SQL format:
        SELECT field1, field2, ... FROM collection WHERE field3 = value AND field4 = value
    
    Limitations:
        - Basic SQL only (SELECT, FROM, WHERE with equality and AND)
        - No JOINs, subqueries, or complex expressions
        - Field types are inferred from JSON schema using Schema._classify_attr_type
    """
    
    def __init__(self, db_signature: str = "DB0"):
        """
        Initialize parser with database schema.
        
        Args:
            db_signature: Database signature (DB0-DB5) to load schema from
        """
        self.db_signature = db_signature
        self._load_schema()
    
    def _load_schema(self):
        """Load JSON schema for the database signature"""
        schema_path = Path(__file__).resolve().parent / 'JSON_schema' / f'json-schema-{self.db_signature}.json'
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
        
        self.schema = Schema(schema_data)
        
        # Build a lookup for field types by collection
        self._build_field_type_lookup()
    
    def _build_field_type_lookup(self):
        """Build a lookup dict: {collection_name: {field_name: type}}"""
        self.field_types = {}
        
        result = self.schema.detect_entities_and_relations()
        
        for entity in result["entities"]:
            collection_name = entity["name"]
            self.field_types[collection_name] = {}
            
            for attr in entity["attributes"]:
                field_name = attr["name"]
                field_type = self.schema._classify_attr_type(attr)
                self.field_types[collection_name][field_name] = field_type
    
    def infer_type(self, collection: str, field_name: str) -> str:
        """
        Infer field type from JSON schema using Schema._classify_attr_type.
        
        Args:
            collection: Collection name
            field_name: The field name to infer type for
            
        Returns:
            Inferred type (defaults to 'integer' if not found in schema)
        """
        if collection in self.field_types and field_name in self.field_types[collection]:
            return self.field_types[collection][field_name]
        
        # Default to integer for unknown fields
        return 'integer'
    
    def parse(self, sql: str, type_overrides: Optional[Dict[str, str]] = None) -> Dict:
        """
        Parse a SQL query into structured format.
        
        Args:
            sql: SQL query string
            type_overrides: Optional dict to override inferred types {field_name: type}
            
        Returns:
            Dict with keys: collection, filter_fields, project_fields
            
        Example:
            >>> parser = QueryParser(db_signature="DB0")
            >>> result = parser.parse("SELECT quantity, location FROM Stock WHERE IDP = 1 AND IDW = 2")
            >>> print(result)
            {
                'collection': 'Stock',
                'filter_fields': [
                    {'name': 'IDP', 'type': 'integer'},
                    {'name': 'IDW', 'type': 'integer'}
                ],
                'project_fields': [
                    {'name': 'quantity', 'type': 'boolean'},
                    {'name': 'location', 'type': 'boolean'}
                ]
            }
        """
        type_overrides = type_overrides or {}
        
        # Clean up the SQL string
        sql = sql.strip()
        # Remove semicolon if present
        sql = sql.rstrip(';')
        # Normalize whitespace
        sql = re.sub(r'\s+', ' ', sql)
        
        # Extract SELECT clause
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE)
        if not select_match:
            raise ValueError("Could not parse SELECT clause")
        
        select_clause = select_match.group(1).strip()
        
        # Extract FROM clause
        from_match = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
        if not from_match:
            raise ValueError("Could not parse FROM clause")
        
        collection = from_match.group(1).strip()
        
        # Remove table alias if present (e.g., "Stock S" -> "Stock")
        # Already handled by \w+ which stops at space
        
        # Extract WHERE clause (optional)
        where_match = re.search(r'WHERE\s+(.+)$', sql, re.IGNORECASE)
        where_clause = where_match.group(1).strip() if where_match else ""
        
        # Parse SELECT fields
        project_fields = self._parse_select_fields(select_clause, collection, type_overrides)
        
        # Parse WHERE conditions
        filter_fields = self._parse_where_clause(where_clause, collection, type_overrides)
        
        return {
            'collection': collection,
            'filter_fields': filter_fields,
            'project_fields': project_fields
        }
    
    def _parse_select_fields(self, select_clause: str, collection: str, type_overrides: Dict[str, str]) -> List[Dict]:
        """
        Parse SELECT clause into project_fields.
        
        Args:
            select_clause: The SELECT portion (e.g., "S.quantity, S.location" or "quantity, location")
            collection: Collection name for schema lookup
            type_overrides: Type overrides dict
            
        Returns:
            List of dicts with 'name' and 'type' keys (type is always 'boolean' for project fields)
        """
        fields = []
        
        # Handle SELECT * (not typically used in cost analysis)
        if select_clause.strip() == '*':
            raise ValueError("SELECT * is not supported - please specify fields explicitly")
        
        # Split by comma
        field_names = [f.strip() for f in select_clause.split(',')]
        
        for field_name in field_names:
            # Remove table alias prefix if present (e.g., "S.quantity" -> "quantity")
            if '.' in field_name:
                field_name = field_name.split('.')[-1]
            
            field_name = field_name.strip()
            
            # For project fields, type is always 'boolean' (indicates field inclusion)
            fields.append({
                'name': field_name,
                'type': 'boolean'
            })
        
        return fields
    
    def _parse_where_clause(self, where_clause: str, collection: str, type_overrides: Dict[str, str]) -> List[Dict]:
        """
        Parse WHERE clause into filter_fields.
        
        Args:
            where_clause: The WHERE portion (e.g., "S.IDP = $IDP AND S.IDW = $IDW")
            collection: Collection name for schema lookup
            type_overrides: Type overrides dict
            
        Returns:
            List of dicts with 'name' and 'type' keys
        """
        fields = []
        
        if not where_clause:
            return fields
        
        # Split by AND (simple approach - doesn't handle OR or complex logic)
        conditions = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
        
        for condition in conditions:
            condition = condition.strip()
            
            # Parse equality condition: field = value or field = $param
            # Supports: field = value, T.field = value, field=$param, etc.
            match = re.match(r'(\w+\.)?(\w+)\s*=\s*.+', condition, re.IGNORECASE)
            
            if match:
                field_name = match.group(2)
                
                # Determine type
                if field_name in type_overrides:
                    field_type = type_overrides[field_name]
                else:
                    field_type = self.infer_type(collection, field_name)
                
                fields.append({
                    'name': field_name,
                    'type': field_type
                })
        
        return fields


# Helper function for quick parsing
def parse_query(sql: str, db_signature: str = "DB0", type_overrides: Optional[Dict[str, str]] = None) -> Dict:
    """
    Convenience function to parse a SQL query.
    
    Args:
        sql: SQL query string
        db_signature: Database signature (DB0-DB5) to use for schema lookup
        type_overrides: Optional dict to override inferred types
        
    Returns:
        Parsed query dict with collection, filter_fields, project_fields
    """
    parser = QueryParser(db_signature)
    return parser.parse(sql, type_overrides)
