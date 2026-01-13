"""
SQL Query Parser for TD2
Parses simplified SQL queries to extract collection, filter_fields, and project_fields.
Now supports JOINs, aggregates (GROUP BY, SUM, COUNT, etc.), and subqueries.
"""

import re
import json
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from services.schema_client import Schema


class QueryParser:
    """
    Parses simplified SQL SELECT queries into structured format for cost calculation.
    
    Supported SQL format:
        - Basic: SELECT field1, field2, ... FROM collection WHERE field3 = value AND field4 = value
        - JOIN: SELECT field1, field2 FROM T1 JOIN T2 ON T1.id = T2.id WHERE ...
        - Aggregate: SELECT field1, SUM(field2) FROM collection GROUP BY field1
        - Subquery: SELECT ... FROM (SELECT ...) AS alias JOIN ...
    
    Field types are inferred from JSON schema using Schema._classify_attr_type
    """
    
    def __init__(self, db_signature: str = "DB1"):
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
        
        # Process top-level entities
        for entity in result["entities"]:
            collection_name = entity["name"]
            self.field_types[collection_name] = {}
            
            for attr in entity["attributes"]:
                field_name = attr["name"]
                field_type = self.schema._classify_attr_type(attr)
                self.field_types[collection_name][field_name] = field_type
        
        # Process nested entities (embedded collections like Stock in DB2)
        # This ensures types are consistent across all DBs regardless of embedding
        for entity in result["nested_entities"]:
            collection_name = entity["name"]
            # Only add if not already present (top-level takes precedence)
            if collection_name not in self.field_types:
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
            Dict with query_type, collections/collection, filter_fields, project_fields, etc.
            
        Example (filter):
            >>> parser = QueryParser(db_signature="DB0")
            >>> result = parser.parse("SELECT quantity, location FROM Stock WHERE IDP = 1 AND IDW = 2")
            
        Example (join):
            >>> result = parser.parse("SELECT P.name, S.quantity FROM Stock S JOIN Product P ON S.IDP = P.IDP WHERE S.IDW = $IDW")
        """
        type_overrides = type_overrides or {}
        
        # Clean up the SQL string
        sql = sql.strip()
        # Remove semicolon if present
        sql = sql.rstrip(';')
        # Normalize whitespace but preserve structure
        sql = re.sub(r'\s+', ' ', sql)
        
        # Check for aggregate functions
        has_aggregate = bool(re.search(r'\b(SUM|COUNT|AVG|MAX|MIN)\s*\(', sql, re.IGNORECASE))
        has_group_by = bool(re.search(r'\bGROUP\s+BY\b', sql, re.IGNORECASE))
        
        # Check for JOIN
        has_join = bool(re.search(r'\bJOIN\b', sql, re.IGNORECASE))
        
        # Check for subquery in FROM
        has_subquery = bool(re.search(r'FROM\s*\(', sql, re.IGNORECASE))
        
        # Determine query type
        if has_aggregate or has_group_by:
            return self._parse_aggregate_query(sql, type_overrides)
        elif has_join or has_subquery:
            return self._parse_join_query(sql, type_overrides)
        else:
            return self._parse_filter_query(sql, type_overrides)
    
    def _parse_filter_query(self, sql: str, type_overrides: Dict[str, str]) -> Dict:
        """Parse a simple filter query (SELECT-FROM-WHERE)."""
        # Extract SELECT clause
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE)
        if not select_match:
            raise ValueError("Could not parse SELECT clause")
        
        select_clause = select_match.group(1).strip()
        
        # Extract FROM clause - handle both "FROM table" and "FROM table alias"
        from_match = re.search(r'FROM\s+(\w+)(?:\s+(\w+))?(?:\s+WHERE|\s+JOIN|\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s*$)', sql, re.IGNORECASE)
        if not from_match:
            raise ValueError("Could not parse FROM clause")
        
        collection = from_match.group(1).strip()
        alias = from_match.group(2).strip() if from_match.group(2) else None
        
        # Extract WHERE clause (optional)
        where_match = re.search(r'WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s*$)', sql, re.IGNORECASE)
        where_clause = where_match.group(1).strip() if where_match else ""
        
        # Parse SELECT fields
        project_fields = self._parse_select_fields(select_clause, collection, type_overrides)
        
        # Parse WHERE conditions
        filter_fields = self._parse_where_clause(where_clause, collection, type_overrides)
        
        return {
            'query_type': 'filter',
            'collection': collection,
            'filter_fields': filter_fields,
            'project_fields': project_fields
        }
    
    def _parse_join_query(self, sql: str, type_overrides: Dict[str, str]) -> Dict:
        """Parse a JOIN query."""
        # Extract SELECT clause
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE)
        if not select_match:
            raise ValueError("Could not parse SELECT clause")
        
        select_clause = select_match.group(1).strip()
        
        # Extract FROM and JOIN clauses
        # Pattern: FROM table1 [alias1] [JOIN table2 [alias2] ON condition]*
        from_join_match = re.search(
            r'FROM\s+(.+?)(?:\s+WHERE|\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s*$)',
            sql,
            re.IGNORECASE
        )
        if not from_join_match:
            raise ValueError("Could not parse FROM/JOIN clause")
        
        from_join_clause = from_join_match.group(1).strip()
        
        # Parse collections and join conditions
        collections, aliases, join_conditions = self._parse_from_join_clause(from_join_clause)
        
        # Extract WHERE clause
        where_match = re.search(r'WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s*$)', sql, re.IGNORECASE)
        where_clause = where_match.group(1).strip() if where_match else ""
        
        # Parse SELECT fields (with collection context)
        project_fields = self._parse_select_fields_with_collections(select_clause, collections, aliases, type_overrides)
        
        # Parse WHERE conditions (with collection context)
        filter_fields = self._parse_where_clause_with_collections(where_clause, collections, aliases, type_overrides)
        
        return {
            'query_type': 'join',
            'collections': collections,
            'aliases': aliases,
            'join_conditions': join_conditions,
            'filter_fields': filter_fields,
            'project_fields': project_fields
        }
    
    def _parse_aggregate_query(self, sql: str, type_overrides: Dict[str, str]) -> Dict:
        """Parse an aggregate query with GROUP BY."""
        # Extract SELECT clause
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE)
        if not select_match:
            raise ValueError("Could not parse SELECT clause")
        
        select_clause = select_match.group(1).strip()
        
        # Check if it's a join with aggregates
        has_join = bool(re.search(r'\bJOIN\b', sql, re.IGNORECASE))
        
        if has_join:
            # Parse as join query first, then add aggregate info
            join_result = self._parse_join_query(sql, type_overrides)
            
            # Extract aggregate functions and GROUP BY
            aggregate_functions = self._parse_aggregate_functions(select_clause, join_result['collections'], join_result['aliases'], type_overrides)
            group_by_fields = self._parse_group_by_clause(sql, join_result['collections'], join_result['aliases'], type_overrides)
            
            join_result['query_type'] = 'join_aggregate'
            join_result['aggregate_functions'] = aggregate_functions
            join_result['group_by_fields'] = group_by_fields
            return join_result
        else:
            # Simple aggregate query
            from_match = re.search(r'FROM\s+(\w+)(?:\s+(\w+))?(?:\s+WHERE|\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s*$)', sql, re.IGNORECASE)
            if not from_match:
                raise ValueError("Could not parse FROM clause")
            
            collection = from_match.group(1).strip()
            alias = from_match.group(2).strip() if from_match.group(2) else None
            
            # Extract WHERE clause
            where_match = re.search(r'WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s*$)', sql, re.IGNORECASE)
            where_clause = where_match.group(1).strip() if where_match else ""
            
            # Parse aggregate functions
            aggregate_functions = self._parse_aggregate_functions(select_clause, [collection], {alias: collection} if alias else {}, type_overrides)
            
            # Parse GROUP BY
            group_by_fields = self._parse_group_by_clause(sql, [collection], {alias: collection} if alias else {}, type_overrides)
            
            # Parse regular project fields (non-aggregate)
            project_fields = self._parse_select_fields(select_clause, collection, type_overrides)
            
            # Parse WHERE conditions
            filter_fields = self._parse_where_clause(where_clause, collection, type_overrides)
            
            return {
                'query_type': 'aggregate',
                'collection': collection,
                'aggregate_functions': aggregate_functions,
                'group_by_fields': group_by_fields,
                'filter_fields': filter_fields,
                'project_fields': project_fields
            }
    
    def _parse_select_fields(self, select_clause: str, collection: str, type_overrides: Dict[str, str]) -> List[Dict]:
        """
        Parse SELECT clause into project_fields.
        Automatically infers field types from JSON schema.
        
        Args:
            select_clause: The SELECT portion (e.g., "S.quantity, S.location" or "quantity, location")
            collection: Collection name for schema lookup
            type_overrides: Type overrides dict
            
        Returns:
            List of dicts with 'name' and 'type' keys (type inferred from schema)
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
            
            # Determine type (same logic as filter fields)
            if field_name in type_overrides:
                field_type = type_overrides[field_name]
            else:
                field_type = self.infer_type(collection, field_name)
            
            fields.append({
                'name': field_name,
                'type': field_type
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
    
    def _parse_from_join_clause(self, from_join_clause: str) -> Tuple[List[str], Dict[str, str], List[Dict]]:
        """
        Parse FROM and JOIN clauses to extract collections, aliases, and join conditions.
        
        Args:
            from_join_clause: The FROM/JOIN portion (e.g., "Stock S JOIN Product P ON S.IDP = P.IDP")
            
        Returns:
            Tuple of (collections, aliases_dict, join_conditions)
        """
        collections = []
        aliases = {}  # {alias: collection_name}
        join_conditions = []
        
        # Split by JOIN keywords
        parts = re.split(r'\s+JOIN\s+', from_join_clause, flags=re.IGNORECASE)
        
        # Parse first table (FROM clause)
        first_part = parts[0].strip()
        first_match = re.match(r'(\w+)(?:\s+(\w+))?', first_part)
        if first_match:
            first_collection = first_match.group(1)
            first_alias = first_match.group(2) if first_match.group(2) else None
            collections.append(first_collection)
            if first_alias:
                aliases[first_alias] = first_collection
        
        # Parse subsequent JOINs
        for i in range(1, len(parts)):
            join_part = parts[i].strip()
            # Pattern: table [alias] ON condition
            join_match = re.match(r'(\w+)(?:\s+(\w+))?\s+ON\s+(.+)', join_part, re.IGNORECASE)
            if join_match:
                collection = join_match.group(1)
                alias = join_match.group(2) if join_match.group(2) else None
                on_condition = join_match.group(3).strip()
                
                collections.append(collection)
                if alias:
                    aliases[alias] = collection
                
                # Parse join condition: T1.field1 = T2.field2
                condition_match = re.match(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', on_condition, re.IGNORECASE)
                if condition_match:
                    left_alias = condition_match.group(1)
                    left_field = condition_match.group(2)
                    right_alias = condition_match.group(3)
                    right_field = condition_match.group(4)
                    
                    # Resolve collection names from aliases
                    left_collection = aliases.get(left_alias, left_alias)
                    right_collection = aliases.get(right_alias, right_alias)
                    
                    join_conditions.append({
                        'left_collection': left_collection,
                        'left_field': left_field,
                        'right_collection': right_collection,
                        'right_field': right_field
                    })
        
        return collections, aliases, join_conditions
    
    def _parse_select_fields_with_collections(
        self, 
        select_clause: str, 
        collections: List[str], 
        aliases: Dict[str, str],
        type_overrides: Dict[str, str]
    ) -> List[Dict]:
        """Parse SELECT fields with collection context for JOIN queries."""
        fields = []
        
        if select_clause.strip() == '*':
            raise ValueError("SELECT * is not supported - please specify fields explicitly")
        
        # Split by comma
        field_parts = [f.strip() for f in select_clause.split(',')]
        
        for field_part in field_parts:
            # Check for aggregate function first
            agg_match = re.match(r'(\w+)\s*\(\s*(\w+\.)?(\w+)\s*\)(?:\s+AS\s+(\w+))?', field_part, re.IGNORECASE)
            if agg_match:
                # This is an aggregate, skip for now (handled separately in aggregate parsing)
                continue
            
            # Parse field with optional alias: T.field or field
            # Also handle AS alias: field AS alias
            field_match = re.match(r'(\w+)\.(\w+)(?:\s+AS\s+(\w+))?|(\w+)(?:\s+AS\s+(\w+))?', field_part, re.IGNORECASE)
            if field_match:
                if field_match.group(1):  # Has collection prefix: T.field
                    alias_or_collection = field_match.group(1)
                    field_name = field_match.group(2)
                    collection = aliases.get(alias_or_collection, alias_or_collection)
                else:  # No prefix: field
                    field_name = field_match.group(4)
                    collection = collections[0] if collections else None
                
                if not collection:
                    continue
                
                # Determine type
                if field_name in type_overrides:
                    field_type = type_overrides[field_name]
                else:
                    field_type = self.infer_type(collection, field_name)
                
                fields.append({
                    'collection': collection,
                    'name': field_name,
                    'type': field_type
                })
        
        return fields
    
    def _parse_where_clause_with_collections(
        self,
        where_clause: str,
        collections: List[str],
        aliases: Dict[str, str],
        type_overrides: Dict[str, str]
    ) -> List[Dict]:
        """Parse WHERE clause with collection context for JOIN queries."""
        fields = []
        
        if not where_clause:
            return fields
        
        # Split by AND
        conditions = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
        
        for condition in conditions:
            condition = condition.strip()
            
            # Parse: T.field = value or field = value
            match = re.match(r'(\w+)\.(\w+)\s*=\s*.+|(\w+)\s*=\s*.+', condition, re.IGNORECASE)
            if match:
                if match.group(1):  # Has collection prefix
                    alias_or_collection = match.group(1)
                    field_name = match.group(2)
                    collection = aliases.get(alias_or_collection, alias_or_collection)
                else:  # No prefix
                    field_name = match.group(3)
                    collection = collections[0] if collections else None
                
                if not collection:
                    continue
                
                # Determine type
                if field_name in type_overrides:
                    field_type = type_overrides[field_name]
                else:
                    field_type = self.infer_type(collection, field_name)
                
                fields.append({
                    'collection': collection,
                    'name': field_name,
                    'type': field_type
                })
        
        return fields
    
    def _parse_aggregate_functions(
        self,
        select_clause: str,
        collections: List[str],
        aliases: Dict[str, str],
        type_overrides: Dict[str, str]
    ) -> List[Dict]:
        """Parse aggregate functions from SELECT clause."""
        aggregate_functions = []
        
        # Split by comma
        field_parts = [f.strip() for f in select_clause.split(',')]
        
        for field_part in field_parts:
            # Match: SUM(field) or SUM(T.field) or SUM(field) AS alias
            agg_match = re.match(
                r'(\w+)\s*\(\s*(\w+\.)?(\w+)\s*\)(?:\s+AS\s+(\w+))?',
                field_part,
                re.IGNORECASE
            )
            if agg_match:
                func_name = agg_match.group(1).upper()
                alias_or_collection = agg_match.group(2).rstrip('.') if agg_match.group(2) else None
                field_name = agg_match.group(3)
                result_alias = agg_match.group(4) if agg_match.group(4) else None
                
                # Determine collection
                if alias_or_collection:
                    collection = aliases.get(alias_or_collection, alias_or_collection)
                else:
                    collection = collections[0] if collections else None
                
                if not collection:
                    continue
                
                # Determine field type (for result)
                if field_name in type_overrides:
                    field_type = type_overrides[field_name]
                else:
                    field_type = self.infer_type(collection, field_name)
                
                aggregate_functions.append({
                    'function': func_name,
                    'collection': collection,
                    'field': field_name,
                    'type': field_type,
                    'alias': result_alias or f"{func_name.lower()}_{field_name}"
                })
        
        return aggregate_functions
    
    def _parse_group_by_clause(
        self,
        sql: str,
        collections: List[str],
        aliases: Dict[str, str],
        type_overrides: Dict[str, str]
    ) -> List[Dict]:
        """Parse GROUP BY clause."""
        group_by_fields = []
        
        # Extract GROUP BY clause
        group_by_match = re.search(r'GROUP\s+BY\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s*$)', sql, re.IGNORECASE)
        if not group_by_match:
            return group_by_fields
        
        group_by_clause = group_by_match.group(1).strip()
        
        # Split by comma
        field_parts = [f.strip() for f in group_by_clause.split(',')]
        
        for field_part in field_parts:
            # Parse: T.field or field
            field_match = re.match(r'(\w+)\.(\w+)|(\w+)', field_part, re.IGNORECASE)
            if field_match:
                if field_match.group(1):  # Has collection prefix
                    alias_or_collection = field_match.group(1)
                    field_name = field_match.group(2)
                    collection = aliases.get(alias_or_collection, alias_or_collection)
                else:  # No prefix
                    field_name = field_match.group(3)
                    collection = collections[0] if collections else None
                
                if not collection:
                    continue
                
                # Determine type
                if field_name in type_overrides:
                    field_type = type_overrides[field_name]
                else:
                    field_type = self.infer_type(collection, field_name)
                
                group_by_fields.append({
                    'collection': collection,
                    'name': field_name,
                    'type': field_type
                })
        
        return group_by_fields


    def _parse_subquery(self, sql: str) -> Optional[Dict]:
        """
        Detect and parse subquery in FROM clause.
        Returns None if no subquery found, otherwise returns parsed subquery.
        """
        # Look for pattern: FROM (SELECT ...) AS alias
        subquery_match = re.search(r'FROM\s*\(\s*(SELECT.+?)\s*\)\s+AS\s+(\w+)', sql, re.IGNORECASE | re.DOTALL)
        if subquery_match:
            subquery_sql = subquery_match.group(1).strip()
            subquery_alias = subquery_match.group(2).strip()
            
            # Recursively parse the subquery
            subquery_result = self.parse(subquery_sql)
            subquery_result['alias'] = subquery_alias
            
            return subquery_result
        
        return None


# Helper function for quick parsing
def parse_query(sql: str, db_signature: str = "DB1", type_overrides: Optional[Dict[str, str]] = None) -> Dict:
    """
    Convenience function to parse a SQL query.
    
    Args:
        sql: SQL query string
        db_signature: Database signature (DB0-DB5) to use for schema lookup
        type_overrides: Optional dict to override inferred types
        
    Returns:
        Parsed query dict with query_type, collections/collection, filter_fields, project_fields, etc.
    """
    parser = QueryParser(db_signature)
    return parser.parse(sql, type_overrides)
