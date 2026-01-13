"""
Query Cost Calculator for TD2
Implements formulas from formulas_TD2.tex for computing query costs.
Now supports both filter queries and nested loop joins.
"""

from typing import Dict, List, Optional, Tuple
from services.statistics import Statistics
from services.schema_client import Schema
from services.sizing import Sizer
import json
from pathlib import Path


class QueryCostCalculator:
    """
    Calculates query execution costs based on formulas from formulas_TD2.tex
    Supports both filter queries and nested loop joins.
    """

    # Collection mapping for embedded collections
    # Maps logical collection names to physical collection names per DB
    COLLECTION_MAPPING = {
        "DB1": {
            "Categories": "Product",  # Categories embedded in Product
            "Supplier": "Product"     # Supplier embedded in Product
        },
        "DB2": {
            "Stock": "Product",      # Stock is embedded in Product
            "Categories": "Product",  # Categories embedded in Product
            "Supplier": "Product"     # Supplier embedded in Product
        },
        "DB3": {
            "Product": "Stock",       # Product is embedded in Stock
            "Categories": "Stock",    # Categories embedded in Stock (via Product)
            "Supplier": "Stock"       # Supplier embedded in Stock (via Product)
        },
        "DB4": {
            "Product": "OrderLine",   # Product is embedded in OrderLine
            "Categories": "OrderLine", # Categories embedded in OrderLine (via Product)
            "Supplier": "OrderLine"   # Supplier embedded in OrderLine (via Product)
        },
        "DB5": {
            "OrderLine": "Product",   # OrderLine is embedded in Product
            "Stock": "Product",       # Stock is embedded in Product
            "Categories": "Product",  # Categories embedded in Product
            "Supplier": "Product"     # Supplier embedded in Product
        }
    }

    def __init__(
        self, 
        db_signature: str = "DB1", 
        collection_size_file: str = "results_TD1.json", 
        manual_counts: Optional[Dict[str, Dict]] = None,
        manual_doc_sizes: Optional[Dict[str, int]] = None
    ):
        """
        Initialize QueryCostCalculator.
        
        Args:
            db_signature: Database signature (DB1-DB5)
            collection_size_file: Path to collection size JSON file
            manual_counts: Optional dict mapping collection names to manual field counts.
                          If provided, these override automatic counting from schema.
                          Format: {
                              "CollectionName": {
                                  "integer": int,
                                  "string": int,
                                  "date": int,
                                  "longstring": int,
                                  "array_int": int,
                                  "array_string": int,
                                  "array_date": int,
                                  "array_longstring": int,
                                  "avg_array_length": int,
                                  "keys": int
                              }
                          }
            manual_doc_sizes: Optional dict mapping collection names to manual document sizes in bytes.
                             If provided, these override document sizes from collection_size_file.
                             Format: {
                                 "CollectionName": int  # size in bytes
                             }
        """
        self.stats = Statistics()
        self.db_signature = db_signature
        self.collection_size_file = collection_size_file
        self.manual_counts = manual_counts
        self.manual_doc_sizes = manual_doc_sizes or {}
        self._load_db_info()
        self._load_schema()

    def _load_db_info(self):
        """Load collection info from specified collection size file"""
        results_path = Path(__file__).resolve().parent / self.collection_size_file
        
        if not results_path.exists():
            raise FileNotFoundError(f"Collection size file not found: {self.collection_size_file}")
        
        with open(results_path, 'r', encoding='utf-8') as f:
            all_results = json.load(f)
        
        if self.db_signature not in all_results:
            raise ValueError(f"DB signature {self.db_signature} not found in {self.collection_size_file}")
        
        self.db_info = all_results[self.db_signature]
        # Create a lookup for collection info
        self.collections = {}
        for coll in self.db_info["collections"]:
            coll_name = coll["collection"]
            # Create a copy to avoid modifying the original
            coll_copy = coll.copy()
            # Override document size if manual_doc_sizes provided
            if coll_name in self.manual_doc_sizes:
                coll_copy["doc_size_bytes"] = self.manual_doc_sizes[coll_name]
                coll_copy["avg_doc_bytes"] = self.manual_doc_sizes[coll_name]
            self.collections[coll_name] = coll_copy
    
    def _load_schema(self):
        """Load JSON schema for the database signature (same pattern as QueryParser)"""
        schema_path = Path(__file__).resolve().parent / 'JSON_schema' / f'json-schema-{self.db_signature}.json'
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
        
        self.schema = Schema(schema_data)
        
        # Build a lookup for field types by collection
        self._build_field_type_lookup()
        
        # Initialize Sizer for calculating embedded object sizes
        # Pass manual_counts if provided to override automatic counting
        self.sizer = Sizer(self.schema, self.stats, manual_counts=self.manual_counts)
    
    def _build_field_type_lookup(self):
        """Build a lookup dict: {collection_name: {field_name: type}} (same as QueryParser)"""
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
        for entity in result["nested_entities"]:
            collection_name = entity["name"]
            self.field_types[collection_name] = {}
            
            for attr in entity["attributes"]:
                field_name = attr["name"]
                field_type = self.schema._classify_attr_type(attr)
                self.field_types[collection_name][field_name] = field_type
    
    def infer_type(self, collection: str, field_name: str) -> str:
        """Infer field type from JSON schema (same as QueryParser.infer_type)"""
        if collection in self.field_types and field_name in self.field_types[collection]:
            return self.field_types[collection][field_name]
        return 'integer'
    
    def _calculate_object_size(self, collection: str, field_name: str) -> int:
        """
        Calculate the size of an embedded object field (like Product.price).
        Leverages Sizer's logic for consistency with TD1 calculations.
        
        Args:
            collection: Collection name
            field_name: Field name of the object
            
        Returns:
            Total size in bytes
        """
        type_sizes = Statistics.size_map()
        KEY_OVERHEAD = type_sizes["key"]
        
        # Find the field in the entity's attributes
        entity = self.sizer._get_entity(collection)
        if not entity:
            return Statistics.SIZE_INTEGER
        
        # Find the specific attribute
        target_attr = None
        for attr in entity.get("attributes", []):
            if attr.get("name") == field_name:
                target_attr = attr
                break
        
        if not target_attr:
            return Statistics.SIZE_INTEGER
        
        attr_type = self.schema._classify_attr_type(target_attr)
        
        # For reference types (embedded objects), calculate the nested entity size
        if attr_type == "reference":
            child_name = target_attr.get("reference_to", "")
            child_entity = self.sizer._get_entity(child_name)
            if child_entity:
                # Use Sizer's recursive calculation
                return self.sizer.estimate_document_size(child_entity)
        
        # Fallback for non-reference types
        return type_sizes.get(attr_type, Statistics.SIZE_INTEGER)
    
    def _resolve_collection(self, logical_collection: str) -> str:
        """
        Resolve logical collection name to physical collection name.
        Handles embedded collections by mapping to their parent collection.
        
        Args:
            logical_collection: The collection name from the query (e.g., "Stock")
            
        Returns:
            The physical collection name that exists in the DB (e.g., "Product" in DB2)
        """
        # Check if this collection is embedded in this DB signature
        mapping = self.COLLECTION_MAPPING.get(self.db_signature, {})
        physical_collection = mapping.get(logical_collection, logical_collection)
        
        return physical_collection

    # ============================================================
    # QUERY SIZE CALCULATION (from formulas_TD2.tex)
    # ============================================================

    def calculate_query_sizes(self, collection: str, filter_fields: List[Dict], project_fields: List[Dict]) -> tuple:
        """
        Calculate size_input and size_msg based on homework formula.
        
        Query Size Formula:
        1. Filter: number_of_ints*8 + number_of_strings*80 + number_of_dates*20
        2. Projection keys (output): (number_of_ints_out + number_of_strings_out + number_of_dates_out) * 8
        3. Key costs: 12 * number_of_keys
        4. Total = sum of all three
        
        Output Size Formula:
        1. Projection keys (output): number_of_ints_out*8 + number_of_strings_out*80 + number_of_dates_out*20
        2. Key costs: (number_of_ints_out + number_of_strings_out + number_of_dates_out) * 12
        3. Total = sum of both
        
        Args:
            collection: Collection name for field type lookup
            filter_fields: List of {"name": str, "type": str} for WHERE clause
            project_fields: List of {"name": str, "type": str} for SELECT clause
        
        Returns:
            (size_input, size_msg) in bytes
        """
        # Base values
        SIZE_INTEGER = 8
        SIZE_STRING = 80
        SIZE_DATE = 20
        SIZE_KEY = 12
        
        # Count filter fields by type
        filter_ints = 0
        filter_strings = 0
        filter_dates = 0
        
        for field in filter_fields:
            field_type = field.get("type", "integer").lower()
            if field_type in ["integer", "number", "boolean"]:
                filter_ints += 1
            elif field_type == "string":
                filter_strings += 1
            elif field_type == "date":
                filter_dates += 1
        
        # Count projection fields by type (using type from parsed field dict)
        # Types are now automatically inferred by query_parser from schema
        proj_ints = 0
        proj_strings = 0
        proj_dates = 0
        
        for field in project_fields:
            # Use type from parsed field dict (already inferred from schema by parser)
            field_type = field.get("type", "integer").lower()
            
            if field_type in ["integer", "number", "boolean", "reference"]:
                proj_ints += 1
            elif field_type == "string":
                proj_strings += 1
            elif field_type == "date":
                proj_dates += 1
        
        # Calculate number of keys
        # Total keys = filter fields + projection fields
        nb_keys = len(filter_fields) + len(project_fields)
        # ============================================================
        # QUERY SIZE (size_input)
        # ============================================================
        
        # 1. Filter: number_of_ints*8 + number_of_strings*80 + number_of_dates*20
        filter_size = filter_ints * SIZE_INTEGER + filter_strings * SIZE_STRING + filter_dates * SIZE_DATE
        
        # 2. Projection keys (output): (number_of_ints_out + number_of_strings_out + number_of_dates_out) * 8
        proj_keys_size = (proj_ints + proj_strings + proj_dates) * SIZE_INTEGER
        
        # 3. Key costs: 12 * number_of_keys
        key_costs = SIZE_KEY * nb_keys
        
        # Total query size
        size_input = filter_size + proj_keys_size + key_costs
        
        # ============================================================
        # OUTPUT SIZE (size_msg)
        # ============================================================
        
        # 1. Projection keys (output): number_of_ints_out*8 + number_of_strings_out*80 + number_of_dates_out*20
        proj_values_size = proj_ints * SIZE_INTEGER + proj_strings * SIZE_STRING + proj_dates * SIZE_DATE
        
        # 2. Key costs: (number_of_ints_out + number_of_strings_out + number_of_dates_out) * 12
        proj_key_costs = (proj_ints + proj_strings + proj_dates) * SIZE_KEY
        
        # Total output size
        size_msg = proj_values_size + proj_key_costs
        
        return (size_input, size_msg)

    # ============================================================
    # JOIN SIZE CALCULATION
    # ============================================================

    def calculate_join_sizes(
        self,
        collections: List[str],
        filter_fields: List[Dict],
        project_fields: List[Dict]
    ) -> Tuple[int, int]:
        """
        Calculate size_input and size_msg for JOIN queries.
        
        Args:
            collections: List of collection names involved in join
            filter_fields: List of filter conditions (with 'collection' field)
            project_fields: List of project fields (with 'collection' field)
            
        Returns:
            (size_input, size_msg) in bytes
        """
        KEY_OVERHEAD = Statistics.SIZE_KEY
        NESTING_OVERHEAD = Statistics.SIZE_KEY
        
        type_sizes = {
            "integer": Statistics.SIZE_INTEGER,
            "boolean": Statistics.SIZE_INTEGER,
            "number": Statistics.SIZE_NUMBER,
            "string": Statistics.SIZE_STRING,
            "date": Statistics.SIZE_DATE,
            "longstring": Statistics.SIZE_LONGSTRING,
        }
        
        # Calculate input size
        size_input = 0
        
        # Filter fields: key + value
        for field in filter_fields:
            field_type = field.get("type", "integer")
            value_size = type_sizes.get(field_type, Statistics.SIZE_INTEGER)
            size_input += KEY_OVERHEAD + value_size
        
        # Project fields: key + boolean (1 to include field)
        for field in project_fields:
            size_input += KEY_OVERHEAD + Statistics.SIZE_INTEGER
        
        # Nesting overhead (one level for collection wrapper)
        size_input += NESTING_OVERHEAD
        
        # Calculate output message size (per result document)
        size_msg = 0
        for field in project_fields:
            field_name = field.get("name")
            field_collection = field.get("collection")
            
            # Look up actual field type
            actual_type = self.infer_type(field_collection, field_name)
            
            # Handle embedded objects
            if actual_type in ["reference", "object"]:
                value_size = self._calculate_object_size(field_collection, field_name)
            else:
                value_size = type_sizes.get(actual_type, Statistics.SIZE_INTEGER)
            
            size_msg += KEY_OVERHEAD + value_size
        
        return (size_input, size_msg)

    # ============================================================
    # SELECTIVITY CALCULATION
    # ============================================================

    def calculate_selectivity(self, collection: str, filter_fields: List[Dict]) -> float:
        """
        Calculate selectivity (fraction of documents matching the filter)
        
        Examples:
        - Filter on IDP + IDW: 1 / (nb_products × nb_warehouses)
        - Filter on brand="Apple": nb_apple_products / nb_products
        - Filter on date: 1 / nb_days
        
        Args:
            collection: Logical collection name from query
            filter_fields: List of filter conditions
        
        Returns:
            selectivity (0 to 1)
        """
        # Resolve to physical collection for embedded cases
        physical_collection = self._resolve_collection(collection)
        
        # Simple heuristic based on filter field names
        filter_names = [f["name"].lower() for f in filter_fields]
        
        # Use logical collection name for selectivity logic (query semantics)
        # but adjust for physical collection when embedded
        if collection.lower() == "stock":
            # Check if Stock is embedded (DB2, DB5)
            if physical_collection.lower() == "product":
                # Stock embedded in Product - filtering on IDP gives 1 product
                if "idp" in filter_names and "idw" in filter_names:
                    # IDP+IDW: 1 product, then filter Stock array for IDW
                    # Selectivity is per Product document (1 out of nb_products)
                    return 1.0 / self.stats.nb_products
                elif "idp" in filter_names:
                    # Just IDP: 1 product with all its Stock entries
                    return 1.0 / self.stats.nb_products
                elif "idw" in filter_names:
                    # Just IDW: all products, but need to scan Stock arrays
                    # Approximately 1/nb_warehouses of products have stock in warehouse W
                    return 1.0 / self.stats.nb_warehouses
            else:
                # Stock is standalone collection
                if "idp" in filter_names and "idw" in filter_names:
                    # Very specific: one document out of all stock records
                    return 1.0 / (self.stats.nb_products * self.stats.nb_warehouses)
                elif "idp" in filter_names:
                    # One product across all warehouses
                    return 1.0 / self.stats.nb_products
                elif "idw" in filter_names:
                    # One warehouse across all products
                    return 1.0 / self.stats.nb_warehouses
        
        elif collection.lower() == "product":
            if "brand" in filter_names:
                # Assume Apple products as only example in our data
                return self.stats.nb_apple_products / self.stats.nb_products
            elif "idp" in filter_names:
                # Specific product
                return 1.0 / self.stats.nb_products
        
        elif collection.lower() == "orderline":
            if "date" in filter_names:
                # Orders on a specific date
                return 1.0 / self.stats.nb_days
            elif "idc" in filter_names:
                # Orders from one client
                return 1.0 / self.stats.nb_clients
            elif "idp" in filter_names:
                # Orders containing one product
                return 1.0 / self.stats.nb_products
        
        # Default: assume moderate selectivity
        return 0.01

    # ============================================================
    # S (SERVERS ACCESSED) CALCULATION
    # ============================================================

    def calculate_S(self, collection: str, filter_fields: List[Dict], sharding_key: Optional[str]) -> int:
        """
        Calculate S (number of servers accessed)
        
        Formula from formulas_TD2.tex:
        S = 1 if filtering on sharding key, else #shards
        
        Args:
            collection: Collection name
            filter_fields: List of filter conditions
            sharding_key: The sharding key for this collection (e.g., "IDP", "IDC")
        
        Returns:
            S (number of servers)
        """
        if sharding_key is None:
            # No sharding, all data on all servers
            return self.stats.nb_servers
        
        # Check if filtering on the sharding key
        filter_names = [f["name"].upper() for f in filter_fields]
        if sharding_key.upper() in filter_names:
            # Filtering on shard key -> access only 1 server
            return 1
        
        # Not filtering on shard key -> must query all shards
        return self.stats.nb_servers

    def _get_nb_srv_working(self, S: int) -> int:
        """
        Get nb_srv_working constant based on number of servers.
        
        Args:
            S: Number of servers (S)
            
        Returns:
            nb_srv_working: 1 if S=1, 50 if S=1000 (or S>1)
        """
        if S == 1:
            return 1
        else:
            # Always 50 when sharding > 1 (e.g., S=1000)
            return 50

    def _calculate_ram_vol_total(self, S: int, has_index: bool, size_doc: int, vol_RAM: float) -> float:
        """
        Calculate RAM Vol (total) based on number of servers.
        
        - Case 1 (S=1): ram_vol_total = ram_vol (simple case)
        - Case 2 (S=1000): nb_srv_working * ram_vol_per_server + (sharding - nb_srv_working) * index * 1.00E+06
        
        Args:
            S: Number of servers (sharding)
            has_index: Whether index exists
            size_doc: Collection document size in bytes (not used in formula, kept for compatibility)
            vol_RAM: RAM volume per server (ram_vol_per_server)
            
        Returns:
            RAM Vol (total)
        """
        if S == 1:
            # Case 1: single server (S=1) - ram_vol_total = ram_vol
            return vol_RAM
        else:
            # Case 2: full cluster (S=1000 or S>1)
            # Formula: nb_srv_working * ram_vol_per_server + (sharding - nb_srv_working) * index * 1.00E+06
            nb_srv_working = self._get_nb_srv_working(S)  # Always 50 when S > 1
            index = 1 if has_index else 0
            multiplier = 1_000_000  # 1.00E+06
            return nb_srv_working * vol_RAM + (S - nb_srv_working) * index * multiplier

    # ============================================================
    # OPERATOR 1: FILTER WITH SHARDING
    # ============================================================
    
    def filter_with_sharding(
        self,
        collection: str,
        project_fields: List[Dict],
        filter_fields: List[Dict],
        sharding_key: str,
        selectivity: Optional[float] = None,
        has_index: bool = False,
        index_size: Optional[int] = None
    ) -> Dict:
        """
        Filter operator with sharding.
        
        Args:
            collection: Collection name
            project_fields: List of fields to project (output format)
            filter_fields: List of filter conditions
            sharding_key: Sharding key (e.g., "IDP", "IDC")
            selectivity: Optional selectivity (default: computed)
            has_index: Whether index exists
            index_size: Index size in bytes (default: 1MB if has_index=True)
            
        Returns:
            Dict with nb_docs, total_size_bytes, and costs
        """
        # index_size is always 1.00E+06, regardless of parameter
        index_size = 1_000_000
        
        # Calculate selectivity if not provided
        if selectivity is None:
            selectivity = self.calculate_selectivity(collection, filter_fields)
        
        # Calculate S (servers accessed) - with sharding
        S = self.calculate_S(collection, filter_fields, sharding_key)
        
        # Get collection info
        physical_collection = self._resolve_collection(collection)
        if physical_collection not in self.collections:
            raise ValueError(f"Collection '{collection}' not found")
        
        coll_info = self.collections[physical_collection]
        nb_docs_total = coll_info["num_docs"]
        size_doc = coll_info["doc_size_bytes"]
        
        # Calculate result count
        res_q = int(selectivity * nb_docs_total)
        
        # Calculate sizes
        size_input, size_msg = self.calculate_query_sizes(collection, filter_fields, project_fields)
        
        # Calculate volumes
        # RAM volume formula: index * index_size + nb_pointers_per_working_server * collection_doc_size
        index = 1 if has_index else 0
        index_size = 1_000_000  # Always 1.00E+06
        nb_pointers_per_working_server = res_q / S if S > 0 else res_q
        vol_RAM = index * index_size + nb_pointers_per_working_server * size_doc
        
        vol_network = S * size_input + res_q * size_msg
        
        # Calculate costs
        # Time: network_vol/bandwidth_network + ram_vol/bandwidth_ram (using ram_vol, NOT ram_vol_total)
        time_cost = vol_network / Statistics.BANDWIDTH_NETWORK + vol_RAM / Statistics.BANDWIDTH_RAM
        carbon_network = vol_network * Statistics.CO2_NETWORK
        carbon_RAM = vol_RAM * Statistics.CO2_RAM
        carbon_total = carbon_network + carbon_RAM
        
        # Total output size
        total_size_bytes = res_q * size_msg
        
        return {
            "operator": "filter_with_sharding",
            "collection": collection,
            "nb_docs": res_q,
            "total_size_bytes": total_size_bytes,
            "selectivity": selectivity,
            "S_servers": S,
            "costs": {
                "vol_network": vol_network,
                "vol_RAM": vol_RAM,
                "time": time_cost,
                "carbon_network": carbon_network,
                "carbon_RAM": carbon_RAM,
                "carbon_total": carbon_total
            }
        }
    
    # ============================================================
    # OPERATOR 2: FILTER WITHOUT SHARDING
    # ============================================================
    
    def filter_without_sharding(
        self,
        collection: str,
        project_fields: List[Dict],
        filter_fields: List[Dict],
        selectivity: Optional[float] = None,
        has_index: bool = False,
        index_size: Optional[int] = None
    ) -> Dict:
        """
        Filter operator without sharding (broadcast to all servers).
        
        Args:
            collection: Collection name
            project_fields: List of fields to project
            filter_fields: List of filter conditions
            selectivity: Optional selectivity (default: computed)
            has_index: Whether index exists
            index_size: Index size in bytes
            
        Returns:
            Dict with nb_docs, total_size_bytes, and costs
        """
        result = self.filter_with_sharding(
            collection=collection,
            project_fields=project_fields,
            filter_fields=filter_fields,
            sharding_key=None,  # No sharding
            selectivity=selectivity,
            has_index=has_index,
            index_size=index_size
        )
        result["operator"] = "filter_without_sharding"
        return result
    
    # ============================================================
    # OPERATOR 3: NESTED LOOP JOIN WITH SHARDING
    # ============================================================
    
    def nested_loop_join_with_sharding(
        self,
        collection1: str,
        collection2: str,
        join_conditions: List[Dict],
        project_fields: List[Dict],
        filter_fields: List[Dict],
        sharding_key: Optional[str],
        selectivity1: Optional[float] = None,
        selectivity2: Optional[float] = None,
        join_selectivity: Optional[float] = None,
        has_index: bool = False,
        index_size: Optional[int] = None
    ) -> Dict:
        """
        Nested loop join operator with sharding.
        
        Algorithm:
        - Outer loop: collection1 (with filters and sharding)
        - Inner loop: collection2 (for each outer match)
        
        Args:
            collection1: Outer collection (e.g., "Stock")
            collection2: Inner collection (e.g., "Product")
            join_conditions: List of join conditions [{"left_collection": "Stock", "left_field": "IDP", ...}]
            project_fields: List of fields to project (from both collections)
            filter_fields: List of filter conditions (from both collections)
            sharding_key: Sharding key for collection1
            selectivity1: Optional selectivity for collection1 filters
            selectivity2: Optional selectivity for collection2 filters
            join_selectivity: Optional join selectivity (default: 1/distinct_join_keys)
            has_index: Whether index exists
            index_size: Index size in bytes
            
        Returns:
            Dict with nb_docs, total_size_bytes, and costs
        """
        # index_size is always 1.00E+06, regardless of parameter
        index_size = 1_000_000
        
        # Resolve collections
        physical_col1 = self._resolve_collection(collection1)
        physical_col2 = self._resolve_collection(collection2)
        
        if physical_col1 not in self.collections:
            raise ValueError(f"Collection '{collection1}' not found")
        if physical_col2 not in self.collections:
            raise ValueError(f"Collection '{collection2}' not found")
        
        coll1_info = self.collections[physical_col1]
        coll2_info = self.collections[physical_col2]
        
        nb_docs1 = coll1_info["num_docs"]
        nb_docs2 = coll2_info["num_docs"]
        size_doc1 = coll1_info["doc_size_bytes"]
        size_doc2 = coll2_info["doc_size_bytes"]
        
        # Split filter fields by collection
        filter_fields1 = [f for f in filter_fields if f.get("collection") == collection1]
        filter_fields2 = [f for f in filter_fields if f.get("collection") == collection2]
        
        # Calculate selectivities
        if selectivity1 is None:
            selectivity1 = self.calculate_selectivity(collection1, filter_fields1)
        if selectivity2 is None:
            selectivity2 = self.calculate_selectivity(collection2, filter_fields2)
        
        # Calculate join selectivity (fraction of outer matches that join with inner)
        if join_selectivity is None:
            # Default: assume join key has same distribution as outer collection
            # For equi-join on IDP: if Stock has IDP, Product has IDP, join_selectivity ≈ 1
            # More sophisticated: could use 1/distinct_join_keys
            join_selectivity = 1.0  # Conservative estimate
        
        # Calculate S (servers for outer collection)
        S1 = self.calculate_S(collection1, filter_fields1, sharding_key)
        
        # For nested loop: inner collection is accessed for each outer match
        # If inner is sharded, we may need to access all shards (unless join key is shard key)
        # Simplified: assume inner collection access pattern
        S2 = self.stats.nb_servers  # Inner collection may need all servers
        
        # Calculate result count
        res_outer = int(selectivity1 * nb_docs1)
        res_join = int(res_outer * selectivity2 * nb_docs2 * join_selectivity)
        
        # Calculate sizes
        # Input: filters from both collections
        all_filter_fields = filter_fields1 + filter_fields2
        size_input, size_msg = self.calculate_join_sizes(
            [collection1, collection2],
            all_filter_fields,
            project_fields
        )
        
        # Calculate volumes for nested loop join
        # RAM volume formula: index * index_size + nb_pointers_per_working_server * collection_doc_size
        index = 1 if has_index else 0
        index_size = 1_000_000  # Always 1.00E+06
        
        # Outer collection: nb_pointers_per_working_server = res_outer / S1
        nb_pointers_outer_per_server = res_outer / S1 if S1 > 0 else res_outer
        vol_RAM_outer = index * index_size + nb_pointers_outer_per_server * size_doc1
        
        # Inner collection: for each outer match, we get inner results
        # Calculate inner results per outer match
        res_inner_per_outer = int(selectivity2 * nb_docs2 * join_selectivity) if res_outer > 0 else 0
        # Total inner results across all servers
        total_inner_results = res_outer * res_inner_per_outer
        # Inner results per server (distributed across S2 servers)
        nb_pointers_inner_per_server = total_inner_results / S2 if S2 > 0 else total_inner_results
        vol_RAM_inner = index * index_size + nb_pointers_inner_per_server * size_doc2
        
        vol_RAM = vol_RAM_outer + vol_RAM_inner
        
        # Network volume: query outer + query inner + results
        # Outer query
        vol_network_outer = S1 * size_input
        # Inner queries (one per outer match, but can be batched)
        # Simplified: assume we send inner query for each outer match
        vol_network_inner = res_outer * S2 * size_input
        # Results
        vol_network_results = res_join * size_msg
        
        vol_network = vol_network_outer + vol_network_inner + vol_network_results
        
        # Calculate costs
        # Time: network_vol/bandwidth_network + ram_vol/bandwidth_ram (using ram_vol, NOT ram_vol_total)
        time_cost = vol_network / Statistics.BANDWIDTH_NETWORK + vol_RAM / Statistics.BANDWIDTH_RAM
        carbon_network = vol_network * Statistics.CO2_NETWORK
        carbon_RAM = vol_RAM * Statistics.CO2_RAM
        carbon_total = carbon_network + carbon_RAM
        
        # Total output size
        total_size_bytes = res_join * size_msg
        
        return {
            "operator": "nested_loop_join_with_sharding",
            "collections": [collection1, collection2],
            "nb_docs": res_join,
            "total_size_bytes": total_size_bytes,
            "selectivity1": selectivity1,
            "selectivity2": selectivity2,
            "join_selectivity": join_selectivity,
            "S_servers_outer": S1,
            "S_servers_inner": S2,
            "costs": {
                "vol_network": vol_network,
                "vol_RAM": vol_RAM,
                "time": time_cost,
                "carbon_network": carbon_network,
                "carbon_RAM": carbon_RAM,
                "carbon_total": carbon_total
            }
        }
    
    # ============================================================
    # OPERATOR 4: NESTED LOOP JOIN WITHOUT SHARDING
    # ============================================================
    
    def nested_loop_join_without_sharding(
        self,
        collection1: str,
        collection2: str,
        join_conditions: List[Dict],
        project_fields: List[Dict],
        filter_fields: List[Dict],
        selectivity1: Optional[float] = None,
        selectivity2: Optional[float] = None,
        join_selectivity: Optional[float] = None,
        has_index: bool = False,
        index_size: Optional[int] = None
    ) -> Dict:
        """
        Nested loop join operator without sharding (broadcast).
        
        Args:
            Same as nested_loop_join_with_sharding, except no sharding_key
            
        Returns:
            Dict with nb_docs, total_size_bytes, and costs
        """
        result = self.nested_loop_join_with_sharding(
            collection1=collection1,
            collection2=collection2,
            join_conditions=join_conditions,
            project_fields=project_fields,
            filter_fields=filter_fields,
            sharding_key=None,  # No sharding
            selectivity1=selectivity1,
            selectivity2=selectivity2,
            join_selectivity=join_selectivity,
            has_index=has_index,
            index_size=index_size
        )
        result["operator"] = "nested_loop_join_without_sharding"
        return result

    # ============================================================
    # MAIN COST CALCULATION (UPDATED FOR JOINS)
    # ============================================================

    def calculate_query_cost(self, query: Dict) -> Dict:
        """
        Calculate complete query cost - supports filter, JOIN, and aggregate queries.
        
        Args:
            query: Query specification dict with:
                - For filter queries:
                    - query_type: 'filter' (optional, default)
                    - collection: str
                    - filter_fields: List[Dict]
                    - project_fields: List[Dict]
                    - sharding_key: Optional[str]
                    - has_index: bool
                - For join queries:
                    - query_type: 'join' or 'join_aggregate'
                    - collections: List[str]
                    - join_conditions: List[Dict]
                    - filter_fields: List[Dict] (with 'collection' field)
                    - project_fields: List[Dict] (with 'collection' field)
                    - sharding_key: Optional[str]
                    - has_index: bool
                    - aggregate_functions: List[Dict] (for join_aggregate)
                    - group_by_fields: List[Dict] (for join_aggregate)
                - For aggregate queries:
                    - query_type: 'aggregate'
                    - collection: str
                    - filter_fields: List[Dict]
                    - project_fields: List[Dict]
                    - aggregate_functions: List[Dict]
                    - group_by_fields: List[Dict]
                    - sharding_key: Optional[str]
                    - has_index: bool
        
        Returns:
            Dict with all cost metrics
        """
        query_type = query.get("query_type", "filter")
        
        if query_type == "join" or query_type == "join_aggregate":
            return self._calculate_join_cost(query)
        elif query_type == "aggregate":
            return self._calculate_aggregate_cost(query)
        else:
            return self._calculate_filter_cost(query)
    
    def _calculate_filter_cost(self, query: Dict) -> Dict:
        """Calculate cost for filter query (existing implementation)."""
        collection = query["collection"]
        filter_fields = query["filter_fields"]
        project_fields = query["project_fields"]
        sharding_key = query.get("sharding_key")
        has_index = query.get("has_index", False)
        # index_size is always 1.00E+06
        index_size = 1_000_000
        
        # Resolve logical collection to physical collection (handles embedding)
        physical_collection = self._resolve_collection(collection)
        
        # Get collection info from physical collection
        if physical_collection not in self.collections:
            available = ", ".join(self.collections.keys())
            raise ValueError(
                f"Collection '{collection}' (resolves to '{physical_collection}') not found in {self.db_signature}. "
                f"Available collections: {available}"
            )
        
        coll_info = self.collections[physical_collection]
        nb_docs = coll_info["num_docs"]
        size_doc = coll_info["doc_size_bytes"]
        
        # Calculate query sizes
        size_input, size_msg = self.calculate_query_sizes(collection, filter_fields, project_fields)
        
        # Calculate selectivity
        sel_att = self.calculate_selectivity(collection, filter_fields)
        
        # Calculate S (servers accessed)
        S = self.calculate_S(collection, filter_fields, sharding_key)
        
        # Calculate res_q (number of results)
        res_q = int(sel_att * nb_docs)
        
        # Calculate collection size per server
        coll_per_server = nb_docs / self.stats.nb_servers
        
        # Calculate RAM volume (per server)
        # Formula: index * index_size + nb_pointers_per_working_server * collection_doc_size
        index = 1 if has_index else 0
        index_size = 1_000_000  # Always 1.00E+06
        nb_pointers_per_working_server = res_q / S if S > 0 else res_q
        vol_RAM = index * index_size + nb_pointers_per_working_server * size_doc

        # Calculate RAM volume total (different calculation based on S)
        vol_ram_total = self._calculate_ram_vol_total(S, has_index, size_doc, vol_RAM)

        # Calculate network volume (filter query formula)
        # vol_network = S × size_input + res_q × size_msg
        vol_network = S * size_input + res_q * size_msg

        # Calculate time cost: network_vol/bandwidth_network + ram_vol/bandwidth_ram
        time_cost = (
            vol_network / Statistics.BANDWIDTH_NETWORK +
            vol_RAM / Statistics.BANDWIDTH_RAM
        )

        # Calculate carbon impact: network_vol*kgCO2eq_network + ram_vol_total*kgCO2eq_ram
        carbon_network = vol_network * Statistics.CO2_NETWORK
        carbon_RAM = vol_ram_total * Statistics.CO2_RAM
        carbon_total = carbon_network + carbon_RAM

        # Calculate price: network_vol * network_price
        budget = vol_network * Statistics.NETWORK_PRICE
        
        # Format numbers in scientific notation for readability
        def format_scientific(value: float) -> str:
            """Format number in scientific notation (e.g., 1.45e-9)"""
            return f"{value:.2e}"
        
        return {
            "query": {
                "collection": collection,
                "db_signature": self.db_signature,
                "filter_fields": filter_fields,
                "project_fields": project_fields,
                "sharding_key": sharding_key,
                "has_index": has_index,
            },
            "sizes": {
                "size_input_bytes": f"{size_input} B",
                "size_msg_bytes": f"{size_msg} B",
                "size_doc_bytes": f"{size_doc} B",
            },
            "distribution": {
                "S_servers": S,
                "selectivity": format_scientific(sel_att),
                "res_q_results": res_q,
                "nb_docs_total": nb_docs,
                "nb_docs_per_server": coll_per_server,
            },
            "volumes": {
                "vol_network": f"{format_scientific(vol_network)} B",
                "vol_RAM": f"{format_scientific(vol_RAM)} B",
                "vol_ram_total": f"{format_scientific(vol_ram_total)} B",
            },
            "costs": {
                "time": f"{format_scientific(time_cost)} s",
                "carbon_network": f"{format_scientific(carbon_network)} kgCO2eq",
                "carbon_RAM": f"{format_scientific(carbon_RAM)} kgCO2eq",
                "carbon_total": f"{format_scientific(carbon_total)} kgCO2eq",
                "budget": f"{format_scientific(budget)} €",
            }
        }
    
    def _calculate_join_cost(self, query: Dict) -> Dict:
        """
        Calculate cost for JOIN query as two sub-queries (when collections are separate)
        or as a single filter (when embedded in same physical collection).

        Subquery model (separate collections):
            Op1: filter on driving collection (e.g. Stock WHERE S.IDW = $IDW)
            Op2: for each Op1 result, point lookup on other collection (e.g. Product by IDP)

        Combined metrics follow:
            metric_total = metric_op1 + nb_iter_op2 * metric_op2_single
            with nb_iter_op2 = nb_output_op1
        """
        collections = query["collections"]
        join_conditions = query["join_conditions"]
        filter_fields = query.get("filter_fields", [])
        project_fields = query.get("project_fields", [])
        sharding_key = query.get("sharding_key")
        has_index = query.get("has_index", False)

        if len(collections) != 2:
            raise ValueError("Currently only supports 2-way joins")

        collection1, collection2 = collections

        # Resolve to physical collections to detect embedding
        phys1 = self._resolve_collection(collection1)
        phys2 = self._resolve_collection(collection2)

        # ---------- Embedded case: both logical collections map to same physical ----------
        if phys1 == phys2:
            # For embedded joins, we need to calculate combined query_size and output_size
            # by adding Op1 and Op2 sizes together, then use as a filter query
            
            # Split filter/projection fields by collection (like in separate case)
            ff1 = [f for f in filter_fields if f.get("collection") == collection1]
            ff2 = [f for f in filter_fields if f.get("collection") == collection2]
            proj1 = [f for f in project_fields if f.get("collection") == collection1]
            proj2 = [f for f in project_fields if f.get("collection") == collection2]
            
            # Determine join key on each side (use first join condition)
            jc = join_conditions[0]
            if jc["left_collection"] == collection1:
                join_field1 = jc["left_field"]
                join_field2 = jc["right_field"]
            else:
                join_field1 = jc["right_field"]
                join_field2 = jc["left_field"]
            
            # Op1: filter on collection1 (the embedded one), project join key + other fields
            # Ensure join key is projected in Op1
            proj1_with_join = proj1
            if not any(f["name"] == join_field1 and f.get("collection") == collection1 for f in proj1):
                proj1_with_join = proj1 + [{
                    "collection": collection1,
                    "name": join_field1,
                    "type": self.infer_type(collection1, join_field1),
                }]
            
            # Op2: filter on collection2 (the parent), project other fields
            # The filter for Op2 would be join_field2 = join_field1 (but we don't add it here)
            # as we're combining the sizes
            
            # Calculate sizes for Op1 and Op2 separately
            size_input1, size_msg1 = self.calculate_query_sizes(collection1, ff1, proj1_with_join)
            size_input2, size_msg2 = self.calculate_query_sizes(collection2, [], proj2)  # Op2 has no filter (it's a join key lookup)
            
            # Add a filter field for Op2 (the join key lookup) - it's 1 integer input
            # We need to account for this in size_input2
            # For Op2, we have: 1 integer input (the join key) + projection outputs
            # So size_input2 should include the join key as a filter
            join_filter_op2 = [{
                "collection": collection2,
                "name": join_field2,
                "type": self.infer_type(collection2, join_field2),
            }]
            size_input2_with_join, _ = self.calculate_query_sizes(collection2, join_filter_op2, proj2)
            
            # Combined sizes: add Op1 and Op2 sizes
            combined_size_input = size_input1 + size_input2_with_join
            combined_size_msg = size_msg1 + size_msg2
            
            # Now use these combined sizes in a filter query on the parent collection
            # The parent collection is the physical collection (phys1 == phys2)
            parent_collection = phys1  # Use physical collection
            
            # Use collection1's filters (since that's what we filter on)
            combined_filters = ff1  # Only use collection1 filters
            combined_projects = project_fields  # All projections
            
            # Create a custom query dict with the combined sizes
            # We'll need to calculate costs manually using the combined sizes
            coll_info = self.collections[parent_collection]
            nb_docs = coll_info["num_docs"]
            size_doc = coll_info["doc_size_bytes"]
            
            # Calculate selectivity (based on collection1 filters)
            sel_att = self.calculate_selectivity(collection1, ff1)
            
            # Calculate S (servers accessed)
            S = self.calculate_S(collection1, ff1, sharding_key)
            
            # Calculate res_q (number of results)
            res_q = int(sel_att * nb_docs)
            
            # Calculate collection size per server
            coll_per_server = nb_docs / self.stats.nb_servers
            
            # Calculate RAM volume (per server)
            index = 1 if has_index else 0
            index_size = 1_000_000
            nb_pointers_per_working_server = res_q / S if S > 0 else res_q
            vol_RAM = index * index_size + nb_pointers_per_working_server * size_doc
            
            # Calculate RAM volume total
            vol_ram_total = self._calculate_ram_vol_total(S, has_index, size_doc, vol_RAM)
            
            # Calculate network volume using combined sizes
            vol_network = S * combined_size_input + res_q * combined_size_msg
            
            # Calculate time cost
            time_cost = vol_network / Statistics.BANDWIDTH_NETWORK + vol_RAM / Statistics.BANDWIDTH_RAM
            
            # Calculate carbon impact
            carbon_network = vol_network * Statistics.CO2_NETWORK
            carbon_RAM = vol_ram_total * Statistics.CO2_RAM
            carbon_total = carbon_network + carbon_RAM
            
            # Calculate price
            budget = vol_network * Statistics.NETWORK_PRICE
            
            # Format numbers
            def format_scientific(value: float) -> str:
                return f"{value:.2e}"
            
            # Get document counts
            nb_docs1 = self.stats.get_collection_count(collection1)
            nb_docs2 = self.stats.get_collection_count(collection2)
            
            # Build result structure
            single_result = {
                "query": {
                    "query_type": query.get("query_type", "join"),
                    "collection": collection1,
                    "collections": collections,
                    "db_signature": self.db_signature,
                    "filter_fields": combined_filters,
                    "project_fields": combined_projects,
                    "sharding_key": sharding_key,
                    "has_index": has_index,
                },
                "sizes": {
                    "size_input_bytes": f"{combined_size_input} B",
                    "size_msg_bytes": f"{combined_size_msg} B",
                    "size_doc_bytes": f"{size_doc} B",
                },
                "distribution": {
                    "S_servers": S,
                    "selectivity": format_scientific(sel_att),
                    "res_q_results": res_q,
                    "nb_docs_total": nb_docs,
                    "nb_docs_per_server": coll_per_server,
                    "nb_docs1": nb_docs1,
                    "nb_docs2": nb_docs2,
                },
                "volumes": {
                    "vol_network": f"{format_scientific(vol_network)} B",
                    "vol_RAM": f"{format_scientific(vol_RAM)} B",
                    "vol_ram_total": f"{format_scientific(vol_ram_total)} B",
                },
                "costs": {
                    "time": f"{format_scientific(time_cost)} s",
                    "carbon_network": f"{format_scientific(carbon_network)} kgCO2eq",
                    "carbon_RAM": f"{format_scientific(carbon_RAM)} kgCO2eq",
                    "carbon_total": f"{format_scientific(carbon_total)} kgCO2eq",
                    "budget": f"{format_scientific(budget)} €",
                },
                "op1": None,  # No separate op1 for embedded case
                "op2": None,  # No separate op2 for embedded case
                "nb_iter_op2": 0,
            }
            
            return single_result

        # ---------- Separate collections: true two-phase join ----------
        # Split filter/projection fields by collection
        ff1 = [f for f in filter_fields if f.get("collection") == collection1]
        ff2 = [f for f in filter_fields if f.get("collection") == collection2]
        proj1 = [f for f in project_fields if f.get("collection") == collection1]
        proj2 = [f for f in project_fields if f.get("collection") == collection2]

        # Determine join key on each side (use first join condition)
        jc = join_conditions[0]
        if jc["left_collection"] == collection1:
            join_field1 = jc["left_field"]
            join_field2 = jc["right_field"]
        else:
            join_field1 = jc["right_field"]
            join_field2 = jc["left_field"]

        # Ensure join key from collection1 is projected in Op1
        if not any(f["name"] == join_field1 and f.get("collection") == collection1 for f in proj1):
            proj1_for_op1 = proj1 + [{
                "collection": collection1,
                "name": join_field1,
                "type": self.infer_type(collection1, join_field1),
            }]
        else:
            proj1_for_op1 = proj1

        # ---------- Op1: filter on collection1 ----------
        op1_query = {
            "collection": collection1,
            "filter_fields": ff1,
            "project_fields": proj1_for_op1,
            "sharding_key": sharding_key,
            "has_index": has_index,
        }
        op1_result = self._calculate_filter_cost(op1_query)

        # Extract Op1 metrics
        size_input1 = int(op1_result["sizes"]["size_input_bytes"].split()[0])
        size_msg1 = int(op1_result["sizes"]["size_msg_bytes"].split()[0])
        S1 = op1_result["distribution"]["S_servers"]
        vol_net1 = float(op1_result["volumes"]["vol_network"].split()[0])
        vol_ram1 = float(op1_result["volumes"]["vol_RAM"].split()[0])
        vol_ram_total1 = float(op1_result["volumes"]["vol_ram_total"].split()[0])
        time1 = float(op1_result["costs"]["time"].split()[0])

        # Calculate nb_output1 based on filter field: nb_docs_X * (1/nb_docs_Y)
        # where X is collection1 and Y is the collection referenced by the filter field
        nb_docs1 = self.stats.get_collection_count(collection1)
        nb_docs2 = self.stats.get_collection_count(collection2)  # Define for later use
        
        # Map filter field names to their referenced collections
        # IDW -> Warehouse, IDP -> Product, IDC -> Client, etc.
        filter_field_to_collection = {
            "IDW": "Warehouse",
            "idw": "Warehouse",
            "IDP": "Product",
            "idp": "Product",
            "IDC": "Client",
            "idc": "Client",
            "IDS": "Supplier",
            "ids": "Supplier",
        }
        
        # Find the filter field that references another collection
        referenced_collection_name = None
        if ff1:
            for filter_field in ff1:
                field_name = filter_field.get("name", "")
                if field_name in filter_field_to_collection:
                    referenced_collection_name = filter_field_to_collection[field_name]
                    break
        
        # Calculate nb_output1 using the formula: nb_docs_X * (1/nb_docs_Y)
        if referenced_collection_name and nb_docs1 > 0:
            nb_docs_referenced = self.stats.get_collection_count(referenced_collection_name)
            if nb_docs_referenced > 0:
                nb_output1 = int(nb_docs1 * (1.0 / nb_docs_referenced))
            else:
                nb_output1 = op1_result["distribution"]["res_q_results"]
        else:
            # Fallback to original calculation if we can't determine referenced collection
            nb_output1 = op1_result["distribution"]["res_q_results"]
        
        # Update op1_result with the corrected nb_output1
        op1_result["distribution"]["res_q_results"] = nb_output1
        # Recalculate op1 volumes and costs with corrected nb_output1
        nb_output1_float = float(nb_output1)
        vol_net1 = S1 * size_input1 + nb_output1_float * size_msg1
        op1_result["volumes"]["vol_network"] = f"{vol_net1:.2e} B"
        # Recalculate vol_ram_total1 (vol_RAM doesn't change, but vol_ram_total might)
        # We need to recalculate it using the helper function
        physical_collection1 = self._resolve_collection(collection1)
        size_doc1 = self.collections[physical_collection1]["doc_size_bytes"]
        vol_ram_total1 = self._calculate_ram_vol_total(S1, has_index, size_doc1, vol_ram1)
        op1_result["volumes"]["vol_ram_total"] = f"{vol_ram_total1:.2e} B"
        # Recalculate time using formula: network_vol/bandwidth_network + ram_vol/bandwidth_ram
        time1 = vol_net1 / Statistics.BANDWIDTH_NETWORK + vol_ram1 / Statistics.BANDWIDTH_RAM
        op1_result["costs"]["time"] = f"{time1:.2e} s"
        # Recalculate CO2 using new formula: network_vol*kgCO2eq_network + ram_vol_total*kgCO2eq_ram
        carbon_net1 = vol_net1 * Statistics.CO2_NETWORK
        carbon_ram1 = vol_ram_total1 * Statistics.CO2_RAM
        op1_result["costs"]["carbon_total"] = f"{(carbon_net1 + carbon_ram1):.2e} kgCO2eq"

        nb_iter2 = nb_output1  # number of executions of Op2 (must equal nb_output1)

        # ---------- Op2: point lookup on collection2 ----------
        # Ensure join key is present in filter fields for Op2
        ff2_for_op2 = ff2 + [{
            "collection": collection2,
            "name": join_field2,
            "type": self.infer_type(collection2, join_field2),
        }]

        op2_query = {
            "collection": collection2,
            "filter_fields": ff2_for_op2,
            "project_fields": proj2,
            # Force S=1 semantics for Op2: routing by join key
            "sharding_key": join_field2,
            "has_index": has_index,
        }
        op2_result = self._calculate_filter_cost(op2_query)

        size_input2 = int(op2_result["sizes"]["size_input_bytes"].split()[0])
        size_msg2 = int(op2_result["sizes"]["size_msg_bytes"].split()[0])
        # We conceptually treat Op2 as S=1 point lookups (even if calculate_S said otherwise)
        S2_effective = 1
        # Override S2 to 1 in op2_result for consistency
        op2_result["distribution"]["S_servers"] = S2_effective
        vol_net2_single = float(op2_result["volumes"]["vol_network"].split()[0])
        vol_ram2_single = float(op2_result["volumes"]["vol_RAM"].split()[0])
        vol_ram_total2_single = float(op2_result["volumes"]["vol_ram_total"].split()[0])
        time2_single = float(op2_result["costs"]["time"].split()[0])
        
        # Extract CO2 and budget for Op2 (per iteration) using new formulas
        carbon_net2_single = vol_net2_single * Statistics.CO2_NETWORK
        carbon_ram2_single = vol_ram_total2_single * Statistics.CO2_RAM  # Use vol_ram_total
        carbon_total2_single = carbon_net2_single + carbon_ram2_single
        budget2_single = vol_net2_single * Statistics.NETWORK_PRICE

        # ---------- Combine metrics ----------
        vol_network_total = vol_net1 + nb_iter2 * vol_net2_single
        vol_ram_total_combined = vol_ram_total1 + nb_iter2 * vol_ram_total2_single  # Use vol_ram_total for each
        # Time: network_vol/bandwidth_network + ram_vol/bandwidth_ram (using per-server RAM vol for time)
        time_total = (vol_network_total / Statistics.BANDWIDTH_NETWORK + 
                     (vol_ram1 + nb_iter2 * vol_ram2_single) / Statistics.BANDWIDTH_RAM)

        # CO2: network_vol*kgCO2eq_network + ram_vol_total*kgCO2eq_ram
        carbon_network = vol_network_total * Statistics.CO2_NETWORK
        carbon_RAM = vol_ram_total_combined * Statistics.CO2_RAM
        carbon_total = carbon_network + carbon_RAM
        
        # Price: network_vol * network_price
        budget_total = vol_network_total * Statistics.NETWORK_PRICE

        # Representative S for the global join: only Op1 is broadcast
        S_effective = S1

        # Final number of results equals number of distinct join keys from Op1
        res_q_final = nb_iter2

        # Representative sizes for UI: Op1 input, Op2 output, doc size from collection1
        physical_collection1 = self._resolve_collection(collection1)
        size_doc1 = self.collections[physical_collection1]["doc_size_bytes"]

        def format_scientific(value: float) -> str:
            return f"{value:.2e}"

        return {
            "query": {
                "query_type": query.get("query_type", "join"),
                "collections": collections,
                "join_conditions": join_conditions,
                "filter_fields": filter_fields,
                "project_fields": project_fields,
                "sharding_key": sharding_key,
                "has_index": has_index,
                "db_signature": self.db_signature,
            },
            "sizes": {
                "size_input_bytes": f"{size_input1} B",
                "size_msg_bytes": f"{size_msg2} B",
                "size_doc_bytes": f"{size_doc1} B",
            },
            "distribution": {
                "S_servers": S_effective,
                "res_q_results": res_q_final,
                "nb_docs_total": nb_docs1 if nb_docs1 else op1_result["distribution"]["nb_docs_total"],
                "nb_docs_per_server": op1_result["distribution"]["nb_docs_per_server"],
                "nb_docs1": nb_docs1,
                "nb_docs2": nb_docs2,
                "nb_iter_op2": nb_iter2,
            },
            "volumes": {
                "vol_network": f"{format_scientific(vol_network_total)} B",
                "vol_RAM": f"{format_scientific(vol_ram1 + nb_iter2 * vol_ram2_single)} B",
                "vol_ram_total": f"{format_scientific(vol_ram_total_combined)} B",
            },
            "costs": {
                "time": f"{format_scientific(time_total)} s",
                "carbon_network": f"{format_scientific(carbon_network)} kgCO2eq",
                "carbon_RAM": f"{format_scientific(carbon_RAM)} kgCO2eq",
                "carbon_total": f"{format_scientific(carbon_total)} kgCO2eq",
                "budget": f"{format_scientific(budget_total)} €",
            },
            "output": {
                "nb_docs": res_q_final,
                "total_size_bytes": res_q_final * size_msg2,
            },
            # Attach detailed subquery results so the UI can display them
            "op1": op1_result,
            "op2": op2_result,
            "nb_iter_op2": nb_iter2,
            "S_op2_effective": S2_effective,
            # Store Op2 per-iteration costs for UI display
            "op2_carbon_total": carbon_total2_single,
            "op2_budget": budget2_single,
        }
    
    def _calculate_aggregate_cost(self, query: Dict) -> Dict:
        """Calculate cost for aggregate query with GROUP BY."""
        collection = query["collection"]
        filter_fields = query.get("filter_fields", [])
        project_fields = query.get("project_fields", [])
        aggregate_functions = query.get("aggregate_functions", [])
        group_by_fields = query.get("group_by_fields", [])
        sharding_key = query.get("sharding_key")
        has_index = query.get("has_index", False)
        index_size = 1_000_000
        
        # Resolve logical collection to physical collection
        physical_collection = self._resolve_collection(collection)
        
        if physical_collection not in self.collections:
            available = ", ".join(self.collections.keys())
            raise ValueError(
                f"Collection '{collection}' (resolves to '{physical_collection}') not found in {self.db_signature}. "
                f"Available collections: {available}"
            )
        
        coll_info = self.collections[physical_collection]
        nb_docs = coll_info["num_docs"]
        size_doc = coll_info["doc_size_bytes"]
        
        # Calculate selectivity for filter fields
        sel_att = self.calculate_selectivity(collection, filter_fields)
        
        # Calculate number of groups
        # Estimate: nb_docs / distinct_values_in_group_by_fields
        if group_by_fields:
            # Estimate distinct values for each group by field
            group_selectivity = 1.0
            for gb_field in group_by_fields:
                field_name = gb_field.get("name")
                # Estimate distinct values (simplified: use collection stats)
                # In practice, this would be more sophisticated
                distinct_estimate = max(1, nb_docs // 100)  # Rough estimate
                group_selectivity *= (1.0 / distinct_estimate)
            
            # Number of groups is approximately: filtered_docs * group_selectivity
            nb_groups = max(1, int(sel_att * nb_docs * (1.0 / group_selectivity)))
        else:
            # No GROUP BY, but has aggregates - single group
            nb_groups = 1
        
        # Calculate S (servers accessed)
        S = self.calculate_S(collection, filter_fields, sharding_key)
        
        # Calculate filtered documents
        res_filtered = int(sel_att * nb_docs)
        
        # Output size: groups × (projection fields + aggregate results)
        all_output_fields = project_fields + [
            {"name": af.get("alias", f"{af['function']}_{af['field']}"), "type": af.get("type", "integer")}
            for af in aggregate_functions
        ]
        size_input, size_msg = self.calculate_query_sizes(
            collection,
            filter_fields,
            all_output_fields
        )
        
        # Calculate RAM volume (per server)
        index = 1 if has_index else 0
        index_size = 1_000_000  # Always 1.00E+06
        # For aggregates, we need to store intermediate results (grouped data)
        # Simplified: assume we store all filtered docs for grouping
        nb_pointers_per_working_server = res_filtered / S if S > 0 else res_filtered
        vol_RAM = index * index_size + nb_pointers_per_working_server * size_doc
        
        # Calculate RAM volume total (different calculation based on S)
        vol_ram_total = self._calculate_ram_vol_total(S, has_index, size_doc, vol_RAM)
        
        # Network volume: query + results (groups, not individual docs)
        vol_network = S * size_input + nb_groups * size_msg
        
        # Calculate time cost: network_vol/bandwidth_network + ram_vol/bandwidth_ram
        time_cost = (
            vol_network / Statistics.BANDWIDTH_NETWORK +
            vol_RAM / Statistics.BANDWIDTH_RAM
        )
        
        # Calculate carbon impact: network_vol*kgCO2eq_network + ram_vol_total*kgCO2eq_ram
        carbon_network = vol_network * Statistics.CO2_NETWORK
        carbon_RAM = vol_ram_total * Statistics.CO2_RAM
        carbon_total = carbon_network + carbon_RAM
        
        # Calculate price: network_vol * network_price
        budget = vol_network * Statistics.NETWORK_PRICE
        
        def format_scientific(value: float) -> str:
            return f"{value:.2e}"
        
        return {
            "query": {
                "query_type": "aggregate",
                "collection": collection,
                "filter_fields": filter_fields,
                "project_fields": project_fields,
                "aggregate_functions": aggregate_functions,
                "group_by_fields": group_by_fields,
                "sharding_key": sharding_key,
                "has_index": has_index,
                "db_signature": self.db_signature,
            },
            "sizes": {
                "size_input_bytes": f"{size_input} B",
                "size_msg_bytes": f"{size_msg} B",
                "size_doc_bytes": f"{size_doc} B",
            },
            "distribution": {
                "S_servers": S,
                "selectivity": format_scientific(sel_att),
                "res_q_results": nb_groups,  # Number of groups, not individual docs
                "nb_docs_total": nb_docs,
                "nb_docs_per_server": nb_docs / self.stats.nb_servers,
                "nb_groups": nb_groups,
                "res_filtered": res_filtered,
            },
            "volumes": {
                "vol_network": f"{format_scientific(vol_network)} B",
                "vol_RAM": f"{format_scientific(vol_RAM)} B",
                "vol_ram_total": f"{format_scientific(vol_ram_total)} B",
            },
            "costs": {
                "time": f"{format_scientific(time_cost)} s",
                "carbon_network": f"{format_scientific(carbon_network)} kgCO2eq",
                "carbon_RAM": f"{format_scientific(carbon_RAM)} kgCO2eq",
                "carbon_total": f"{format_scientific(carbon_total)} kgCO2eq",
                "budget": f"{format_scientific(budget)} €",
            }
        }