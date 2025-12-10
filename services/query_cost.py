"""
Query Cost Calculator for TD2
Implements formulas from formulas_TD2.tex for computing query costs.
"""

from typing import Dict, List, Optional
from services.statistics import Statistics
from services.schema_client import Schema
from services.sizing import Sizer
import json
from pathlib import Path


class QueryCostCalculator:
    """
    Calculates query execution costs based on formulas from formulas_TD2.tex
    """

    # Collection mapping for embedded collections
    # Maps logical collection names to physical collection names per DB
    COLLECTION_MAPPING = {
        "DB0": {},  # No embedding in DB0
        "DB1": {},  # No embedding in DB1
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

    def __init__(self, db_signature: str = "DB0", collection_size_file: str = "results_TD1.json"):
        self.stats = Statistics()
        self.db_signature = db_signature
        self.collection_size_file = collection_size_file
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
            self.collections[coll["collection"]] = coll
    
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
        self.sizer = Sizer(self.schema, self.stats)
    
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
        Calculate size_input and size_msg based on formulas from formulas_TD2.tex
        
        Formula:
        size_input = Σ(12 + size(value_i)) + 12 × (nesting levels)
        size_msg = Σ(12 + size(value_j)) for projected fields (using actual field types from schema)
        
        Args:
            collection: Collection name for field type lookup
            filter_fields: List of {"name": str, "type": str} for WHERE clause
            project_fields: List of {"name": str, "type": str} for SELECT clause
        
        Returns:
            (size_input, size_msg) in bytes
        """
        KEY_OVERHEAD = Statistics.SIZE_KEY
        NESTING_OVERHEAD = Statistics.SIZE_KEY
        
        type_sizes = {
            "integer": Statistics.SIZE_INTEGER,
            "boolean": Statistics.SIZE_INTEGER,  # boolean = int
            "number": Statistics.SIZE_NUMBER,
            "string": Statistics.SIZE_STRING,
            "date": Statistics.SIZE_DATE,
            "longstring": Statistics.SIZE_LONGSTRING,
        }
        
        # Calculate input size (query sent to servers)
        size_input = 0
        
        # Filter fields: key + value
        for field in filter_fields:
            field_type = field.get("type", "integer")
            value_size = type_sizes.get(field_type, Statistics.SIZE_INTEGER)
            size_input += KEY_OVERHEAD + value_size
        
        # Project fields: key + boolean (1 to include field)
        for field in project_fields:
            size_input += KEY_OVERHEAD + Statistics.SIZE_INTEGER  # Always boolean (1)
        
        # Nesting overhead (one level for collection wrapper)
        size_input += NESTING_OVERHEAD
        
        # Calculate output message size (per result document)
        # Use actual field types from schema, not the 'boolean' from parser
        size_msg = 0
        for field in project_fields:
            field_name = field.get("name")
            # Look up actual field type using infer_type (same as QueryParser)
            actual_type = self.infer_type(collection, field_name)
            
            # Handle embedded objects (like Product.price)
            if actual_type in ["reference", "object"]:
                value_size = self._calculate_object_size(collection, field_name)
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
            collection: Collection name
            filter_fields: List of filter conditions
        
        Returns:
            selectivity (0 to 1)
        """
        # Simple heuristic based on filter field names
        filter_names = [f["name"].lower() for f in filter_fields]
        
        if collection.lower() == "stock":
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

    # ============================================================
    # MAIN COST CALCULATION
    # ============================================================

    def calculate_query_cost(self, query: Dict) -> Dict:
        """
        Calculate complete query cost using formulas from formulas_TD2.tex
        
        Args:
            query: Query specification dict with:
                - collection: str
                - filter_fields: List[Dict]
                - project_fields: List[Dict]
                - sharding_key: Optional[str]
                - has_index: bool
                - index_size: Optional[int] (bytes, if has_index=True)
        
        Returns:
            Dict with all cost metrics
        """
        collection = query["collection"]
        filter_fields = query["filter_fields"]
        project_fields = query["project_fields"]
        sharding_key = query.get("sharding_key")
        has_index = query.get("has_index", False)
        index_size = query.get("index_size", Statistics.DEFAULT_INDEX_SIZE if has_index else 0)
        
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
        
        # Calculate RAM volume
        if has_index:
            # Index lookup: vol_RAM = index_q + sel_att × coll × size_doc
            vol_RAM = index_size + sel_att * coll_per_server * size_doc
        else:
            # Full scan: sel_att = 1
            vol_RAM = 1.0 * coll_per_server * size_doc
        
        # Calculate network volume (filter query formula)
        # vol_network = S × size_input + res_q × size_msg
        vol_network = S * size_input + res_q * size_msg
        
        # Calculate time cost
        time_cost = (
            vol_network / Statistics.BANDWIDTH_NETWORK +
            vol_RAM / Statistics.BANDWIDTH_RAM
        )
        
        # Calculate carbon impact
        carbon_network = vol_network * Statistics.CO2_NETWORK
        carbon_RAM = vol_RAM * Statistics.CO2_RAM
        carbon_total = carbon_network + carbon_RAM
        
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
            },
            "costs": {
                "time": f"{format_scientific(time_cost)} s",
                "carbon_network": f"{format_scientific(carbon_network)} gCO2",
                "carbon_RAM": f"{format_scientific(carbon_RAM)} gCO2",
                "carbon_total": f"{format_scientific(carbon_total)} gCO2",
            }
        }
