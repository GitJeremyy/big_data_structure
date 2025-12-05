"""
Query Cost Calculator for TD2
Implements formulas from formulas_TD2.tex for computing query costs.
"""

from typing import Dict, List, Optional
from services.statistics import Statistics
import json
from pathlib import Path


class QueryCostCalculator:
    """
    Calculates query execution costs based on formulas from formulas_TD2.tex
    """

    def __init__(self, db_signature: str = "DB0"):
        self.stats = Statistics()
        self.db_signature = db_signature
        self._load_db_info()

    def _load_db_info(self):
        """Load collection info from results_TD1.json"""
        results_path = Path(__file__).resolve().parent / 'results_TD1.json'
        with open(results_path, 'r', encoding='utf-8') as f:
            all_results = json.load(f)
        
        if self.db_signature not in all_results:
            raise ValueError(f"DB signature {self.db_signature} not found")
        
        self.db_info = all_results[self.db_signature]
        # Create a lookup for collection info
        self.collections = {}
        for coll in self.db_info["collections"]:
            self.collections[coll["collection"]] = coll

    # ============================================================
    # QUERY SIZE CALCULATION (from formulas_TD2.tex)
    # ============================================================

    def calculate_query_sizes(self, filter_fields: List[Dict], project_fields: List[Dict]) -> tuple:
        """
        Calculate size_input and size_msg based on formulas from formulas_TD2.tex
        
        Formula:
        size_input = Σ(12 + size(value_i)) + 12 × (nesting levels)
        size_msg = Σ(12 + size(value_j)) for projected fields
        
        Args:
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
        size_msg = 0
        for field in project_fields:
            field_type = field.get("type", "integer")
            value_size = type_sizes.get(field_type, 8)
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
                # Assume Apple products for example
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
        
        # Get collection info
        if collection not in self.collections:
            raise ValueError(f"Collection {collection} not found in {self.db_signature}")
        
        coll_info = self.collections[collection]
        nb_docs = coll_info["num_docs"]
        size_doc = coll_info["doc_size_bytes"]
        
        # Calculate query sizes
        size_input, size_msg = self.calculate_query_sizes(filter_fields, project_fields)
        
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
                "size_input": size_input,
                "size_msg": size_msg,
                "size_doc": size_doc,
            },
            "distribution": {
                "S": S,
                "selectivity": sel_att,
                "res_q": res_q,
                "nb_docs_total": nb_docs,
                "nb_docs_per_server": coll_per_server,
            },
            "volumes": {
                "vol_network_bytes": vol_network,
                "vol_RAM_bytes": vol_RAM,
            },
            "costs": {
                "time_seconds": time_cost,
                "carbon_network_gCO2": carbon_network,
                "carbon_RAM_gCO2": carbon_RAM,
                "carbon_total_gCO2": carbon_total,
            }
        }
