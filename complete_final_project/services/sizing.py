from typing import Dict, Optional
from services.statistics import Statistics
from services.schema_client import Schema

class Sizer:
    """
    The Sizer class estimates document and collection sizes
    based purely on the parsed JSON schema and dataset statistics.
    
    Supports manual field counts override: if manual_counts is provided,
    it will use those values instead of automatic counting from schema.
    """

    def __init__(self, schema: Schema, stats: Statistics, manual_counts: Optional[Dict[str, Dict]] = None):
        """
        Initialize Sizer with schema and statistics.
        
        Args:
            schema: Schema instance for parsing JSON schema
            stats: Statistics instance for dataset volumes
            manual_counts: Optional dict mapping collection names to manual field counts.
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
        """
        self.schema = schema
        self.stats = stats
        self.manual_counts = manual_counts or {}

        # Parse entities once
        er = self.schema.detect_entities_and_relations()
        self.entities: Dict[str, dict] = {e["name"]: e for e in er["entities"]}

        # Include nested entities as well
        for e in er["nested_entities"]:
            self.entities[e["name"]] = e

    # ============================================================
    # BASIC UTILITIES
    # ============================================================
    def _get_entity(self, name: str) -> Optional[dict]:
        """Retrieve entity definition by name, case-insensitive."""
        if name in self.entities:
            return self.entities[name]
        lname = name.lower()
        for k, v in self.entities.items():
            if k.lower() == lname:
                return v
        return None

    # ============================================================
    # CORE SIZING LOGIC
    # ============================================================
    def _avg_array_len(self, parent_entity_name: str, attr_name: str) -> int:
        """
        Decide a reasonable average length for arrays based on stats and name heuristics.
        - categories: avg_categories_per_product
        - orderline/orderlines: nb_orderlines / nb_products (DB5)
        - default: 1
        """
        an = attr_name.lower()
        if "categories" in an:
            return int(getattr(self.stats, "avg_categories_per_product", 1))
        if "orderline" in an:
            # When OL embedded in Product (DB5), estimate average OLs per product
            # Avoid div-by-zero just in case.
            prods = max(1, int(getattr(self.stats, "nb_products", 1)))
            ols = int(getattr(self.stats, "nb_orderlines", 0))
            r=ols // prods
            return max(0, ols // prods)
        return 1

    def estimate_document_size(self, entity: dict) -> int:
        """
        Estimate average document size following spreadsheet formula exactly:
        (# integer × 8) + (# strings × 80) + (# date × 20) + (# longString × 200) +
        (avg_array_length × (# array int × 8 + # array string × 80 + # array date × 20 + # array longString × 200)) +
        (# keys × 12)
        
        If manual_counts are provided for this collection, uses those values.
        Otherwise, automatically counts fields from schema.
        """
        entity_name = entity.get("name", "")
        
        # Check if manual counts are provided for this collection
        if entity_name in self.manual_counts:
            counts = self.manual_counts[entity_name].copy()
            # Ensure all required keys exist with defaults
            counts.setdefault("integer", 0)
            counts.setdefault("string", 0)
            counts.setdefault("date", 0)
            counts.setdefault("longstring", 0)
            counts.setdefault("array_int", 0)
            counts.setdefault("array_string", 0)
            counts.setdefault("array_date", 0)
            counts.setdefault("array_longstring", 0)
            counts.setdefault("avg_array_length", 0)
            counts.setdefault("keys", 0)
        else:
            # Use automatic counting from schema
            counts = self._count_fields_and_keys(entity)
        
        # Apply spreadsheet formula
        type_sizes = Statistics.size_map()
        total_size = (
            counts["integer"] * type_sizes["integer"] +
            counts["string"] * type_sizes["string"] +
            counts["date"] * type_sizes["date"] +
            counts["longstring"] * type_sizes["longstring"] +
            counts["avg_array_length"] * (
                counts["array_int"] * type_sizes["integer"] +
                counts["array_string"] * type_sizes["string"] +
                counts["array_date"] * type_sizes["date"] +
                counts["array_longstring"] * type_sizes["longstring"]
            ) +
            counts["keys"] * type_sizes["key"]
        )
        
        return int(total_size)
    
    def _count_fields_and_keys(self, entity: dict) -> dict:
        """
        Count all fields by type and total keys (including nested).
        Returns dict with counts matching spreadsheet columns.
        """
        counts = {
            "integer": 0,
            "string": 0,
            "date": 0,
            "longstring": 0,
            "array_int": 0,
            "array_string": 0,
            "array_date": 0,
            "array_longstring": 0,
            "avg_array_length": 0,
            "keys": 0
        }
        
        parent_name = entity.get("name", "")
        
        for attr in entity.get("attributes", []):
            name = attr.get("name", "")
            attr_type = self.schema._classify_attr_type(attr)
            required = bool(attr.get("required", False))
            
            # Only count required fields (matching spreadsheet behavior)
            if not required:
                continue
            
            # Count this key
            counts["keys"] += 1
            
            # Count primitive types
            if attr_type in ("number", "integer"):
                counts["integer"] += 1
            elif attr_type == "string":
                counts["string"] += 1
            elif attr_type == "date":
                counts["date"] += 1
            elif attr_type == "longstring":
                counts["longstring"] += 1
            elif attr_type == "unknown":
                counts["integer"] += 1  # Default to integer
            
            elif attr_type == "array":
                items = attr.get("items", {}) or {}
                has_properties = isinstance(items, dict) and "properties" in items
                properties = items.get("properties", {}) if has_properties else {}
                is_array_of_objects = has_properties and isinstance(properties, dict) and len(properties) > 0
                
                if is_array_of_objects:
                    # Array of objects: recursively count child entity
                    child_entity_name = name.capitalize()
                    child_entity = self._get_entity(child_entity_name)
                    if child_entity:
                        avg_len = self._avg_array_len(parent_name, name)
                        counts["avg_array_length"] = max(counts["avg_array_length"], avg_len)
                        # Recursively count child's fields and keys
                        child_counts = self._count_fields_and_keys(child_entity)
                        # Add child's counts multiplied by array length
                        counts["integer"] += avg_len * child_counts["integer"]
                        counts["string"] += avg_len * child_counts["string"]
                        counts["date"] += avg_len * child_counts["date"]
                        counts["longstring"] += avg_len * child_counts["longstring"]
                        counts["keys"] += avg_len * child_counts["keys"]
                else:
                    # Arrays of primitives
                    item_type = items.get("type")
                    if not item_type:
                        item_type = "string" if "categories" in name.lower() else "unknown"
                    
                    avg_len = self._avg_array_len(parent_name, name)
                    counts["avg_array_length"] = max(counts["avg_array_length"], avg_len)
                    
                    if item_type in ("number", "integer", "unknown"):
                        counts["array_int"] += 1
                    elif item_type == "string":
                        counts["array_string"] += 1
                    elif item_type == "date":
                        counts["array_date"] += 1
                    elif item_type == "longstring":
                        counts["array_longstring"] += 1
            
            elif attr_type == "reference":
                # Embedded object: recursively count all its fields and keys
                child_name = attr.get("reference_to", "")
                child_entity = self._get_entity(child_name)
                if not child_entity:
                    child_entity = self._get_entity(child_name.capitalize())
                if child_entity:
                    child_counts = self._count_fields_and_keys(child_entity)
                    # Add child's counts (this recursively includes all nested objects)
                    counts["integer"] += child_counts["integer"]
                    counts["string"] += child_counts["string"]
                    counts["date"] += child_counts["date"]
                    counts["longstring"] += child_counts["longstring"]
                    counts["keys"] += child_counts["keys"]
        
        return counts


    def compute_collection_sizes(self) -> Dict[str, dict]:
        """
        Compute estimated size (in bytes) for each collection.
        Uses dataset statistics for the number of documents.
        """
        results = {}
        total_db_size = 0

        # Get the entity-relation structure to identify nested entities
        er = self.schema.detect_entities_and_relations()
        nested_entity_names = {e["name"] for e in er["nested_entities"]}

        for name, entity in self.entities.items():
            # Skip nested entities - they're already counted in their parent's size
            if name in nested_entity_names:
                continue

            # Estimate number of documents per collection
            nb_docs = self.stats.get_collection_count(name)
            avg_size = self.estimate_document_size(entity)
            total_size = nb_docs * avg_size
            # print(f"Estimated size for collection {name}: {nb_docs} docs x {avg_size} B/doc = {total_size} B")
            total_db_size += total_size

            results[name] = {
                "nb_docs": nb_docs,
                "avg_doc_bytes": avg_size,
                "total_size_bytes": total_size,
                "total_size_human": self._format_bytes(total_size, name),
            }

        # Database total summary
        results["Database_Total"] = {
            "total_size_bytes": total_db_size,
            "total_size_human": self._format_bytes(total_db_size, "Database_Total"),
        }

        return results

    # ============================================================
    # BYTE FORMATTING
    # ============================================================
    def _format_bytes(self, size_in_bytes, collection_name: str = "") -> str:
        """
        Convert bytes into a human-readable format (B, KB, MB, GB).
        """
        try:
            if size_in_bytes is None:
                return f"0.00 GB ({collection_name})" if collection_name else "0.00 GB"
            size = float(size_in_bytes)
        except (TypeError, ValueError):
            return f"0.00 GB ({collection_name})" if collection_name else "0.00 GB"

        if size <= 0:
            return f"0.00 GB ({collection_name})" if collection_name else "0.00 GB"

        units = ["B", "KB", "MB", "GB"]
        unit_index = 0
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024.0
            unit_index += 1

        human_size = f"{size:.2f} {units[unit_index]}"
        return f"{human_size} ({collection_name})" if collection_name else human_size