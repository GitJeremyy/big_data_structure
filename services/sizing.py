from typing import Dict, Optional
from services.statistics import Statistics
from services.schema_client import Schema

class Sizer:
    """
    The Sizer class estimates document and collection sizes
    based purely on the parsed JSON schema and dataset statistics.
    """

    def __init__(self, schema: Schema, stats: Statistics):
        self.schema = schema
        self.stats = stats

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
        Estimate average document size for one entity (pure schema-based).
        Rule: For every REQUIRED attribute, add +12 B for the key.
        Then add the value cost:
        - primitives: Statistics.size_map()
        - array of primitives: avg_len x size(type)
        - array of objects:   avg_len x size(child object)
        - embedded object ('reference'): inline child object size
        """
        type_sizes = Statistics.size_map()
        KEY = type_sizes["key"]

        total_size = 0
        parent_name = entity.get("name", "")

        for attr in entity.get("attributes", []):
            name = attr.get("name", "")
            required = bool(attr.get("required"))
            attr_type = self.schema._classify_attr_type(attr)

            # 1) Key overhead for REQUIRED attributes
            if required:
                total_size += KEY

            # 2) Value cost
            if attr_type in ("number", "integer", "string", "date", "longstring", "unknown"):
                total_size += type_sizes.get(attr_type, type_sizes["unknown"])

            elif attr_type == "array":
                # Key overhead already applied above if required
                items = attr.get("items", {}) or {}
                # Arrays of objects?
                if isinstance(items, dict) and "properties" in items:
                    # Your parser created a nested entity with name derived from the field name.
                    # For arrays-of-objects, you used child_name = prop_name.capitalize()
                    child_entity_name = name.capitalize()
                    child_entity = self._get_entity(child_entity_name)
                    child_size = self.estimate_document_size(child_entity) if child_entity else 0
                    avg_len = self._avg_array_len(parent_name, name)
                    total_size += avg_len * child_size
                else:
                    # Arrays of primitives
                    item_type = items.get("type")
                    if not item_type:
                        # Heuristic: categories default to string; otherwise unknown
                        item_type = "string" if "categories" in name.lower() else "unknown"
                    per_item = type_sizes.get(item_type, type_sizes["unknown"])
                    avg_len = self._avg_array_len(parent_name, name)
                    total_size += avg_len * per_item

            elif attr_type == "reference":
                # Embedded object (object with properties)
                # Your parser set attr['reference_to'] to the child name.
                child_name = attr.get("reference_to", "")
                child_entity = self._get_entity(child_name)
                if not child_entity:
                    # Try capitalised fallback (parser often capitalises nested entity names)
                    child_entity = self._get_entity(child_name.capitalize())
                if child_entity:
                    total_size += self.estimate_document_size(child_entity)
                # else: if not found, we can’t expand; nothing more to add

            elif attr_type == "object":
                # Object-without-properties case: nothing to expand.
                # We already added the key if required. Value unknown: treat as 0.
                pass

            else:
                # Any truly unknown fallback — already handled by "unknown" above
                pass

        return int(total_size)


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