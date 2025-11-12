import json

class Schema:
    def __init__(self, schema_json):
        if isinstance(schema_json, str):
            self.schema = json.loads(schema_json)
        else:
            self.schema = schema_json

    def _classify_attr_type(self, attr: dict) -> str:
        """
        Determine a normalised logical type name for an attribute.
        Returns one of: number, integer, string, date, longstring,
        array, object, reference, unknown.
        """
        name = str(attr.get("name", "")).lower()
        t = attr.get("type", "unknown")

        # Name-based overrides (priority)
        if "description" in name or "comment" in name:
            return "longstring"
        if "date" in name:
            return "date"

        # Fallback to declared JSON Schema type
        if t in {"number", "integer", "string", "array", "object", "reference"}:
            return t

        return "unknown"

    def count_attribute_types(self, entity: dict) -> dict:
        """
        Count intrinsic attribute types for a single entity.
        """
        counts = {t: 0 for t in [
            "number", "integer", "string", "date", "longstring",
            "array", "object", "reference", "unknown"
        ]}

        for attr in entity.get("attributes", []):
            counts[self._classify_attr_type(attr)] += 1

        print(f"Attribute type counts for {entity.get('name', 'unknown')}: {counts}")

        return counts

    def get_collections(self):
        """
        Return top-level collections (entities) from schema.
        """
        return [
            key for key, value in self.schema.items()
            if isinstance(value, dict) and len(value) > 0
        ]

    def detect_entities_and_relations(self):
        """
        Detect entities and nested entities in a JSON schema.
        """
        entities = {}
        nested_entities = {}
        schema_props = self.schema.get("properties", self.schema)

        for collection_name in schema_props:
            self._extract_entities_recursive(
                collection_name,
                schema_props[collection_name],
                entities,
                nested_entities,
                parent_path=None
            )

        return {
            "entities": list(entities.values()),
            "nested_entities": list(nested_entities.values())
        }

    def _extract_entities_recursive(self, name, schema_def, entities, nested_entities, parent_path=None):
        """
        Recursively extract entities and nested entities.
        """
        if not isinstance(schema_def, dict):
            return

        properties = schema_def.get("properties", {})
        required_fields = schema_def.get("required", [])
        attributes = []
        nested_objects = []

        for prop_name, prop_def in properties.items():
            if not isinstance(prop_def, dict):
                continue

            prop_type = prop_def.get("type", "unknown")

            # Embedded object (nested entity)
            if prop_type == "object" and "properties" in prop_def:
                nested_objects.append((prop_name, prop_def))
                attributes.append({
                    "name": prop_name,
                    "type": "reference",
                    "reference_to": prop_name,
                    "required": prop_name in required_fields,
                    "embedded": True
                })
            elif prop_type == "object":
                attributes.append({
                    "name": prop_name,
                    "type": "object",
                    "required": prop_name in required_fields,
                    "structure": prop_def
                })
            elif prop_type == "array":
                attributes.append({
                    "name": prop_name,
                    "type": "array",
                    "items": prop_def.get("items", {}),
                    "required": prop_name in required_fields
                })

                # Detect arrays of objects → create nested entity
                items = prop_def.get("items", {})
                if isinstance(items, dict) and "properties" in items:
                    child_name = prop_name.capitalize()
                    self._extract_entities_recursive(
                        child_name,
                        items,
                        entities,
                        nested_entities,
                        parent_path=name
                    )
            else:
                attributes.append({
                    "name": prop_name,
                    "type": prop_type,
                    "format": prop_def.get("format"),
                    "minLength": prop_def.get("minLength"),
                    "required": prop_name in required_fields
                })

        entity_info = {
            "name": name,
            "attributes": attributes,
            "parent": parent_path
        }

        if parent_path is None:
            entities[name] = entity_info
        else:
            nested_entities[name] = entity_info

        # Handle nested objects
        for nested_name, nested_def in nested_objects:
            entity_name = nested_name.capitalize() if nested_name.islower() else nested_name
            self._extract_entities_recursive(
                entity_name,
                nested_def,
                entities,
                nested_entities,
                parent_path=name
            )

    def print_entities_and_relations(self):
        """
        Display entities and nested entities for debugging.
        """
        result = self.detect_entities_and_relations()

        print("\n=== ENTITÉS PRINCIPALES ===")
        for entity in result["entities"]:
            print(f"\n{entity['name']}")
            print("  Attributs:")
            for attr in entity["attributes"]:
                req = "✓" if attr.get("required") else " "
                print(f"    [{req}] {attr['name']}: {attr['type']}")

        print("\n=== ENTITÉS IMBRIQUÉES ===")
        for entity in result["nested_entities"]:
            print(f"\n{entity['name']} (dans {entity['parent']})")
            print("  Attributs:")
            for attr in entity["attributes"]:
                req = "✓" if attr.get("required") else " "
                print(f"    [{req}] {attr['name']}: {attr['type']}")

        return result