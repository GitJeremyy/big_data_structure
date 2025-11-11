from typing import Dict, Set, Optional, Tuple
from services.relationships import DenormProfile, RelationshipSpec
from services.statistics import Statistics
from services.schema_client import Schema

class Sizer:
    def __init__(self, schema: Schema, profile: DenormProfile, stats: Statistics):
        self.schema = schema
        self.profile = profile
        self.stats = stats

        # Parse entities once
        er = self.schema.detect_entities_and_relations()
        # Map: entity_name -> entity_dict
        self.entities: Dict[str, dict] = {e["name"]: e for e in er["entities"]}
        # Note: nested entities are not top-level collections; if needed
        # you can add them to this dict as well:
        for e in er["nested_entities"]:
            self.entities[e["name"]] = e

        # Memo for per-entity document size (including relationship costs)
        self._doc_size_cache: Dict[str, int] = {}
        # Memo for per-entity type counts (relationship-aware)
        self._type_count_cache: Dict[Tuple[str, bool], Dict[str, int]] = {}

    def _get_entity(self, name: str) -> Optional[dict]:
        """Retrieve entity definition by name, case-insensitive fallback."""
        if name in self.entities:
            return self.entities[name]
        # fallback: case-insensitive match
        lname = name.lower()
        for k, v in self.entities.items():
            if k.lower() == lname:
                return v
        return None

    def _entity_intrinsic_size(self, entity_name: str) -> int:
        entity = self._get_entity(entity_name)
        if not entity:
            return 0
        return self.schema.estimate_document_size(entity, self.stats)

    def _rels_from(self, entity_name: str):
        for r in self.profile.relationships:
            if r.from_entity == entity_name:
                yield r

    def _entity_doc_size(self, entity_name: str, stack: Optional[Set[str]] = None) -> int:
        """
        Full average document size for 'entity_name',
        including relationship contributions per profile.
        """
        if entity_name in self._doc_size_cache:
            return self._doc_size_cache[entity_name]

        if stack is None:
            stack = set()

        # cycle guard
        if entity_name in stack:
            # In a rare cycle, fallback to intrinsic only to prevent infinite recursion
            size = self._entity_intrinsic_size(entity_name)
            self._doc_size_cache[entity_name] = size
            return size

        stack.add(entity_name)

        size = self._entity_intrinsic_size(entity_name)

        for rel in self._rels_from(entity_name):
            if rel.stored_as == "fk":
                # Use central size policy: 8B per foreign key field
                per_fk = self.stats.SIZE_NUMBER
                size += per_fk * (len(rel.fk_fields) if rel.fk_fields else 1)

            elif rel.stored_as == "embed_one":
                child_size = self._entity_doc_size(rel.to_entity, stack)
                size += child_size

            elif rel.stored_as == "embed_many":
                # 12B array overhead + m × size(child)
                m = rel.avg_multiplicity or 0
                child_size = self._entity_doc_size(rel.to_entity, stack)
                size += int(m * child_size)

            else:
                # Unknown storage hint; ignore
                pass

        stack.remove(entity_name)
        self._doc_size_cache[entity_name] = size
        return size

    def compute_collection_sizes(self) -> Dict[str, dict]:
        """
        For each top-level collection in the profile:
        - compute document size (with relationships),
        - multiply by number of documents from statistics,
        - return sizes in bytes and human form,
        - also include a database total.
        """
        results: Dict[str, dict] = {}
        total_db_bytes = 0

        for coll in self.profile.collections:
            avg_doc_bytes = self._entity_doc_size(coll)
            nb_docs = self.stats.get_collection_count(coll)
            total_bytes = avg_doc_bytes * nb_docs
            total_db_bytes += total_bytes

            results[coll] = {
                "nb_docs": nb_docs,
                "avg_doc_bytes": avg_doc_bytes,
                "total_bytes": total_bytes,
                "total_human": self.schema._format_bytes(total_bytes, coll),
            }

        results["Database_Total"] = {
            "total_bytes": total_db_bytes,
            "total_human": self.schema._format_bytes(total_db_bytes, "Database_Total"),
        }
        return results

    # ---------------------------
    # Debug: type counts per entity
    # ---------------------------
    def _merge_counts(self, base: Dict[str, int], extra: Dict[str, int], factor: int = 1) -> None:
        for k, v in extra.items():
            base[k] = base.get(k, 0) + v * factor

    def _entity_type_counts(
        self,
        entity_name: str,
        stack: Optional[Set[str]] = None,
        multiply_embed_many: bool = False,
    ) -> Dict[str, int]:
        """
        Relationship-aware type counts for a single entity's document.
        - Includes intrinsic attribute types from the schema.
        - Adds counts for relationships per profile:
          fk        -> +N numbers (one per FK field or 1 if unspecified)
          embed_one -> +child entity counts
          embed_many-> +1 array, +child entity counts (optionally multiplied)
        Cycle-safe and cached.
        """
        cache_key = (entity_name, multiply_embed_many)
        if cache_key in self._type_count_cache:
            return self._type_count_cache[cache_key]

        if stack is None:
            stack = set()
        if entity_name in stack:
            # Cycle guard: return intrinsic only to break the loop
            ent = self.entities.get(entity_name)
            return self.schema.count_attribute_types(ent) if ent else {}

        stack.add(entity_name)

        ent = self._get_entity(entity_name)
        counts: Dict[str, int] = {k: 0 for k in [
            "number", "integer", "string", "date", "longstring",
            "array", "object", "reference", "unknown"
        ]}

        # Intrinsic attributes
        if ent:
            self._merge_counts(counts, self.schema.count_attribute_types(ent))

        # Relationship contributions
        for rel in self._rels_from(entity_name):
            if rel.stored_as == "fk":
                n = len(rel.fk_fields) if rel.fk_fields else 1
                counts["number"] = counts.get("number", 0) + n

            elif rel.stored_as == "embed_one":
                child = self._entity_type_counts(rel.to_entity, stack, multiply_embed_many)
                self._merge_counts(counts, child)

            elif rel.stored_as == "embed_many":
                # The array field itself
                counts["array"] = counts.get("array", 0) + 1
                child = self._entity_type_counts(rel.to_entity, stack, multiply_embed_many)
                if multiply_embed_many and rel.avg_multiplicity:
                    self._merge_counts(counts, child, factor=int(rel.avg_multiplicity))
                else:
                    self._merge_counts(counts, child)

        stack.remove(entity_name)
        self._type_count_cache[cache_key] = counts
        return counts

    def debug_type_counts(self, multiply_embed_many: bool = False) -> Dict[str, Dict[str, int]]:
        """
        Return a mapping {collection_name: {type_name: count}} for all
        top-level collections in the current profile. If multiply_embed_many
        is True, counts for embedded-many children are multiplied by their
        average multiplicity; otherwise they are included once (plus the array field).
        """
        out: Dict[str, Dict[str, int]] = {}
        for coll in self.profile.collections:
            out[coll] = self._entity_type_counts(coll, multiply_embed_many=multiply_embed_many)
        return out