from typing import Dict, Set, Optional
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

    def _entity_intrinsic_size(self, entity_name: str) -> int:
        entity = self.entities.get(entity_name)
        if not entity:
            return 0
        return self.schema.estimate_document_size(entity)

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
                array_overhead = self.stats.SIZE_ARRAY
                m = rel.avg_multiplicity or 0
                child_size = self._entity_doc_size(rel.to_entity, stack)
                size += array_overhead + int(m * child_size)

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
                "total_human": self.schema._format_bytes(total_bytes),
            }

        results["Database_Total"] = {
            "total_bytes": total_db_bytes,
            "total_human": self.schema._format_bytes(total_db_bytes),
        }
        return results