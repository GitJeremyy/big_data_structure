import json
from services.statistics import Statistics

class Schema:
    def __init__(self, schema_json):
        if isinstance(schema_json, str):
            self.schema = json.loads(schema_json)
        else:
            self.schema = schema_json

    def get_collections(self):
        # Generalize: collections are top-level keys representing entities/relations
        # Heuristic: keys with dict values and at least one field
        return [
            key for key, value in self.schema.items()
            if isinstance(value, dict) and len(value) > 0
        ]
    
    def detect_entities_and_relations(self):
        """
        Détecte les entités et les relations avec leurs attributs dans un JSON schema.
        Gère les structures imbriquées (nested objects).
        
        Returns:
            dict: Un dictionnaire avec:
                - 'entities': Liste des entités détectées avec leurs attributs
                - 'relations': Liste des relations détectées
                - 'nested_entities': Entités trouvées imbriquées dans d'autres
        """
        entities = {}
        # relations = []
        nested_entities = {}
        
        # Naviguer dans le schéma properties si présent
        schema_props = self.schema.get('properties', self.schema)
        
        # Extraire toutes les entités (top-level et imbriquées)
        for collection_name in schema_props:
            self._extract_entities_recursive(
                collection_name, 
                schema_props[collection_name], 
                entities, 
                nested_entities,
                parent_path=None
            )
        
        # Analyser les relations entre entités
        #relations = self._detect_relations(entities, nested_entities)
        
        return {
            'entities': list(entities.values()),
            'nested_entities': list(nested_entities.values())
            #'relations': relations
        }
    
    def _extract_entities_recursive(self, name, schema_def, entities, nested_entities, parent_path=None):
        """
        Extrait récursivement les entités d'un schéma, y compris les objets imbriqués.
        
        Args:
            name: Nom de l'entité
            schema_def: Définition du schéma de l'entité
            entities: Dictionnaire des entités de premier niveau
            nested_entities: Dictionnaire des entités imbriquées
            parent_path: Chemin parent (pour les entités imbriquées)
        """
        if not isinstance(schema_def, dict):
            return
        
        properties = schema_def.get('properties', {})
        required_fields = schema_def.get('required', [])
        
        # Extraire les attributs simples et identifier les sous-entités
        attributes = []
        nested_objects = []
        
        for prop_name, prop_def in properties.items():
            if not isinstance(prop_def, dict):
                continue
            
            prop_type = prop_def.get('type', 'unknown')
            
            # Si c'est un objet avec des propriétés, c'est potentiellement une entité imbriquée
            if prop_type == 'object' and 'properties' in prop_def:
                nested_objects.append((prop_name, prop_def))
                # Marquer comme référence dans les attributs
                attributes.append({
                    'name': prop_name,
                    'type': 'reference',
                    'reference_to': prop_name,
                    'required': prop_name in required_fields,
                    'embedded': True
                })
            elif prop_type == 'object' and 'properties' not in prop_def:
                # Objet simple (comme price avec amount, currency, vat_rate)
                attributes.append({
                    'name': prop_name,
                    'type': 'object',
                    'required': prop_name in required_fields,
                    'structure': prop_def
                })
            elif prop_type == 'array':
                attributes.append({
                    'name': prop_name,
                    'type': 'array',
                    'items': prop_def.get('items', {}),
                    'required': prop_name in required_fields
                })
            else:
                # Attribut simple
                attributes.append({
                    'name': prop_name,
                    'type': prop_type,
                    'format': prop_def.get('format'),
                    'minLength': prop_def.get('minLength'),
                    'required': prop_name in required_fields
                })
        
        # Identifier la clé primaire
        primary_key = self._identify_primary_key(attributes)
        
        # Stocker l'entité
        entity_info = {
            'name': name,
            'attributes': attributes,
            'primary_key': primary_key,
            'parent': parent_path
        }
        
        if parent_path is None:
            entities[name] = entity_info
        else:
            nested_entities[name] = entity_info
        
        # Extraire récursivement les entités imbriquées
        for nested_name, nested_def in nested_objects:
            # Capitaliser le nom si nécessaire
            entity_name = nested_name.capitalize() if nested_name.islower() else nested_name
            self._extract_entities_recursive(
                entity_name,
                nested_def,
                entities,
                nested_entities,
                parent_path=name
            )
    
    def _identify_primary_key(self, attributes):
        """
        Identifie la clé primaire parmi les attributs.
        
        Heuristiques:
        - Attributs nommés 'id', '_id', ou commençant par 'ID' (comme IDC, IDP, IDS)
        """
        # Chercher les patterns de clés primaires
        for attr in attributes:
            attr_name = attr['name']
            # ID suivi de majuscule (IDC, IDP, IDS, IDW)
            if attr_name.startswith('ID') and len(attr_name) <= 4:
                return attr_name
            # Patterns classiques
            if attr_name.lower() in ['id', '_id', 'pk']:
                return attr_name
        
        return None
    
    def print_entities_and_relations(self):
        """
        Affiche de manière formatée les entités et relations détectées.
        Utile pour le debugging.
        """
        result = self.detect_entities_and_relations()
        
        print("\n=== ENTITÉS PRINCIPALES ===")
        for entity in result['entities']:
            print(f"\n{entity['name']} (PK: {entity.get('primary_key', 'N/A')})")
            print("  Attributs:")
            for attr in entity['attributes']:
                req = "✓" if attr.get('required') else " "
                print(f"    [{req}] {attr['name']}: {attr['type']}")
        
        print("\n=== ENTITÉS IMBRIQUÉES ===")
        for entity in result['nested_entities']:
            print(f"\n{entity['name']} (dans {entity['parent']}) (PK: {entity.get('primary_key', 'N/A')})")
            print("  Attributs:")
            for attr in entity['attributes']:
                req = "✓" if attr.get('required') else " "
                print(f"    [{req}] {attr['name']}: {attr['type']}")
        
        return result
    
    def estimate_document_size(self, entity):
        """
        Estimate the average document size (in bytes) for one entity
        based on attribute types, using official approximation rules.
        """
        # official approximate byte sizes come from Statistics
        type_sizes = Statistics.size_map()

        total_size = 0

        for attr in entity["attributes"]:
            attr_type = attr.get("type", "unknown")

            # handle specific fields specially
            name = attr["name"].lower()

            # Detect "description" or "comment" → longstring
            if "description" in name or "comment" in name:
                total_size += type_sizes["longstring"]
            # Detect "date" fields → date
            elif "date" in name:
                total_size += type_sizes["date"]
            # Normal type lookup
            else:
                total_size += type_sizes.get(attr_type, type_sizes["unknown"])

        return total_size
    
    def compute_collection_sizes(self, stats):
        """
        Compute estimated size (in bytes) for each collection
        based on the number of documents and estimated document size.
        Also computes the total database size.
        """
        entity_results = self.detect_entities_and_relations()
        entities = entity_results["entities"]

        results = {}
        total_db_size = 0

        for entity in entities:
            name = entity["name"]

            # Estimate number of documents per collection
            if name.lower() == "client":
                nb_docs = stats.nb_clients
            elif name.lower() == "product":
                nb_docs = stats.nb_products
            elif name.lower() == "orderline":
                nb_docs = stats.nb_orderlines
            elif name.lower() == "warehouse":
                nb_docs = stats.nb_warehouses
            elif name.lower() == "stock":
                nb_docs = stats.nb_products  # one stock entry per product
            else:
                nb_docs = 0  # fallback

            avg_size = self.estimate_document_size(entity)
            total_size = nb_docs * avg_size
            total_db_size += total_size

            results[name] = {
                "nb_docs": nb_docs,
                "avg_size_bytes": avg_size,
                "total_size_bytes": total_size,
                "total_size_human": self._format_bytes(total_size)
            }

        # Add total DB summary
        results["Database_Total"] = {
            "total_size_bytes": total_db_size,
            "total_size_human": self._format_bytes(total_db_size)
        }

        return results
    
    def _format_bytes(self, size_in_bytes):
        """
        Convert bytes into a human-readable format (B, KB, MB, GB, TB).
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_in_bytes < 1024:
                return f"{size_in_bytes:.2f} {unit}"
            size_in_bytes /= 1024
        return f"{size_in_bytes:.2f} PB"  # just in case it’s huge