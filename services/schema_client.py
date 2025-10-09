import json

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
    
    # def _detect_relations(self, entities, nested_entities):
    #     """
    #     Détecte les relations entre entités basées sur les références et l'imbrication.
        
    #     Returns:
    #         list: Liste des relations détectées
    #     """
    #     relations = []
    #     all_entities = {**entities, **nested_entities}
        
    #     for entity_name, entity_info in all_entities.items():
    #         for attr in entity_info['attributes']:
    #             # Relation par référence/embedding
    #             if attr.get('type') == 'reference':
    #                 ref_to = attr.get('reference_to')
    #                 if ref_to:
    #                     relations.append({
    #                         'from': entity_name,
    #                         'to': ref_to.capitalize() if ref_to.islower() else ref_to,
    #                         'type': 'embeds' if attr.get('embedded') else 'references',
    #                         'attribute': attr['name'],
    #                         'cardinality': '1:1' if attr.get('required') else '1:0..1'
    #                     })
                
    #             # Relation par array (potentiellement 1:N)
    #             elif attr.get('type') == 'array':
    #                 # Si le nom de l'attribut correspond à une entité
    #                 attr_name = attr['name']
    #                 potential_entity = attr_name.rstrip('s').capitalize()
    #                 if potential_entity in all_entities or attr_name.capitalize() in all_entities:
    #                     relations.append({
    #                         'from': entity_name,
    #                         'to': potential_entity if potential_entity in all_entities else attr_name.capitalize(),
    #                         'type': 'has_many',
    #                         'attribute': attr['name'],
    #                         'cardinality': '1:N'
    #                     })
        
    #     return relations
    
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
        
        # print("\n=== RELATIONS ===")
        # for rel in result['relations']:
        #     print(f"  {rel['from']} --[{rel['type']}]--> {rel['to']} ({rel['cardinality']}) via '{rel['attribute']}'")
        
        return result