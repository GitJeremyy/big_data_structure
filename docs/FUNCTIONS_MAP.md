# Fonctions et Rôles par Fichier

Ce document liste toutes les fonctions et méthodes des principaux fichiers du projet, avec leur rôle exact, pour faciliter la navigation et la compréhension.

---

## services/schema_client.py

- **Schema.**init**(schema_json):** Initialise l'objet schéma à partir d'une chaîne JSON ou d'un dict.
- **Schema.\_classify_attr_type(attr):** Normalise le type logique d'un attribut en l'un de: number, integer, string, date, longstring, array, object, reference, unknown. Priorité aux heuristiques basées sur le nom ("date", "description/comment"), sinon sur le `type` du JSON Schema.
- **Schema.detect_entities_and_relations():** Parcourt le schéma et retourne deux listes: `entities` (racines) et `nested_entities` (imbriquées), chaque entité décrivant ses attributs et son parent.
- **Schema.\_extract_entities_recursive(name, schema_def, entities, nested_entities, parent_path=None):** Méthode interne récursive qui construit la structure des entités, détecte objets imbriqués et tableaux d'objets, marque les attributs `reference` et `array` et leur caractère `required`.
- **Schema.print_entities_and_relations():** Affiche un résumé lisible des entités principales et imbriquées (debug) et retourne le résultat.

## services/statistics.py

- **Statistics.size_map():** Classe-méthode renvoyant la table des tailles par type logique (`integer:8`, `string:80`, etc.).
- **Statistics.**init**():** Initialise les statistiques globales: volumétrie (clients, produits, orderlines, warehouses), moyennes comportementales, et `nb_servers`.
- **Statistics.get_collection_count(name):** Renvoie le nombre attendu de documents pour une collection (`Client`, `Product`, `OrderLine`, `Warehouse`, `Stock`).
- **Statistics.describe():** Imprime un résumé complet de la volumétrie (debug).
- **Statistics.compute_sharding_stats():** Calcule/affiche des statistiques moyennes de distribution par stratégie de sharding (docs/server, keys/server, serveurs actifs) et renvoie un dict détaillé.

## services/sizing.py

- **Sizer.**init**(schema, stats, manual_counts=None):** Initialise avec `Schema` et `Statistics`, prépare `entities` (racines + imbriquées), accepte des comptages manuels par collection pour surcharger la détection automatique.
- **Sizer.\_get_entity(name):** Récupère une entité par nom (insensible à la casse).
- **Sizer.\_avg_array_len(parent_entity_name, attr_name):** Estime la longueur moyenne des tableaux selon heuristiques/statistiques (ex: `categories` → `avg_categories_per_product`, `orderline` → `nb_orderlines/nb_products`, sinon 1).
- **Sizer.estimate_document_size(entity):** Calcule la taille moyenne d'un document selon la formule du TD: somme des champs multipliée par tailles type + clés + contenu des tableaux (longueur moyenne × contenu). Utilise `manual_counts` si fournis, sinon `_count_fields_and_keys`.
- **Sizer.\_count_fields_and_keys(entity):** Compte tous les champs (par type) et le nombre total de clés pour une entité, en respectant `required`. Gère tableaux de primitifs (incrémente `array_*`), tableaux d'objets (récursion), et références imbriquées (récursion).
- **Sizer.compute_collection_sizes():** Renvoie pour chaque collection (non imbriquée) le nombre de documents, la taille moyenne des docs, la taille totale, et le total base. Utilise `Statistics.get_collection_count`.
- **Sizer.\_format_bytes(size_in_bytes, collection_name=""):** Formate une taille en B/KB/MB/GB.

## services/query_parser.py

- **QueryParser.**init**(db_signature="DB1"):** Charge le JSON Schema correspondant et construit le lookup `field_types`.
- **QueryParser.\_load_schema():** Lit `JSON_schema/json-schema-<DB>.json` et instancie `Schema`.
- **QueryParser.\_build_field_type_lookup():** Construit `field_types` par collection avec types normalisés des attributs (inclut entités imbriquées).
- **QueryParser.infer_type(collection, field_name):** Renvoie le type d'un champ à partir de `field_types` (fallback `integer`).
- **QueryParser.parse(sql, type_overrides=None):** Détermine si la requête est `filter`, `join` ou `aggregate` (détecte `JOIN`, `GROUP BY`, fonctions d'agrégat, sous-requêtes) et délègue vers les parseurs spécialisés.
- **QueryParser.\_parse_filter_query(sql, type_overrides):** Extrait `SELECT`, `FROM`, `WHERE`; construit `project_fields` et `filter_fields` avec types inférés.
- **QueryParser.\_parse_join_query(sql, type_overrides):** Parse `SELECT`, `FROM + JOIN + ON`, résout `collections`, `aliases`, `join_conditions`, puis `WHERE` multi-collections; renvoie `project_fields` et `filter_fields` avec leur collection d'origine.
- **QueryParser.\_parse_aggregate_query(sql, type_overrides):** Version agrégats (avec ou sans JOIN): extrait fonctions d'agrégat (type et alias), `GROUP BY`, `SELECT` non-agrégés, `WHERE`.
- **QueryParser.\_parse_select_fields(select_clause, collection, type_overrides):** Transforme la liste de champs en `project_fields` (type inféré).
- **QueryParser.\_parse_where_clause(where_clause, collection, type_overrides):** Transforme la clause WHERE (égalité + `AND`) en `filter_fields` (type inféré).
- **QueryParser.\_parse_from_join_clause(from_join_clause):** Extrait collections, alias, et conditions `ON` (avec résolution des alias).
- **QueryParser.\_parse_select_fields_with_collections(select_clause, collections, aliases, type_overrides):** Parse `SELECT` en contexte multi-collections (JOIN).
- **QueryParser.\_parse_where_clause_with_collections(where_clause, collections, aliases, type_overrides):** Parse `WHERE` multi-collections (JOIN).
- **QueryParser.\_parse_aggregate_functions(select_clause, collections, aliases, type_overrides):** Extrait fonctions d'agrégat (SUM/COUNT/AVG/MAX/MIN), leur collection, type, et alias résultant.
- **QueryParser.\_parse_group_by_clause(sql, collections, aliases, type_overrides):** Extrait les champs `GROUP BY` (avec ou sans alias).
- **QueryParser.\_parse_subquery(sql):** Détecte une sous-requête `FROM (SELECT ...) AS alias`, la parse récursivement et renvoie sa structure.
- **parse_query(sql, db_signature="DB1", type_overrides=None):** Helper global: instancie `QueryParser` et appelle `parse()`.

## services/calculate_stats.py

- **format_scientific(value):** Formatage en notation scientifique.
- **calculate_budget(network_vol):** Convertit un volume réseau (octets) en coût en euros via `PRICE_PER_BYTE`.
- **extract_field_counts_by_type(fields, collection, calculator):** Compte `integer/string/date` dans une liste de champs (utilise `calculator.infer_type()` si type absent/boolean).
- **extract_ram_vol_per_server(result, has_index):** Extrait la formule RAM par serveur à partir de `calculate_query_cost()`: `index*1e6 + (res_q/S)*size_doc`.
- **extract_projection_counts_by_type(fields):** Compte les types dans `project_fields` (types déjà inférés par le parser).
- **extract_query_characteristics(parsed_query, calculator):** Agrège (selon `filter/join/aggregate`) les compteurs de champs, calcule `(size_input, size_msg)` via `calculator`, et retourne `filter_counts`, `proj_counts`, `nb_keys`, `query_size`, `output_size`.
- **extract_cost_breakdown(result, sharding_key, has_index):** Récupère des métriques du résultat (S, tailles, nb_output, volumes, temps, CO2, budget), recalcule `vol_ram_per_server`, et produit un résumé (inclut métriques `join` si présentes).

## services/manual_counts_example.py

- **get_manual_counts_for_db(db_signature):** Retourne un dict des comptages manuels pour une DB donnée, fusionné avec collections communes. Note: le module expose aussi des constantes `MANUAL_COUNTS_DB*` et `MANUAL_COUNTS_COMMON` pour surcharger les estimations.

## services/query_cost.py

- **QueryCostCalculator.**init**(db_signature="DB1", collection_size_file="results_TD1.json", manual_counts=None, manual_doc_sizes=None):** Prépare statistiques, schéma, tailles de docs (TD1), et `Sizer`; accepte des overrides pour comptages/tailles.
- **\_load_db_info():** Lit `results_TD1.json`, charge les collections (nom, nb docs, taille doc), applique `manual_doc_sizes` si fournis.
- **\_load_schema():** Charge le schéma JSON et initialise `Sizer` (avec `manual_counts`).
- **\_build_field_type_lookup():** Construit le mapping `field_types` pour inférence des types par collection.
- **infer_type(collection, field_name):** Type inféré pour un champ (fallback `integer`).
- **\_calculate_object_size(collection, field_name):** Pour une projection d'objet `reference/object`, calcule la taille via `Sizer.estimate_document_size()`; sinon mappe la taille primitive.
- **\_resolve_collection(logical_collection):** Mappe une collection logique vers sa collection physique selon `COLLECTION_MAPPING` (gestion des collections imbriquées).
- **calculate_query_sizes(collection, filter_fields, project_fields):** Calcule `size_input` (taille de la requête) et `size_msg` (taille du message par résultat) via la formule TD (types + overhead clés).
- **calculate_join_sizes(collections, filter_fields, project_fields):** Version JOIN: chaque filtre/projection ajoute `SIZE_KEY` + taille du type; gère `reference/object` via `_calculate_object_size`; ajoute overhead d'emboîtement.
- **calculate_selectivity(collection, filter_fields):** Heuristiques de sélectivité selon collection et noms des champs (`Stock(IDP/IDW)`, `Product(brand/IDP)`, `OrderLine(date/IDC/IDP)`), sinon 0.01.
- **calculate_S(collection, filter_fields, sharding_key):** Retourne `1` si le filtre porte sur la clé de sharding, sinon `nb_servers` (broadcast).
- **\_get_nb_srv_working(S):** Constante `1` si `S=1`, sinon `50`.
- **\_calculate_ram_vol_total(S, has_index, size_doc, vol_RAM):** Total RAM cluster: si `S=1` → `vol_RAM`, sinon `50*vol_RAM + (S-50)*(index*1e6)`.
- **filter_with_sharding(...):** Opérateur filtre shardé. Calcule `S`, `res_q`, `(size_input,size_msg)`, `vol_RAM`, `vol_network`, `time`, `CO2`. Retourne métriques détaillées.
- **filter_without_sharding(...):** Délègue à `filter_with_sharding` avec `sharding_key=None` et marque l'opérateur (broadcast).
- **nested_loop_join_with_sharding(...):** JOIN en boucle imbriquée: calcule sélectivités des deux collections, `S1` (outer), `S2` (inner broadcast), `vol_RAM` (outer+inner), `vol_network` (requêtes outer+inner+résultats). Retourne métriques cumulées.
- **nested_loop_join_without_sharding(...):** Idem sans sharding → réutilise la version shardée et re-étiquette.
- **calculate_query_cost(query):** Router principal; redirige vers `_calculate_filter_cost`, `_calculate_join_cost` ou `_calculate_aggregate_cost` selon `query_type`.
- **\_calculate_filter_cost(query):** Implémente la formule complète d'un filtre, avec `vol_ram_total` cluster et sorties formatées pour l'UI.
- **\_calculate_join_cost(query):** Deux cas:
  - Collections imbriquées physiquement: combine tailles (`Op1+Op2`) et calcule coûts comme un filtre sur la collection parente.
  - Collections séparées: exécute `Op1` (filtre + projection de clé de jointure), corrige `nb_output1` via ratio `nb_docs_X/nb_docs_Y`, exécute `Op2` (lookup clé, forcé `S=1`), puis combine les métriques (`vol_network_total`, `vol_ram_total_combined`, `time`, `CO2`, `budget`); attache résultats détaillés `op1`/`op2`.
- **\_calculate_aggregate_cost(query):** Cas agrégat + `GROUP BY`: estime le nombre de groupes, calcule `(size_input,size_msg)`, volumes réseau et RAM (stockage intermédiaire des docs filtrés), coûts et sorties formatées.

> Note: des helpers locaux `format_scientific()` existent à l'intérieur de certaines méthodes pour le formatage; ils ne sont pas exposés au module.

## query_stats_app/app.py

- **get_nb_srv_working(S):** Constante UI: 1 si `S=1`, sinon 50.
- **calculate_ram_vol_total(S, vol_ram_per_server, has_index=True):** Calcul UI du total RAM cluster: si `S=1` → `vol_ram_per_server`, sinon `50*vol_ram_per_server + (S-50)*(index*1e6)`.
- **initialize_session_state():** Prépare `st.session_state.manual_overrides` pour les overrides utilisateur (types, comptes, tailles, valeurs de requête/coûts).
- **main():** Orchestration Streamlit: configuration, saisie, parsing via `parse_query()`, calcul via `QueryCostCalculator`, affichage des tableaux/éditeurs et métriques.

---

## Exemple rapide (filtre)

- Requête: `SELECT name, date FROM Product WHERE IDP = $IDP`
- `QueryParser` infère types, `QueryCostCalculator.calculate_query_sizes()` donne `size_input` (clés+valeurs filtre + clés de projection) et `size_msg` (valeurs projetées + clés).
- Sélectivité: `1/nb_products`. Résultats `res_q = sel * nb_docs`.
- RAM par serveur: `index*1e6 + (res_q/S)*size_doc`.
- RAM totale cluster: si `S>1` → `50*ram_local + (S-50)*(index*1e6)`.
- Réseau: `S*size_input + res_q*size_msg`.
- Temps: `réseau/BW_net + RAM/BW_ram`; CO2 et budget proportionnels.
