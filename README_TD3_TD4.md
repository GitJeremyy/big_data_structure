# TD3 and TD4: Query Cost Analysis System

This document explains the query cost analysis system, covering query parsing, cost calculation, statistics computation, and the application interface with manual override capabilities.

## Table of Contents

1. [Query Parsing](#query-parsing)
2. [Query Cost Calculation](#query-cost-calculation)
3. [Statistics Calculation](#statistics-calculation)
4. [Application Interface](#application-interface)

---

## Query Parsing

The query parsing system (`services/query_parser.py`) converts SQL queries into structured dictionaries that can be used for cost analysis. The parser supports three main query types: **filter queries**, **join queries**, and **aggregate queries**.

### Filter Query Parsing

Filter queries are simple SELECT-FROM-WHERE queries without joins or aggregates.

**Process:**

1. **Query Type Detection**: The parser checks for aggregate functions (SUM, COUNT, AVG, etc.), GROUP BY clauses, and JOIN keywords to determine the query type.

2. **SELECT Clause Parsing** (`_parse_select_fields`):
   - Extracts field names from the SELECT clause
   - Removes table aliases (e.g., `S.quantity` → `quantity`)
   - Infers field types from the JSON schema using `infer_type()`
   - Returns a list of dictionaries: `[{"name": "field1", "type": "integer"}, ...]`

3. **FROM Clause Parsing**:
   - Extracts the collection name
   - Handles optional table aliases

4. **WHERE Clause Parsing** (`_parse_where_clause`):
   - Splits conditions by `AND` operators
   - Parses equality conditions (e.g., `field = value` or `field = $param`)
   - Infers field types from schema
   - Returns a list of filter field dictionaries

5. **Type Inference**:
   - Uses the JSON schema loaded for the database signature (DB1-DB5)
   - Builds a field type lookup: `{collection_name: {field_name: type}}`
   - Defaults to `'integer'` if field not found in schema

**Example:**
```sql
SELECT quantity, location FROM Stock WHERE IDP = 1 AND IDW = 2
```

**Parsed Result:**
```python
{
    'query_type': 'filter',
    'collection': 'Stock',
    'filter_fields': [
        {'name': 'IDP', 'type': 'integer'},
        {'name': 'IDW', 'type': 'integer'}
    ],
    'project_fields': [
        {'name': 'quantity', 'type': 'integer'},
        {'name': 'location', 'type': 'string'}
    ]
}
```

### Join Query Parsing

Join queries involve multiple collections connected by JOIN conditions.

**Process:**

1. **FROM/JOIN Clause Parsing** (`_parse_from_join_clause`):
   - Extracts all collections involved in the join
   - Parses table aliases (e.g., `Stock S`, `Product P`)
   - Extracts JOIN conditions: `ON T1.field1 = T2.field2`
   - Resolves collection names from aliases
   - Returns:
     - `collections`: List of collection names
     - `aliases`: Dictionary mapping aliases to collections
     - `join_conditions`: List of join condition dictionaries

2. **SELECT Fields with Collections** (`_parse_select_fields_with_collections`):
   - Handles qualified field names (e.g., `S.quantity`, `P.name`)
   - Resolves which collection each field belongs to using aliases
   - Infers types per collection
   - Returns fields with `collection` attribute: `[{"collection": "Stock", "name": "quantity", "type": "integer"}, ...]`

3. **WHERE Clause with Collections** (`_parse_where_clause_with_collections`):
   - Similar to filter parsing but maintains collection context
   - Handles qualified field names in WHERE conditions
   - Returns filter fields with `collection` attribute

**Example:**
```sql
SELECT P.name, S.quantity FROM Stock S JOIN Product P ON S.IDP = P.IDP WHERE S.IDW = $IDW
```

**Parsed Result:**
```python
{
    'query_type': 'join',
    'collections': ['Stock', 'Product'],
    'aliases': {'S': 'Stock', 'P': 'Product'},
    'join_conditions': [{
        'left_collection': 'Stock',
        'left_field': 'IDP',
        'right_collection': 'Product',
        'right_field': 'IDP'
    }],
    'filter_fields': [{
        'collection': 'Stock',
        'name': 'IDW',
        'type': 'integer'
    }],
    'project_fields': [
        {'collection': 'Product', 'name': 'name', 'type': 'string'},
        {'collection': 'Stock', 'name': 'quantity', 'type': 'integer'}
    ]
}
```

### Aggregate Query Parsing

Aggregate queries include GROUP BY and aggregate functions (SUM, COUNT, AVG, etc.).

**Process:**

1. **Aggregate Function Parsing** (`_parse_aggregate_functions`):
   - Matches patterns like `SUM(field)`, `COUNT(T.field)`, `AVG(field) AS alias`
   - Extracts function name, field name, collection, and optional alias
   - Infers field type for the aggregate result

2. **GROUP BY Parsing** (`_parse_group_by_clause`):
   - Extracts fields in GROUP BY clause
   - Handles qualified field names
   - Returns list of group-by field dictionaries

3. **Query Type Determination**:
   - If JOIN + aggregates → `join_aggregate`
   - If aggregates only → `aggregate`

---

## Query Cost Calculation

The query cost calculator (`services/query_cost.py`) implements cost formulas from `formulas_TD2.tex` to compute execution costs for queries.

### Main Entry Point: `calculate_query_cost()`

The main method routes queries to appropriate cost calculation methods based on query type:

```python
def calculate_query_cost(self, query: Dict) -> Dict:
    query_type = query.get("query_type", "filter")
    
    if query_type == "join" or query_type == "join_aggregate":
        return self._calculate_join_cost(query)
    elif query_type == "aggregate":
        return self._calculate_aggregate_cost(query)
    else:
        return self._calculate_filter_cost(query)
```

### Filter Query Cost Calculation

**Process (`_calculate_filter_cost`):**

1. **Collection Resolution**:
   - Maps logical collection names to physical collections (handles embedded collections)
   - Example: In DB2, `Stock` maps to `Product` (Stock is embedded in Product)

2. **Selectivity Calculation** (`calculate_selectivity`):
   - Estimates the fraction of documents matching the filter
   - Uses heuristics based on filter field names:
     - `IDP + IDW` on Stock: `1 / (nb_products × nb_warehouses)`
     - `IDP` only: `1 / nb_products`
     - `brand = "Apple"`: `nb_apple_products / nb_products`

3. **Server Count (S)** (`calculate_S`):
   - If filtering on sharding key: `S = 1` (single server)
   - Otherwise: `S = 1000` (all servers)

4. **Query Size Calculation** (`calculate_query_sizes`):
   - **size_input** (query size):
     - Filter values: `filter_ints × 8 + filter_strings × 80 + filter_dates × 20`
     - Projection keys: `(proj_ints + proj_strings + proj_dates) × 8`
     - Key overhead: `nb_keys × 12`
     - Total: `filter_size + proj_keys_size + key_costs`
   
   - **size_msg** (output size):
     - Projection values: `proj_ints × 8 + proj_strings × 80 + proj_dates × 20`
     - Key overhead: `(proj_ints + proj_strings + proj_dates) × 12`
     - Total: `proj_values_size + proj_key_costs`

5. **Volume Calculations**:
   - **Network Volume**: `vol_network = S × size_input + res_q × size_msg`
   - **RAM Volume (per server)**: `vol_RAM = index × 1MB + (res_q/S) × size_doc`
   - **RAM Volume (total)**: 
     - If `S = 1`: `vol_ram_total = vol_RAM`
     - If `S > 1`: `vol_ram_total = nb_srv_working × vol_RAM + (S - nb_srv_working) × index × 1MB`
     - Where `nb_srv_working = 1` if `S=1`, else `50`

6. **Cost Calculations**:
   - **Time**: `time = vol_network / BANDWIDTH_NETWORK + vol_RAM / BANDWIDTH_RAM`
   - **CO2**: `carbon_total = vol_network × CO2_NETWORK + vol_ram_total × CO2_RAM`
   - **Budget**: `budget = vol_network × NETWORK_PRICE`

### Join Query Cost Calculation

**Process (`_calculate_join_cost`):**

Join queries are calculated as a two-phase operation:

1. **Embedded Collection Check**:
   - If both collections map to the same physical collection (embedded case):
     - Treats as a single filter query on the parent collection
     - Combines sizes from both collections
     - Uses combined filters and projections

2. **Separate Collections (Two-Phase Join)**:

   **Op1: Filter on Driving Collection (collection1)**
   - Calculates filter cost on collection1 with its filters
   - Projects join key + other fields from collection1
   - Returns `nb_output1` results

   **Op2: Point Lookup on Other Collection (collection2)**
   - For each result from Op1, performs a point lookup on collection2
   - Filters on join key (e.g., `Product.IDP = Stock.IDP`)
   - Uses `S = 1` semantics (routing by join key)
   - Executed `nb_iter_op2 = nb_output1` times

   **Combined Metrics**:
   - `vol_network_total = vol_net1 + nb_iter_op2 × vol_net2_single`
   - `vol_ram_total_combined = vol_ram_total1 + nb_iter_op2 × vol_ram_total2_single`
   - `time_total = vol_network_total / BANDWIDTH_NETWORK + (vol_ram1 + nb_iter_op2 × vol_ram2_single) / BANDWIDTH_RAM`
   - `carbon_total = vol_network_total × CO2_NETWORK + vol_ram_total_combined × CO2_RAM`

### Aggregate Query Cost Calculation

**Process (`_calculate_aggregate_cost`):**

1. **Group Count Estimation**:
   - Estimates number of groups based on GROUP BY fields
   - Uses selectivity heuristics for distinct values

2. **Output Size**:
   - Includes projection fields + aggregate function results
   - Output is groups, not individual documents

3. **Cost Calculation**:
   - Similar to filter queries but uses `nb_groups` instead of `res_q`
   - Network volume: `S × size_input + nb_groups × size_msg`

---

## Statistics Calculation

The statistics calculation module (`services/calculate_stats.py`) extracts and formats query characteristics and cost breakdowns for display.

### Query Characteristics Extraction

**Function: `extract_query_characteristics(parsed_query, calculator)`**

This function extracts metrics for display in the Query Characteristics table.

**For Filter Queries:**

1. **Field Counts by Type**:
   - **Filter counts**: Uses `extract_field_counts_by_type()` which:
     - Infers types from schema using `calculator.infer_type()`
     - Counts fields by type: `{"integer": count, "string": count, "date": count}`
   - **Projection counts**: Uses `extract_projection_counts_by_type()` which:
     - Uses types from parsed query (already inferred by parser)
     - Counts projection fields by type

2. **Size Calculation**:
   - Calls `calculator.calculate_query_sizes()` to get:
     - `query_size` (size_input): Filter + projection keys + key overhead
     - `output_size` (size_msg): Projection values + key overhead

3. **Key Count**:
   - `nb_keys = len(filter_fields) + len(project_fields)`

**For Join Queries:**

1. **Aggregate Filter Counts**:
   - Iterates through filter fields from all collections
   - Aggregates counts by type across collections

2. **Aggregate Projection Counts**:
   - Counts all projection fields across collections

3. **Size Calculation**:
   - Uses `calculator.calculate_join_sizes()` which:
     - Accounts for nesting overhead
     - Handles embedded object sizes
     - Calculates input and output sizes for join queries

**For Aggregate Queries:**

1. **Field Counts**:
   - Includes aggregate function results in projection counts
   - Adds aggregate functions to key count

2. **Size Calculation**:
   - Includes aggregate function results in output size calculation

### Cost Breakdown Extraction

**Function: `extract_cost_breakdown(result, sharding_key, has_index)`**

Extracts cost metrics from the result dictionary returned by `calculate_query_cost()`.

**Process:**

1. **Extract Base Values**:
   - `S`: Number of servers
   - `size_input`, `size_msg`: Query and output sizes
   - `nb_output`: Number of result documents
   - `vol_network`, `vol_ram_total`: Volumes
   - `time_cost`, `co2_kg`, `budget`: Costs

2. **Calculate RAM Per Server**:
   - Uses `extract_ram_vol_per_server()`:
     - Formula: `index × 1MB + (res_q/S) × size_doc`

3. **Format Values**:
   - All values are extracted from formatted strings in result dict
   - Scientific notation formatting applied

4. **Join-Specific Metrics**:
   - If join query, extracts `S_servers_outer`, `S_servers_inner`
   - Includes `nb_iter_op2` for iteration count

5. **Aggregate-Specific Metrics**:
   - If aggregate query, includes `nb_groups` and `res_filtered`

---

## Application Interface

The Streamlit application (`query_stats_app/app.py`) provides an interactive interface for query cost analysis with comprehensive manual override capabilities.

### Application Flow

1. **Query Input**:
   - User enters SQL query in sidebar
   - Selects database signature (DB1-DB5)
   - Configures sharding type and keys
   - Enables/disables index

2. **Query Parsing**:
   - Calls `parse_query()` to convert SQL to structured dict
   - Displays query type and collections

3. **Manual Field Type Overrides** (if enabled):
   - User can edit field types via dropdowns
   - Changes stored in `st.session_state.manual_overrides['field_types']`
   - Applied to parsed query before cost calculation

4. **Query Characteristics Calculation**:
   - Calls `extract_query_characteristics()` to get field counts and sizes
   - Displays in editable table if overrides enabled

5. **Cost Calculation**:
   - For each sharding key, calls `calculator.calculate_query_cost()`
   - Extracts cost breakdown using `extract_cost_breakdown()`
   - Displays results in cost breakdown tables

6. **Manual Override Application**:
   - If overrides enabled, applies stored overrides to results
   - Recalculates derived values (volumes, costs)

### Manual Override System

The application supports comprehensive manual value overrides for fine-tuning calculations.

#### Override Types

1. **Field Type Overrides**:
   - Location: Sidebar dropdowns for filter and projection fields
   - Storage: `st.session_state.manual_overrides['field_types']`
   - Impact: Affects type inference and size calculations
   - Application: Applied to parsed query before characteristics calculation

2. **Query Characteristics Overrides**:
   - Location: "Edit Query Characteristics" section
   - Editable values:
     - Filter counts by type (integer, string, date)
     - Projection counts by type
     - `nb_keys`: Total number of keys
     - `query_size`: Query input size in bytes
     - `output_size`: Output message size in bytes
   - Storage: `st.session_state.manual_overrides['query_char_overrides']`
   - Impact: Directly affects query and output size calculations
   - Application: Applied before cost calculation

3. **Cost Overrides**:
   - Location: Cost Breakdown tables (Network and RAM sections)
   - Editable values:
     - **Network Section**: `S` (servers), `query size`, `output size`
     - **RAM Section**: `Index`, `Nb pointers per working`, `collection doc size`
   - Storage: `st.session_state.manual_overrides['cost_overrides_{idx}']`
   - Impact: Triggers automatic recalculation of derived values

#### Recalculation Process

When manual overrides are applied, the system recalculates all derived values:

1. **Network Volume Recalculation**:
   ```python
   vol_network = S × size_input + nb_output × size_msg
   ```

2. **RAM Volume Recalculation**:
   ```python
   nb_pointers = nb_output / S  # if S > 0
   vol_ram_per_server = index × 1MB + nb_pointers × size_doc
   vol_ram_total = calculate_ram_vol_total(S, vol_ram_per_server, has_index)
   ```

3. **Time Cost Recalculation**:
   ```python
   time_cost = vol_network / BANDWIDTH_NETWORK + vol_ram_per_server / BANDWIDTH_RAM
   ```

4. **CO2 Recalculation**:
   ```python
   co2_kg = vol_network × CO2_NETWORK + vol_ram_total × CO2_RAM
   ```

5. **Budget Recalculation**:
   ```python
   budget = vol_network × NETWORK_PRICE
   ```

#### Override Persistence

- Overrides are stored in Streamlit's session state
- Persist across "Calculate Costs" button clicks
- Can be cleared using "🔄 Clear All Overrides" button
- Applied automatically on next calculation

#### Interaction Flow

1. **Enable Overrides**: Check "Allow Manual Value Overrides" in sidebar

2. **Edit Values**: 
   - Edit field types in sidebar
   - Edit query characteristics in main area
   - Edit cost values in cost breakdown tables

3. **Save Changes**: 
   - Changes are automatically saved to session state
   - Message displayed: "💡 Changes saved. Click 'Calculate Costs' again to see updated results."

4. **Recalculate**: 
   - Click "Calculate Costs" button
   - System applies all stored overrides
   - Recalculates all derived values
   - Displays updated results

### Integration Points

The application integrates with the services layer through these key functions:

- **`parse_query()`**: Converts SQL to structured dict
- **`QueryCostCalculator.calculate_query_cost()`**: Computes query costs
- **`extract_query_characteristics()`**: Extracts query metrics
- **`extract_cost_breakdown()`**: Formats cost results

All manual overrides are applied at the application layer, ensuring the services layer remains pure and testable.

---

## Summary

The TD3 and TD4 system provides a complete query cost analysis pipeline:

1. **Parsing**: SQL queries are parsed into structured dictionaries with type inference
2. **Cost Calculation**: Costs are computed using formulas from `formulas_TD2.tex`
3. **Statistics**: Query characteristics and cost breakdowns are extracted for display
4. **Interface**: Interactive Streamlit app with manual override capabilities for fine-tuning

The system supports filter queries, join queries, and aggregate queries across different database configurations (DB1-DB5) with comprehensive sharding and indexing strategies.


