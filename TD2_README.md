# TD2 - Query Cost Analysis

## Overview

This module implements **Chapter 3 (Filter Queries)** from the Big Data Structure course. It calculates execution costs for database queries in distributed NoSQL systems using formulas from `formulas_TD2.tex`.

For each query, the system computes:
- **S**: Number of servers accessed
- **Network volume**: Data transferred over network
- **RAM volume**: Data processed in memory
- **Time cost**: Query execution time
- **Carbon footprint**: Environmental impact (gCO2)

---

## Architecture

### Core Components

**`services/statistics.py`**
- Contains all constants and dataset statistics
- TD2 constants: bandwidth, CO2 coefficients, default index size
- Dataset stats: number of clients, products, order lines, etc.

**`services/query_cost.py`**
- Main calculation engine (`QueryCostCalculator` class)
- Implements all formulas from `formulas_TD2.tex`
- Loads collection info from `results_TD1.json`

**`test_query_cost.py`**
- Test file with examples for Q1, Q2, Q3
- Demonstrates how to use the calculator

---

## Calculation Logic

### 1. Query Size Calculation

#### Input Size (Query Sent to Servers)
```
size_input = Σ(12 + size(value_i)) + 12 × (nesting levels)
```

**Components:**
- **Filter fields**: Each field has key overhead (12 B) + value size
- **Project fields**: Each field has key overhead (12 B) + boolean (8 B)
- **Nesting overhead**: 12 B for collection wrapper

**Value sizes** (from `Statistics`):
- `integer/boolean`: 8 B
- `string`: 80 B
- `date`: 20 B
- `longstring`: 200 B
- `key overhead`: 12 B

**Example Q1:**
```python
filter_fields = [{"name": "IDP", "type": "integer"}, {"name": "IDW", "type": "integer"}]
project_fields = [{"name": "quantity", "type": "boolean"}, {"name": "location", "type": "boolean"}]

# Calculation:
# Filters: 2 × (12 + 8) = 40 B
# Projects: 2 × (12 + 8) = 40 B
# Nesting: 12 B
# Total: 92 B
```

#### Output Size (Result Message per Document)
```
size_msg = Σ(12 + size(value_j)) for each projected field
```

**Example Q1:**
```python
# quantity: 12 + 8 = 20 B
# location: 12 + 8 = 20 B
# Total: 40 B per result document
```

---

### 2. Selectivity Calculation

**Selectivity (sel_att)**: Fraction of documents matching the filter (0 to 1)

**Automatic calculation based on filter:**

| Collection | Filter | Formula | Example |
|------------|--------|---------|---------|
| Stock | IDP + IDW | `1 / (nb_products × nb_warehouses)` | 1/20M = 0.00000005 |
| Stock | IDP only | `1 / nb_products` | 1/100k = 0.00001 |
| Product | brand | `nb_apple_products / nb_products` | 50/100k = 0.0005 |
| OrderLine | date | `1 / nb_days` | 1/365 = 0.0027 |
| OrderLine | IDC | `1 / nb_clients` | 1/10M = 0.0000001 |

**Result count:**
```
res_q = selectivity × total_documents
```

---

### 3. Server Access (S)

**Formula:**
```
S = 1  if filtering on sharding key
S = #shards  otherwise
```

**Sharding decision:**
- If query filters on the **sharding key** → only 1 server needed
- Otherwise → must query **all 1,000 servers**

**Examples:**
- Q1: Stock filtered by IDP (shard key) → **S = 1**
- Q2: Product filtered by brand (not shard key) → **S = 1000**
- Q3: OrderLine filtered by date (not shard key) → **S = 1000**

---

### 4. RAM Volume (Data Processed)

**Formula (with index):**
```
vol_RAM(q,n) = index_q + sel_att × coll_q,n × size_doc
```

**Formula (full scan, no index):**
```
vol_RAM(q,n) = 1 × coll_q,n × size_doc
```

**Components:**
- `index_q`: Index size (~1 MB default per server)
- `sel_att`: Selectivity (fraction of docs matching)
- `coll_q,n`: Number of documents on server n
- `size_doc`: Average document size (from `results_TD1.json`)

**Index usage:**
- If `has_index=True` → use index lookup (add 1 MB, apply selectivity)
- If `has_index=False` → full scan (sel_att = 1, scan all docs)

**Parallelism:**
```
vol_RAM(q) = max(vol_RAM(q,1), ..., vol_RAM(q,n))
```
Takes the max across all servers (bottleneck server determines time)

---

### 5. Network Volume

**Formula (filter queries):**
```
vol_network = S × size_input + res_q × size_msg
```

**Components:**
- `S × size_input`: Cost of sending query to S servers
- `res_q × size_msg`: Cost of receiving results back

**Example:**
- S = 1, size_input = 92 B, res_q = 1, size_msg = 40 B
- vol_network = 1 × 92 + 1 × 40 = 132 B

---

### 6. Time Cost

**Formula:**
```
time = vol_network / bandwidth_network + vol_RAM / bandwidth_RAM
```

**Constants:**
- `bandwidth_network`: 1 Gb/s = 125,000,000 bytes/s
- `bandwidth_RAM`: 25 Gb/s = 3,125,000,000 bytes/s

**Interpretation:**
- Time to transfer data over network + time to process in RAM
- Result in **seconds**

---

### 7. Carbon Footprint

**Formula:**
```
carbon_network = vol_network × CO2_network
carbon_RAM = vol_RAM × CO2_RAM
carbon_total = carbon_network + carbon_RAM
```

**Constants:**
- `CO2_network`: 1.10e-11 g CO2 per byte
- `CO2_RAM`: 2.80e-11 g CO2 per byte

**Result:** Environmental impact in **grams of CO2**

---

## How to Use `query_cost.py`

### Basic Usage

```python
from services.query_cost import QueryCostCalculator

# Initialize calculator with a DB signature
calc = QueryCostCalculator(db_signature="DB0")

# Define your query
query = {
    "collection": "Stock",
    "filter_fields": [
        {"name": "IDP", "type": "integer"},
        {"name": "IDW", "type": "integer"}
    ],
    "project_fields": [
        {"name": "quantity", "type": "boolean"},
        {"name": "location", "type": "boolean"}
    ],
    "sharding_key": "IDP",  # Optional: shard key for this collection
    "has_index": True,      # Optional: whether index exists (default: False)
    "index_size": 1000000   # Optional: index size in bytes (default: 1 MB)
}

# Calculate costs
result = calc.calculate_query_cost(query)

# Access results
print(f"Servers accessed: {result['distribution']['S']}")
print(f"Network volume: {result['volumes']['vol_network_bytes']} bytes")
print(f"Time cost: {result['costs']['time_seconds']} seconds")
print(f"Carbon: {result['costs']['carbon_total_gCO2']} gCO2")
```

### Query Structure

**Required fields:**
- `collection`: Collection name (e.g., "Stock", "Product", "OrderLine")
- `filter_fields`: List of `{"name": str, "type": str}` for WHERE conditions
- `project_fields`: List of `{"name": str, "type": str}` for SELECT fields

**Optional fields:**
- `sharding_key`: Attribute used for sharding (e.g., "IDP", "IDC")
- `has_index`: Boolean indicating if an index exists on filter attribute
- `index_size`: Index size in bytes (defaults to 1 MB if `has_index=True`)

### Field Types

Supported types for filter/project fields:
- `"integer"` → 8 bytes
- `"boolean"` → 8 bytes
- `"string"` → 80 bytes
- `"date"` → 20 bytes
- `"longstring"` → 200 bytes

### Result Structure

```python
{
    "query": {
        "collection": str,
        "db_signature": str,
        "filter_fields": List[Dict],
        "project_fields": List[Dict],
        "sharding_key": str,
        "has_index": bool
    },
    "sizes": {
        "size_input": int,      # Query input size (bytes)
        "size_msg": int,        # Output message size per doc (bytes)
        "size_doc": int         # Average document size (bytes)
    },
    "distribution": {
        "S": int,               # Number of servers accessed
        "selectivity": float,   # Fraction of docs matching (0-1)
        "res_q": int,           # Number of result documents
        "nb_docs_total": int,   # Total documents in collection
        "nb_docs_per_server": float  # Avg docs per server
    },
    "volumes": {
        "vol_network_bytes": float,  # Network data transferred
        "vol_RAM_bytes": float       # RAM data processed
    },
    "costs": {
        "time_seconds": float,           # Execution time
        "carbon_network_gCO2": float,    # Network carbon impact
        "carbon_RAM_gCO2": float,        # RAM carbon impact
        "carbon_total_gCO2": float       # Total carbon footprint
    }
}
```

---

## Example Queries

### Q1: Stock Lookup (Very Selective, Shard Key)
```python
query = {
    "collection": "Stock",
    "filter_fields": [
        {"name": "IDP", "type": "integer"},
        {"name": "IDW", "type": "integer"}
    ],
    "project_fields": [
        {"name": "quantity", "type": "boolean"},
        {"name": "location", "type": "boolean"}
    ],
    "sharding_key": "IDP",
    "has_index": True
}
```
**Expected:** S=1, very low selectivity, minimal network/RAM cost

### Q2: Product by Brand (Medium Selectivity, No Shard Key)
```python
query = {
    "collection": "Product",
    "filter_fields": [
        {"name": "brand", "type": "string"}
    ],
    "project_fields": [
        {"name": "name", "type": "boolean"},
        {"name": "price", "type": "boolean"}
    ],
    "sharding_key": "IDP",  # Not filtering on shard key
    "has_index": True
}
```
**Expected:** S=1000, medium selectivity (~50 products), higher network cost

### Q3: OrderLine by Date (Low Selectivity, No Shard Key)
```python
query = {
    "collection": "OrderLine",
    "filter_fields": [
        {"name": "date", "type": "date"}
    ],
    "project_fields": [
        {"name": "IDP", "type": "boolean"},
        {"name": "quantity", "type": "boolean"}
    ],
    "sharding_key": "IDC",  # Not filtering on shard key
    "has_index": True
}
```
**Expected:** S=1000, high result count (~11M docs), very high network/RAM cost

---

## Constants Reference

All constants are defined in `services/statistics.py`:

### TD2 Query Cost Constants
- `BANDWIDTH_NETWORK = 125_000_000` (1 Gb/s)
- `BANDWIDTH_RAM = 3_125_000_000` (25 Gb/s)
- `CO2_NETWORK = 1.10e-11` (g CO2 per byte)
- `CO2_RAM = 2.80e-11` (g CO2 per byte)
- `DEFAULT_INDEX_SIZE = 1_000_000` (~1 MB)

### Data Type Sizes
- `SIZE_KEY = 12` (key overhead)
- `SIZE_INTEGER = 8`
- `SIZE_NUMBER = 8`
- `SIZE_STRING = 80`
- `SIZE_DATE = 20`
- `SIZE_LONGSTRING = 200`

### Dataset Statistics
- `nb_clients = 10^7` (10 million)
- `nb_products = 10^5` (100,000)
- `nb_orderlines = 4 × 10^9` (4 billion)
- `nb_warehouses = 200`
- `nb_servers = 1000`
- `nb_distinct_brands = 5000`
- `nb_apple_products = 50`
- `nb_days = 365`

---

## Testing

Run the test suite:
```bash
python test_query_cost.py
```

This will execute all three example queries (Q1, Q2, Q3) and display:
- Query details
- Size calculations
- Distribution metrics
- Volume calculations
- Cost results (time and carbon)

---

## Formulas Reference

All formulas are documented in `formulas_TD2.tex`. Key formulas:

**Query Sizes:**
- size_input = Σ(12 + size(value)) + 12
- size_msg = Σ(12 + size(value))

**Volumes:**
- vol_network = S × size_input + res_q × size_msg
- vol_RAM = index_q + sel_att × coll × size_doc

**Costs:**
- time = vol_network / BW_network + vol_RAM / BW_RAM
- carbon = vol_network × CO2_network + vol_RAM × CO2_RAM

**Server Access:**
- S = 1 (if filtering on shard key) or #shards (otherwise)

---

## Notes

- **Filter queries only**: This implementation handles filter queries (WHERE clauses), not join queries
- **Single query cost**: No frequency multipliers - calculates cost for one execution
- **Automatic selectivity**: Calculates selectivity based on filter conditions and dataset stats
- **Default index size**: Uses 1 MB if not specified (based on course material)
- **DB signatures**: Supports DB0-DB5 from TD1 (different denormalization strategies)

---

## Future Enhancements

Potential additions:
- API endpoint for query cost analysis
- Support for aggregate queries
- Query parsing from SQL/MongoDB syntax
- Batch query analysis
- Cost comparison across different DB signatures
- Visualization of cost breakdowns
