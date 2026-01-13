# Query Cost Analysis (TD2)

This document explains how the query cost analysis system works for distributed NoSQL databases. It calculates the performance costs of executing queries across sharded databases with different denormalisation strategies.

---

## Overview

When executing a query in a distributed database, performance depends on several factors:
- **Network traffic:** Data transferred between servers and client
- **Server count (S):** How many servers must process the query
- **RAM usage:** Memory needed for query processing
- **Selectivity:** Proportion of documents matching the filters

This tool calculates these metrics to compare database designs (DB0-DB5) and evaluate sharding strategies.

---

## How It Works

### 1. Query Parsing

The system parses SQL-like queries to extract:
- **Collection:** Which collection/table to query (e.g., `Stock`, `Product`)
- **Filter fields:** WHERE clause conditions (e.g., `IDP = 1`, `brand = 'Apple'`)
- **Project fields:** SELECT clause fields (e.g., `quantity`, `location`)

### 2. Schema Loading

For each database design, the system loads JSON schemas containing:
- **Entities:** Top-level collections with their fields and types
- **Nested entities:** Embedded objects within collections
- **Field types:** `integer`, `string`, `date`, `longstring`, array types, embedded objects

This enables accurate type inference for all fields, including deeply nested ones.

### 3. Collection Mapping

Since some designs embed collections within others, the system maps logical to physical collections:

```
DB2: Stock → Product (Stock embedded in Product)
DB3: Product → Stock (Product embedded in Stock)
DB4: Product → OrderLine (Product embedded in OrderLine)
DB5: Stock → Product, OrderLine → Product (multiple embeddings)
```

When you query `Stock` in DB2, you're actually querying `Product` documents.

### 4. Size Calculations

#### Input Size (`size_input`)

Data sent from client to servers:
```
size_input = Σ(12 + filter_value_size) +   Σ(12 + 8)   +    12
            └─────── filters ────────┘  └ projections ┘ └ nesting ┘
```

**Field sizes:** `integer/number`=8B, `string`=80B, `date`=20B, `longstring`=200B, `key`=12B

#### Message Size (`size_msg`)

Data returned from servers to client:
```
size_msg = Σ(12 + actual_field_size)
```

The system uses schema information to determine actual field types, not just boolean flags. For embedded objects (like `price` containing `amount`, `currency`, `tax`), it uses the `Sizer` class to recursively calculate sizes.

### 5. Selectivity Calculation

Proportion of documents matching the query. For each filter field, calculate `1 / distinct_values`, then multiply them together:

```
sel_att = (1 / distinct_IDP) × (1 / distinct_IDW) × ...
```

**Example:** `WHERE IDP = 1 AND IDW = 2`
- `IDP` has 1000 distinct values → `1/1000 = 0.001`
- `IDW` has 10 distinct values → `1/10 = 0.1`
- **Selectivity:** `0.001 × 0.1 = 0.0001` (0.01% of documents match)

**Important:** The system accounts for physical vs logical collections. In DB2, filtering `Stock` by `IDP = 1` actually filters `Product` documents, so selectivity is `1/nb_products`, not `1/(nb_products × nb_warehouses)`.

### 6. Server Count (S)

Number of servers processing the query:
```
S = 1 if sharding_key in filter_fields
S = total_servers otherwise (broadcast query)
```

This is the **most critical factor** for performance. Wrong sharding can cause 100-1000× performance degradation.

### 7. Derived Metrics

```
vol_network = S × size_input + res_q × size_msg
vol_RAM = index_size + sel_att × docs_per_server × doc_size
time_network = vol_network / 1 GB/s
time_RAM = vol_RAM / 25 GB/s
carbon_footprint = based on network and RAM volumes
```

### 8. Statistics and Constants

The system uses realistic hardware performance metrics:

| Metric | Value |
|--------|-------|
| **Network Bandwidth** | 1 GB/s |
| **RAM Bandwidth** | 25 GB/s |
| **Index Size** | 1 MB |
| **Total Servers** | 1000 |

---

## The Importance of Good Sharding

**Query:** `SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = 1 AND S.IDW = 2`

This query filters by product ID (`IDP`) and warehouse ID (`IDW`), then returns two fields (quantity and location).

### Good Sharding (by IDP)

**[DB0, Sharding: IDP, Index: true]**

```
S = 1 (routes to single server)
res_q = 1
vol_network = 204 B
time_total = 0.322 ms
```

The query goes directly to the server containing `IDP = 1`. Fast and efficient.

### Bad Sharding (by IDC - Customer ID)

**[DB0, Sharding: IDC, Index: true]**

```
S = 1000 (broadcasts to all servers)
res_q = 1
vol_network = 92,112 B
time_total = 1.06 ms
```

The query must be sent to all 1000 servers because `IDC` isn't in the filter. **451× more network traffic.**

### Why This Matters

| Metric | Good Sharding | Bad Sharding | Increase |
|--------|---------------|--------------|----------|
| Servers contacted | 1 | 1000 | **1000×** |
| Network volume | 204 B | 92 KB | **451×** |
| Time | 0.322 ms | 1.06 ms | **3.3×** |

**Key insight:** Same query, same result (1 document), but choosing the wrong sharding key causes:
- **1000× more servers** to process the query
- **451× more network bandwidth** consumed
- **3.3× longer execution time**

**At scale:** For 1 million such queries, bad sharding wastes **92 GB** of network bandwidth vs only **204 MB** with good sharding.

---

## Key Lessons

### 1. Sharding Key Selection is Critical

Choosing the right sharding key can make the difference between routing to 1 server or broadcasting to all 1000 servers.

**Impact of wrong sharding:**
- 1000× more servers contacted
- 100-500× more network bandwidth consumed
- 3-10× longer execution time

**Best practice:** Shard by the fields most commonly used in WHERE clauses. In our example, sharding by `IDP` is optimal because most queries filter by product ID.

### 2. Collection Mapping Handles Embedded Collections

The system automatically resolves logical to physical collections when data is embedded:
- Querying `Stock` in DB2 → actually queries `Product` collection
- Selectivity calculations adjust accordingly
- Result counts remain correct despite different physical schema

This allows comparing different database designs fairly, even when collections are embedded differently.

---

## Implementation Details

### Core Functions

The system uses two main components for query analysis:

**`parse_query(sql, db_signature)` (in `services/query_parser.py`)**
- Parses SQL-like queries into structured format
- Extracts collection name from `FROM` clause (e.g., `Stock S` → collection: `Stock`)
- Extracts filter fields from `WHERE` clause with their types (e.g., `IDP = 1` → `{name: "IDP", type: "number"}`)
- Extracts project fields from `SELECT` clause (e.g., `quantity, location`)
- Supports table aliases and infers field types from JSON schema

The query cost calculator (`services/query_cost.py`) then uses the parsed query:

**`calculate_query_sizes(collection, filter_fields, project_fields)`**
- Loads JSON schema to infer actual field types
- Calculates `size_input` (data sent to servers) and `size_msg` (data returned)
- Uses `Sizer` class for embedded objects like `price` (with nested `amount`, `currency`, `tax`)

**`calculate_selectivity(collection, filter_fields)`**
- Resolves physical collection (e.g., `Stock` → `Product` in DB2)
- Multiplies selectivity of each filter field: `(1/distinct_IDP) × (1/distinct_IDW)`
- Adjusts for embedded collections to ensure correct result counts

**`calculate_S(collection, filter_fields, sharding_key)`**
- Returns `S = 1` if sharding key appears in filters (direct routing)
- Returns `S = 1000` otherwise (broadcast to all servers)
- This single calculation determines most of the performance difference

### Collection Mapping

The `COLLECTION_MAPPING` dictionary maps logical to physical collections for embedded designs:

- **DB2:** Stock/Categories/Supplier → Product
- **DB3:** Product/Categories/Supplier → Stock  
- **DB4:** Product/Categories/Supplier → OrderLine
- **DB5:** OrderLine/Stock/Categories/Supplier → Product

This allows the system to query the correct physical collection and calculate accurate selectivity.

---

## API Usage

### Start the Server

```bash
uv run fastapi dev app/main.py
```

The API will be available at **http://127.0.0.1:8000**

Open **http://127.0.0.1:8000/docs** for all of the endpoints

### Make a Request

**Endpoint:** `GET /TD2/queryCalculateCost`

**Request Body:**
```json
{
  "database": "DB1",
  "query": "SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = 1 AND S.IDW = 2",
  "sharding_key": "IDP"
}
```

**Response Example:**
```json
{
  "message": "Query cost calculated successfully!",
  "sql": "SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW;",
  "db_signature": "DB1",
  "collection_size_file": "results_TD1.json",
  "parsed_query": {
    "collection": "Stock",
    "filter_fields": [
      {
        "name": "IDP",
        "type": "number"
      },
      {
        "name": "IDW",
        "type": "number"
      }
    ],
    "project_fields": [
      {
        "name": "quantity",
        "type": "boolean"
      },
      {
        "name": "location",
        "type": "boolean"
      }
    ]
  },
  "cost_analysis": {
    "sizes": {
      "size_input_bytes": "92 B",
      "size_msg_bytes": "112 B",
      "size_doc_bytes": "152 B"
    },
    "distribution": {
      "S_servers": 1,
      "selectivity": "5.00e-08",
      "res_q_results": 1,
      "nb_docs_total": 20000000,
      "nb_docs_per_server": 20000
    },
    "volumes": {
      "vol_network": "2.04e+02 B",
      "vol_RAM": "1.00e+06 B"
    },
    "costs": {
      "time": "3.22e-04 s",
      "carbon_network": "2.24e-09 gCO2",
      "carbon_RAM": "2.80e-05 gCO2",
      "carbon_total": "2.80e-05 gCO2"
    }
  }
}
```
