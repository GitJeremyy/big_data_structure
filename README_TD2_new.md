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
size_input = Σ(12 + filter_value_size) + Σ(12 + 8) + 12
             └─────── filters ────────┘   └─ projections ─┘  └ nesting ┘
```

**Field sizes:** `integer/number`=8B, `string`=80B, `date`=20B, `longstring`=200B, `key`=12B

#### Message Size (`size_msg`)

Data returned from servers to client:
```
size_msg = Σ(12 + actual_field_size)
```

The system uses schema information to determine actual field types, not just boolean flags. For embedded objects (like `price` containing `amount`, `currency`, `tax`), it uses the `Sizer` class to recursively calculate sizes.

### 5. Selectivity Calculation

Proportion of documents matching the query:
```
sel_att = Π(1 / distinct_values) for each filter field
```

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
time_network = vol_network / 125 MB/s
time_RAM = vol_RAM / 3125 MB/s
carbon_footprint = based on network and RAM volumes
```

---

## The Importance of Good Sharding

### Example 1: Simple Stock Lookup

**Query:** `SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = 1 AND S.IDW = 2`

This query filters by product ID (`IDP`) and warehouse ID (`IDW`), then returns two fields.

#### Good Sharding (by IDP)

```
S = 1 (routes to single server)
vol_network = 204 B
time_total = 0.322 ms
```

The query goes directly to the server containing `IDP = 1`. Fast and efficient.

#### Bad Sharding (by IDC - Customer ID)

```
S = 1000 (broadcasts to all servers)
vol_network = 92,112 B
time_total = 0.738 ms
```

The query must be sent to all 1000 servers because `IDC` isn't in the filter. **451× more network traffic.**

#### Why This Matters

| Metric | Good Sharding | Bad Sharding | Increase |
|--------|---------------|--------------|----------|
| Servers contacted | 1 | 1000 | **1000×** |
| Network volume | 204 B | 92 KB | **451×** |
| Time | 0.322 ms | 0.738 ms | **2.3×** |

**For 1 million queries:** Bad sharding wastes 92 GB of network bandwidth vs 204 MB with good sharding.

---

### Example 2: Product Search by Brand

**Query:** `SELECT P.name, P.price FROM Product P WHERE P.brand = 'Apple'`

This query searches for all Apple products.

#### Results Across Database Designs

| Database | Results | Network (Good) | Network (Bad) | Time (Good) | Time (Bad) |
|----------|---------|----------------|---------------|-------------|------------|
| DB0-DB3, DB5 | 50 | 12 KB | 12.8 MB | 1.57 ms | 3.77 s |
| **DB4** | **2,000,000** | **472 MB** | **472 GB** | **3.78 s** | **2.52 hours** |

#### What Happened in DB4?

DB4 embeds `Product` inside `OrderLine`. When you filter by `brand = 'Apple'`:
- Normal designs: Return 50 Apple products
- **DB4**: Returns every order line that contains an Apple product = 2 million results

**This is a 40,000× explosion** because denormalisation duplicated products across millions of order lines.

#### Sharding Impact

Even in well-designed databases (DB0-DB3, DB5):
- **Good sharding (by brand):** 12 KB network, 1.57 ms
- **Bad sharding (by date):** 12.8 MB network, 3.77 s (**2401× worse**)

---

### Example 3: Order Lines by Date

**Query:** `SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = '2024-01-01'`

This query finds all order lines from a specific date.

#### Results Across Database Designs

| Database | Results | Network (Good) | Network (Bad) | Time (Good) | Time (Bad) |
|----------|---------|----------------|---------------|-------------|------------|
| DB0-DB4 | 273 | 11 KB | 6.6 MB | 1.41 ms | 2.11 s |
| **DB5** | **11,000,000** | **440 MB** | **440 GB** | **3.52 s** | **1.47 hours** |

#### What Happened in DB5?

DB5 embeds `Product` (which contains `Stock[]`) inside `OrderLine`. Querying by date returns:
- Normal designs: 273 order lines
- **DB5**: 11 million results (273 orders × ~40,000 products with stock items)

**This is a 40,293× explosion** due to extreme denormalisation.

---

## Key Lessons

### 1. Sharding Key Choice is Critical

**Always include your sharding key in query filters when possible.**

- ✅ Good: Query routes to 1 server (S=1)
- ❌ Bad: Query broadcasts to all servers (S=1000)
- 📊 Impact: **100-1000× more network traffic with wrong sharding**

### 2. Denormalisation Has Trade-offs

**Embedding reduces joins but can explode result sets.**

- ✅ DB1: Duplicating fields in Stock (minor denormalisation) - works well
- ⚠️ DB4: Embedding Product in OrderLine - catastrophic for brand queries
- 🚨 DB5: Embedding Product+Stock in OrderLine - catastrophic for date queries

### 3. Design for Your Query Patterns

**Different designs excel at different queries.**

- Stock lookups by product: Any design works with good sharding
- Product searches by brand: Avoid DB4 at all costs
- OrderLine queries by date: Avoid DB5 at all costs

### 4. Collection Mapping Matters

**The system automatically handles embedded collections.**

When querying `Stock` in DB2 (where Stock is embedded in Product):
- Physical collection: `Product`
- Selectivity: Adjusted for Product count, not Stock count
- Result: Correct result count (1 product document with Stock array)

---

## Implementation Details

### Core Functions

#### `calculate_query_sizes(collection, filter_fields, project_fields)`

Calculates `size_input` and `size_msg`:
- Loads JSON schema for the database
- Infers actual field types (not just booleans)
- Handles embedded objects using `Sizer.estimate_document_size()`
- Returns tuple: `(size_input, size_msg)`

#### `calculate_selectivity(collection, filter_fields)`

Calculates proportion of documents matching filters:
- Resolves physical collection (handles embedded collections)
- Gets distinct value counts from statistics
- Adjusts for embedded cases (e.g., Stock in Product)
- Returns: `sel_att` (float between 0 and 1)

#### `calculate_S(collection, filter_fields, sharding_key)`

Determines server count:
- Checks if sharding key appears in filters
- Returns 1 if present, total servers otherwise
- Critical for performance estimation

#### `_calculate_object_size(collection, field_name)`

Calculates size of embedded objects:
- Uses `Sizer.estimate_document_size()` for recursion
- Handles nested structures (e.g., `price` object)
- Returns total size in bytes

### Collection Mapping Dictionary

```python
COLLECTION_MAPPING = {
    "DB2": {
        "Stock": "Product",
        "Categories": "Product",
        "Supplier": "Product"
    },
    "DB3": {
        "Product": "Stock",
        "Categories": "Stock",
        "Supplier": "Stock"
    },
    "DB4": {
        "Product": "OrderLine",
        "Categories": "OrderLine",
        "Supplier": "OrderLine"
    },
    "DB5": {
        "OrderLine": "Product",
        "Stock": "Product",
        "Categories": "Product",
        "Supplier": "Product"
    }
}
```

This maps logical collections to their physical storage location when embedded.

---

## API Usage

### Start the Server

```bash
uv run uvicorn app.main:app --reload
```

### Make a Request

```bash
curl -X POST http://localhost:8000/TD2 \
  -H "Content-Type: application/json" \
  -d '{
    "database": "DB0",
    "query": "SELECT S.quantity FROM Stock S WHERE S.IDP = 1",
    "sharding_key": "IDP"
  }'
```

### Response Format

```json
{
    "database": "DB0",
    "collection": "Stock",
    "size_input": 52,
    "size_msg": 20,
    "sel_att": 0.001,
    "S": 1,
    "res_q": 1,
    "vol_network": 72,
    "vol_RAM": 1000080.0,
    "time_network": 1.632e-06,
    "time_RAM": 0.00032002,
    "time_total": 0.000321,
    "carbon_footprint": 1.96e-07
}
```

The response includes all calculated metrics: input/message sizes, selectivity, server count, volumes, timings, and carbon footprint.
