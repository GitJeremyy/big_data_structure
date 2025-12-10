# TD2 - Query Cost Analysis Documentation

## 📊 Overview

TD2 focuses on analyzing the execution cost of database queries across different denormalization strategies. The goal is to understand how database design choices impact query performance, resource consumption, and environmental footprint.

---

## 🎯 Learning Objectives

1. **Compare query costs** - See how the same query performs across different database designs
2. **Understand distributed queries** - Learn how sharding affects query execution
3. **Analyze resource usage** - Calculate network, RAM, and time costs
4. **Consider sustainability** - Measure carbon footprint of database queries

---

## 🏗️ Architecture

### Core Components

#### 1. `query_parser.py` - SQL Query Parser
**Purpose:** Parse SQL queries into structured format for cost calculation

**Features:**
- Extracts collection name from `FROM` clause
- Extracts filter fields from `WHERE` clause
- Extracts project fields from `SELECT` clause
- Infers field types from JSON schema
- Supports table aliases (e.g., `Stock S`)

**Example:**
```python
from services.query_parser import parse_query

sql = "SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = 1 AND S.IDW = 2"
result = parse_query(sql, db_signature="DB1")

# Returns:
{
    "collection": "Stock",
    "filter_fields": [
        {"name": "IDP", "type": "number"},
        {"name": "IDW", "type": "number"}
    ],
    "project_fields": [
        {"name": "quantity", "type": "boolean"},
        {"name": "location", "type": "boolean"}
    ]
}
```

#### 2. `query_cost.py` - Query Cost Calculator
**Purpose:** Calculate complete query execution costs using formulas from course materials

**Key Formulas:**

##### Query Size Calculation
```
size_input = Σ(12 + size(filter_value)) + Σ(12 + 8) for projections + 12 (nesting)
size_msg = Σ(12 + size(projected_value)) per result document
```

##### Selectivity Calculation
```
Stock (IDP + IDW): 1 / (nb_products × nb_warehouses) = 1/20M
Product (brand): nb_apple_products / nb_products = 50/100k
OrderLine (date): 1 / nb_days = 1/365
OrderLine (IDC): 1 / nb_clients = 1/10M
```

##### Server Access Calculation (S)
```
S = 1    if filtering on sharding key
S = 1000 if not filtering on sharding key (broadcast query)
```

##### Volume Calculations
```
vol_network = S × size_input + res_q × size_msg
vol_RAM = index_size + sel_att × docs_per_server × doc_size  (with index)
vol_RAM = docs_per_server × doc_size                          (without index)
```

##### Cost Calculations
```
time = vol_network / BANDWIDTH_NETWORK + vol_RAM / BANDWIDTH_RAM
carbon_network = vol_network × 1.10e-11 gCO2/byte
carbon_RAM = vol_RAM × 2.80e-11 gCO2/byte
carbon_total = carbon_network + carbon_RAM
```

**Constants (from `statistics.py`):**
```python
BANDWIDTH_NETWORK = 125_000_000    # 1 Gb/s in bytes/s
BANDWIDTH_RAM = 3_125_000_000      # 25 Gb/s in bytes/s
CO2_NETWORK = 1.10e-11             # g CO2 per byte
CO2_RAM = 2.80e-11                 # g CO2 per byte
DEFAULT_INDEX_SIZE = 1_000_000     # 1 MB
```

---

## 🗺️ Collection Mapping

### Why We Need Mapping

**Problem:** When querying "Stock" in DB2, Stock doesn't exist as a separate collection—it's embedded within Product.

**Solution:** Automatic collection mapping resolves logical collection names to physical collection names.

**Mapping Table:**
```python
COLLECTION_MAPPING = {
    "DB0": {},  # No embedding
    "DB1": {},  # No embedding
    "DB2": {
        "Stock": "Product",      # Stock embedded in Product
        "Categories": "Product",
        "Supplier": "Product"
    },
    "DB3": {
        "Product": "Stock",      # Product embedded in Stock
        "Categories": "Stock",
        "Supplier": "Stock"
    },
    "DB4": {
        "Product": "OrderLine",  # Product embedded in OrderLine
        "Categories": "OrderLine",
        "Supplier": "OrderLine"
    },
    "DB5": {
        "OrderLine": "Product",  # OrderLine embedded in Product
        "Stock": "Product",
        "Categories": "Product",
        "Supplier": "Product"
    }
}
```

**Why This Design Choice?**
1. ✅ **Enables cross-design comparison** - Same query works on all DB designs
2. ✅ **Matches TD objective** - Compare cost impact of different denormalizations
3. ✅ **Educational value** - Students learn about logical vs physical schemas
4. ✅ **Maintains query semantics** - "Give me Stock info" works regardless of where Stock lives
5. ✅ **Automatic and transparent** - Users don't need to know embedding details

**Example:**
```
Query: SELECT S.quantity FROM Stock WHERE S.IDP = 1

DB0: Uses Stock collection (standalone)
DB1: Uses Stock collection (standalone)
DB2: Automatically maps to Product collection (Stock is embedded)
DB3: Uses Stock collection (contains Product)
DB4: Uses Stock collection (standalone)
DB5: Automatically maps to Product collection (Stock is embedded)
```

---

## 📡 API Endpoints

### `/TD2/queryParserTest`

Test the SQL parser with predefined example queries.

**Parameters:**
- `example` (query param): One of Q1, Q2, Q3

**Example Request:**
```bash
GET http://127.0.0.1:8000/TD2/queryParserTest?example=Q1
```

**Example Response:**
```json
{
  "message": "Test Q1 executed successfully!",
  "description": "Stock query with IDP and IDW",
  "sql": "SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW",
  "db_signature": "DB1",
  "parsed_query": {
    "collection": "Stock",
    "filter_fields": [
      {"name": "IDP", "type": "number"},
      {"name": "IDW", "type": "number"}
    ],
    "project_fields": [
      {"name": "quantity", "type": "boolean"},
      {"name": "location", "type": "boolean"}
    ]
  }
}
```

---

### `/TD2/queryCalculateCost`

Complete query cost analysis: parse SQL and calculate all execution metrics.

**Parameters:**
- `sql` (query param, default: Q1) - SQL query to analyze
- `db_signature` (query param, default: DB1) - Database design (DB0-DB5)
- `collection_size_file` (query param, default: results_TD1.json) - Data source
- `sharding_key` (query param, default: IDP) - Sharding field (IDP, IDC, IDW, IDS, date)
- `has_index` (query param, default: true) - Index existence

**Example Request:**
```bash
GET http://127.0.0.1:8000/TD2/queryCalculateCost?sql=SELECT%20S.quantity%20FROM%20Stock%20S%20WHERE%20S.IDP=1&db_signature=DB1&sharding_key=IDP&has_index=true
```

**Example Response:**
```json
{
  "message": "Query cost calculated successfully!",
  "sql": "SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW;",
  "db_signature": "DB1",
  "collection_size_file": "results_TD1.json",
  "parsed_query": {
    "collection": "Stock",
    "filter_fields": [...],
    "project_fields": [...]
  },
  "cost_analysis": {
    "sizes": {
      "size_input_bytes": "92 B",
      "size_msg_bytes": "40 B",
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
      "vol_network": "1.32e+02 B",
      "vol_RAM": "1.00e+06 B"
    },
    "costs": {
      "time": "3.21e-04 s",
      "carbon_network": "1.45e-09 gCO2",
      "carbon_RAM": "2.80e-05 gCO2",
      "carbon_total": "2.80e-05 gCO2"
    }
  }
}
```

**Output Explanation:**

**Sizes:**
- `size_input_bytes` - Query message sent to servers
- `size_msg_bytes` - Result message per document
- `size_doc_bytes` - Average document size in collection

**Distribution:**
- `S_servers` - Number of servers that execute the query
- `selectivity` - Fraction of documents matching filter (0-1)
- `res_q_results` - Number of result documents returned
- `nb_docs_total` - Total documents in collection
- `nb_docs_per_server` - Average documents per server

**Volumes:**
- `vol_network` - Total data transferred over network
- `vol_RAM` - Total data processed in memory

**Costs:**
- `time` - Query execution time in seconds
- `carbon_network` - CO2 emissions from network transfer
- `carbon_RAM` - CO2 emissions from RAM processing
- `carbon_total` - Total carbon footprint

---

## 🔍 Query Analysis Examples

### Example 1: Q1 - Stock Lookup (Highly Selective)

**Query:**
```sql
SELECT S.quantity, S.location 
FROM Stock S 
WHERE S.IDP = $IDP AND S.IDW = $IDW
```

**Characteristics:**
- **Selectivity:** Very high (1 out of 20 million)
- **Sharding:** Filter on IDP (sharding key) → Only 1 server accessed
- **Index:** Beneficial (small result set)

**Cost Comparison:**

| Design | Collection | S | Selectivity | Network | RAM | Time | Carbon |
|--------|-----------|---|-------------|---------|-----|------|--------|
| DB0 | Stock | 1 | 5e-8 | 132 B | 1 MB | 0.32 ms | 28 µgCO2 |
| DB1 | Stock | 1 | 5e-8 | 132 B | 1 MB | 0.32 ms | 28 µgCO2 |
| DB2 | Product | 1 | 5e-8 | 132 B | 1 MB | 0.32 ms | 28 µgCO2 |

**Insight:** Highly selective queries perform similarly across designs.

---

### Example 2: Q2 - Products by Brand (Moderate Selectivity)

**Query:**
```sql
SELECT P.name, P.price 
FROM Product P 
WHERE P.brand = 'Apple'
```

**Characteristics:**
- **Selectivity:** Moderate (50 out of 100k = 0.0005)
- **Sharding:** Filter NOT on sharding key → All 1000 servers accessed
- **Index:** Very beneficial (avoids full scan)

**Cost Comparison:**

| Design | Collection | S | Results | Network | RAM | Time | Carbon |
|--------|-----------|---|---------|---------|-----|------|--------|
| DB0 | Product | 1000 | 50 | ~92 KB | ~1 MB/server | High | High |
| DB1 | Product | 1000 | 50 | ~114 KB | ~1.2 MB/server | Higher | Higher |

**Insight:** Broadcast queries (S=1000) are expensive. Embedding increases cost.

---

### Example 3: Q3 - Orders by Date (Low Selectivity)

**Query:**
```sql
SELECT O.IDP, O.quantity 
FROM OrderLine O 
WHERE O.date = '2024-01-01'
```

**Characteristics:**
- **Selectivity:** Low (1/365 days)
- **Result count:** ~11 million documents
- **Sharding:** Filter NOT on sharding key → All servers accessed
- **Index:** Critical for performance

**Cost Comparison:**

| Design | Collection | S | Results | Network | RAM | Time | Carbon |
|--------|-----------|---|---------|---------|-----|------|--------|
| DB0 | OrderLine | 1000 | 11M | ~11 GB | 1.3 GB | Very High | Very High |
| DB4 | OrderLine | 1000 | 11M | ~11 GB | Higher | Higher | Higher |

**Insight:** Large result sets dominate cost. Embedding makes it worse.

---

## 📊 Design Decision Impact

### When to Use Each Design

**DB0 (Fully Normalized)**
- ✅ Flexible querying
- ✅ Minimal storage
- ❌ Requires joins
- **Best for:** Read-heavy workloads with diverse query patterns

**DB1 (Categories & Supplier in Product)**
- ✅ Product queries include related data
- ❌ Slight storage increase
- **Best for:** Product catalog browsing

**DB2 (Stock also in Product)**
- ✅ Product + availability in one query
- ❌ Significant storage increase
- ❌ Stock queries on non-IDP are expensive
- **Best for:** Inventory management systems

**DB3 (Product in Stock)**
- ❌ Huge storage increase
- ❌ Most queries become expensive
- **Best for:** Rarely recommended

**DB4 (Product in OrderLine)**
- ✅ Order details with product info
- ❌ Storage increase
- **Best for:** Order processing systems

**DB5 (OrderLine in Product)**
- ✅ Product purchase history together
- ❌ Very large Product documents
- **Best for:** Recommendation systems

---

## 🎓 Key Learnings

### Query Cost Factors

1. **Sharding Key Match**
   - Filtering on sharding key: S = 1 (optimal)
   - Not filtering on sharding key: S = 1000 (expensive)

2. **Selectivity**
   - High selectivity (few results): Low cost
   - Low selectivity (many results): High cost

3. **Index Usage**
   - With index: Only scan index + matching docs
   - Without index: Full collection scan (very expensive)

4. **Embedding Impact**
   - Increases document size
   - Increases RAM usage
   - Can increase network transfer
   - But reduces need for joins

### Optimization Strategies

1. **Choose sharding key wisely** - Most common filter should be sharding key
2. **Index frequently filtered fields** - Dramatically reduces RAM usage
3. **Consider query patterns** - Embed data queried together
4. **Balance selectivity** - Low selectivity queries need careful design
5. **Monitor result size** - Large results dominate network cost

---

## 🔧 Testing

Run query cost tests:
```bash
uv run python test_query_cost.py
```

Run query parser tests:
```bash
uv run python test_query_parser.py
```

---

## 🌱 Carbon Footprint Considerations

**Why Measure Carbon?**
- Data centers consume ~1% of global electricity
- Query optimization = energy savings = lower emissions
- Design choices have environmental impact

**CO2 Coefficients:**
- Network: 1.10e-11 gCO2/byte (data transmission)
- RAM: 2.80e-11 gCO2/byte (processing power)

**Example Impact:**
- Q1 (selective): ~28 µgCO2 per query
- Q3 (large result): ~280 mgCO2 per query (10,000× more!)

**Takeaway:** Efficient queries aren't just faster—they're greener! 🌍

---

## 📚 Related Files

- `services/query_parser.py` - SQL parser implementation
- `services/query_cost.py` - Cost calculator implementation
- `services/statistics.py` - Constants and dataset stats
- `test_query_cost.py` - Query cost tests
- `test_query_parser.py` - Parser tests
- `formulas_TD2.tex` - Mathematical formulas (LaTeX)

---

## 🤔 Common Questions

**Q: Why does the same query cost more in DB2?**
A: Because Stock is embedded in Product, documents are larger, requiring more RAM to process.

**Q: Why is S sometimes 1 and sometimes 1000?**
A: S=1 when filtering on the sharding key (direct lookup), S=1000 when broadcasting to all servers.

**Q: What's the biggest cost factor?**
A: Usually the result size. Queries returning millions of documents are expensive regardless of design.

**Q: Should I always use indexes?**
A: For selective queries, yes! Indexes reduce RAM usage dramatically. For very low selectivity, the benefit is smaller.

**Q: How accurate are carbon calculations?**
A: They're estimates based on typical data center coefficients. Real values vary by infrastructure, but relative comparisons are valid.

---

[← TD1 Documentation](README_TD1.md) | [Back to General README →](README_GENERAL.md)
