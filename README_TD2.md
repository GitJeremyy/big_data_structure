# TD2 - Query Cost Analysis Documentation

## 📊 Overview

TD2 focuses on analysing the execution cost of database queries across different denormalization strategies. The goal is to understand how database design choices impact query performance, resource consumption, and environmental footprint.

---

## 🎯 Learning Objectives

1. **Compare query costs** - See how the same query performs across different database designs
2. **Understand distributed queries** - Learn how sharding affects query execution
3. **Analyse resource usage** - Calculate network, RAM, and time costs
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
  - Filter values: use actual types (number, string, date, etc.)
  - Project fields: use boolean/integer (8 bytes) which indicates "include this field"
  
size_msg = Σ(12 + size(projected_value)) per result document
  - Uses actual field types from schema (quantity=number, location=string, etc.)
  - Example: SELECT quantity, location → 12+8 (number) + 12+80 (string) = 112 B
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
vol_RAM = docs_per_server × doc_size                         (without index)
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

**Problem:** When querying "Stock" in DB2, Stock doesn't exist as a separate collection (it's embedded within Product).

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
3. ✅ **Educational value** - Learn about logical vs physical schemas
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
DB5: Uses Stock collection (standalone)
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
- `sql` (query param, default: Q1) - SQL query to analyse
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
WHERE S.IDP = 1 AND S.IDW = 2
```

**Characteristics:**
- **Selectivity:** Very high (1 out of 20 million = 5.00e-08)
- **Filter includes:** IDP (product) and IDW (warehouse)
- **Good sharding key:** IDP (in filter)
- **Bad sharding key:** IDC (not in filter)

**Size Breakdown:**
- `size_input = 92 B`: IDP filter (12+8) + IDW filter (12+8) + quantity project (12+8) + location project (12+8) + nesting (12)
- `size_msg = 112 B`: quantity (12+8) + location (12+80)

#### Results Across All Databases

| DB | Collection | Good Key | S (Good) | Results | Network (Good) | S (Bad) | Network (Bad) | Network Increase |
|----|-----------|----------|----------|---------|----------------|---------|---------------|------------------|
| DB0 | Stock | IDP | 1 | 1 | 2.04e+02 B | 1000 | 9.21e+04 B | **451×** |
| DB1 | Stock | IDP | 1 | 1 | 2.04e+02 B | 1000 | 9.21e+04 B | **451×** |
| DB2 | Product* | IDP | 1 | **0** | 9.20e+01 B | 1000 | 9.20e+04 B | **1000×** |
| DB3 | Stock | IDP | 1 | 1 | 2.04e+02 B | 1000 | 9.21e+04 B | **451×** |
| DB4 | Stock | IDP | 1 | 1 | 2.04e+02 B | 1000 | 9.21e+04 B | **451×** |
| DB5 | Product* | IDP | 1 | **0** | 9.20e+01 B | 1000 | 9.20e+04 B | **1000×** |

*Stock embedded in Product - query returns 0 because we're filtering by IDP+IDW but Stock is nested

**Key Insights:**
- ✅ **Sharding matters hugely:** 450-1000× more network traffic with wrong key
- ⚠️ **DB2 & DB5 return 0 results:** Stock embedded in Product, can't filter by IDP at Stock level
- 🎯 **Optimal design:** DB0, DB1, DB3, DB4 with IDP sharding (S=1, 204B network)
- ❌ **Worst case:** Any database with IDC sharding (S=1000, 92KB network)

---

### Example 2: Q2 - Products by Brand (Moderate Selectivity)

**Query:**
```sql
SELECT P.name, P.price 
FROM Product P 
WHERE P.brand = 'Apple'
```

**Characteristics:**
- **Selectivity:** Moderate (50 out of 100k = 5.00e-04)
- **Filter includes:** brand (NOT a sharding key!)
- **Result:** S=1000 for ALL databases (broadcast query)
- **Index:** Critical (avoids full collection scan)

**Size Breakdown:**
- `size_input = 144 B`: brand filter (12+80) + name project (12+8) + price project (12+8) + nesting (12)
- `size_msg = 236 B`: name (12+80) + price object (12+132)
  - **price object = 132 B**: amount (12+8) + currency (12+80) + VAT (12+8)

#### Results Across All Databases

| DB | Collection | S | Results | Network | Time | Carbon | Result Explanation |
|----|-----------|---|---------|---------|------|--------|--------------------|
| DB0 | Product | 1000 | 50 | 1.56e+05 B | 1.57e-03 s | 2.97e-05 gCO2 | Normal: 50 products |
| DB1 | Product | 1000 | 50 | 1.56e+05 B | 1.57e-03 s | 2.97e-05 gCO2 | Normal: 50 products |
| DB2 | Product | 1000 | 50 | 1.56e+05 B | 1.57e-03 s | 2.97e-05 gCO2 | Normal: 50 products |
| DB3 | Product* | 1000 | **10,000** | **2.43e+06 B** | **1.98e-02 s** | **5.51e-05 gCO2** | Product in Stock: 50 × 200 warehouses |
| DB4 | Product* | 1000 | **2,000,000** | **4.72e+08 B** | **3.78e+00 s** | **5.30e-03 gCO2** | Product in OrderLine: 50 × 40k orders |
| DB5 | Product | 1000 | 50 | 1.56e+05 B | 1.57e-03 s | 2.97e-05 gCO2 | Normal: 50 products |

*Product embedded - returns duplicates for each parent document

**Key Insights:**
- 🚨 **DB4 is catastrophic:** 2 million results, 472 MB network, 3.78 seconds!
- ⚠️ **DB3 is problematic:** 10k results (200× more than expected)
- ✅ **DB0, DB1, DB2, DB5 are optimal:** 50 results, 156 KB, 1.57 ms
- 📊 **Embedding impact:** Product embedded in high-cardinality collections causes massive result inflation
- 💡 **Sharding key irrelevant:** Brand not in any sharding key → always S=1000

---

### Example 3: Q3 - Orders by Date (Low Selectivity)

**Query:**
```sql
SELECT O.IDP, O.quantity 
FROM OrderLine O 
WHERE O.date = '2024-01-01'
```

**Characteristics:**
- **Selectivity:** Low (1/365 days = 2.74e-03)
- **Expected results:** ~11 million documents (0.27% of 4 billion)
- **Good sharding key:** date (in filter)
- **Bad sharding key:** IDC (not in filter)

**Size Breakdown:**
- `size_input = 84 B`: date filter (12+20) + IDP project (12+8) + quantity project (12+8) + nesting (12)
- `size_msg = 40 B`: IDP (12+8) + quantity (12+8)

#### Results Across All Databases

| DB | Collection | S (Good) | S (Bad) | Results | Network | Time | Impact |
|----|-----------|----------|---------|---------|---------|------|--------|
| DB0 | OrderLine | 1 | 1000 | 10,958,904 | 4.38e+08 B | 3.51 s | Same cost both ways! |
| DB1 | OrderLine | 1 | 1000 | 10,958,904 | 4.38e+08 B | 3.51 s | Same cost both ways! |
| DB2 | OrderLine | 1 | 1000 | 10,958,904 | 4.38e+08 B | 3.51 s | Same cost both ways! |
| DB3 | OrderLine | 1 | 1000 | 10,958,904 | 4.38e+08 B | 3.51 s | Same cost both ways! |
| DB4 | OrderLine | 1 | 1000 | 10,958,904 | 4.38e+08 B | 3.51 s | Same cost both ways! |
| DB5 | Product* | 1 | 1000 | **273** | **1.10e+04 B** | **1.59e-03 s** | **40,000× fewer results!** |

*OrderLine embedded in Product - fundamentally different query semantics

**Key Insights:**
- 🔥 **Massive result set:** 11M documents = 438 MB transferred
- ⏱️ **Time dominated by network:** 3.51 seconds (438MB ÷ 125MB/s)
- 🤔 **Sharding key irrelevant:** date vs IDC makes NO difference!
  - **Why?** Result set so large that network transfer dominates
  - date sharding (S=1): 1 server → coordinator transfers 438 MB
  - IDC sharding (S=1000): 1000 servers → coordinator still receives 438 MB total
- 🌟 **DB5 is radically different:** Only 273 products had orders on that date
  - **11 KB vs 438 MB** - 40,000× smaller!
  - **1.59 ms vs 3.51 s** - 2,200× faster!
  - Different semantics: "Products with orders on date" vs "Orders on date"
- 💡 **Lesson:** Low selectivity queries need result set optimization, not just sharding

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
