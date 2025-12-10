# TD1 - Storage Analysis Documentation

## 📊 Overview

TD1 focuses on calculating and analyzing the storage footprint of different database denormalization strategies. The goal is to understand how embedding documents and arrays affects the total storage requirements in NoSQL databases.

---

## 🎯 Learning Objectives

1. **Understand denormalization impact** - See how different embedding strategies affect storage
2. **Calculate document sizes** - Learn to compute byte-level storage for complex documents
3. **Analyze data distribution** - Understand sharding and data distribution across servers
4. **Compare designs** - Evaluate trade-offs between different database designs

---

## 🏗️ Architecture

### Core Components

#### 1. `schema_client.py` - JSON Schema Parser
**Purpose:** Parse JSON schemas and detect entities, attributes, and relationships

**Key Methods:**
- `detect_entities_and_relations()` - Extract entities from JSON schema
- `_classify_attr_type(attr)` - Determine field types (string, integer, date, etc.)
- `_extract_entities_recursive()` - Handle nested entities and arrays

**Type Classification:**
```python
"description" in name → "longstring"
"date" in name → "date"
type == "number" → "number"
type == "integer" → "integer"
type == "string" → "string"
type == "array" → "array"
type == "object" → "object" or "reference"
```

#### 2. `statistics.py` - Dataset Statistics
**Purpose:** Centralize all dataset volumes and byte sizes

**Key Constants:**
```python
# Byte sizes (MongoDB-style)
SIZE_NUMBER = 8          # numeric fields
SIZE_INTEGER = 8         # integer fields
SIZE_STRING = 80         # short strings
SIZE_DATE = 20           # ISO date strings
SIZE_LONGSTRING = 200    # descriptions
SIZE_KEY = 12            # key overhead

# Dataset volumes
nb_clients = 10**7           # 10 million
nb_products = 10**5          # 100,000
nb_orderlines = 4 * 10**9    # 4 billion
nb_warehouses = 200
```

#### 3. `sizing.py` - Storage Calculator
**Purpose:** Calculate document sizes and collection totals

**Calculation Formula:**
```
Document Size = Σ(field overhead + field value size) + nesting overhead
Collection Size = num_documents × avg_document_size
Database Total = Σ(all collections)
```

**Nesting Rules:**
- Each embedded object: +12 bytes overhead
- Each array: 12 bytes per element + element size
- Each field: 12 bytes key overhead + value size

---

## 📡 API Endpoints

### `/TD1/bytesCalculator`

Calculate total storage size for a database design.

**Parameters:**
- `db_signature` (query param): One of DB0, DB1, DB2, DB3, DB4, DB5

**Example Request:**
```bash
GET http://127.0.0.1:8000/TD1/bytesCalculator?db_signature=DB1
```

**Example Response:**
```json
{
  "message": "Byte calculation successful!",
  "db_signature": "DB1",
  "database_total": "1256.61 GB",
  "collections": {
    "Product": {
      "nb_docs": 100000,
      "avg_doc_bytes": 1148,
      "total_size_bytes": 114800000,
      "total_size_human": "109.47 MB"
    },
    "Stock": {
      "nb_docs": 20000000,
      "avg_doc_bytes": 152,
      "total_size_bytes": 3040000000,
      "total_size_human": "2.83 GB"
    },
    ...
  }
}
```

**Output Fields:**
- `nb_docs` - Number of documents in collection
- `avg_doc_bytes` - Average document size in bytes
- `total_size_bytes` - Total collection size in bytes
- `total_size_human` - Human-readable size (KB/MB/GB)

---

### `/TD1/shardingStats`

Calculate data distribution across servers.

**Parameters:**
- `db_signature` (query param): One of DB0, DB1, DB2, DB3, DB4, DB5

**Example Request:**
```bash
GET http://127.0.0.1:8000/TD1/shardingStats?db_signature=DB1
```

**Example Response:**
```json
{
  "message": "Sharding statistics calculated successfully!",
  "db_signature": "DB1",
  "nb_servers": 1000,
  "collections": {
    "Product": {
      "nb_docs_total": 100000,
      "docs_per_server": 100,
      "sharding_key": "IDP",
      "nb_distinct_keys": 100000,
      "keys_per_server": 100
    },
    "Stock": {
      "nb_docs_total": 20000000,
      "docs_per_server": 20000,
      "sharding_key": "IDW",
      "nb_distinct_keys": 200,
      "keys_per_server": 0.2
    }
  }
}
```

**Output Fields:**
- `nb_docs_total` - Total documents in collection
- `docs_per_server` - Average documents per server
- `sharding_key` - Field used for sharding
- `nb_distinct_keys` - Number of unique sharding key values
- `keys_per_server` - Average sharding keys per server

---

## 🔍 Design Comparison

### Example: Stock Collection Across Designs

| Design | Collection | Documents | Doc Size | Total Size | Embedded In |
|--------|-----------|-----------|----------|------------|-------------|
| DB0 | Stock | 20M | 152 B | 2.83 GB | Standalone |
| DB1 | Stock | 20M | 152 B | 2.83 GB | Standalone |
| DB2 | Product | 100K | 1260 B | 120.16 MB | Product contains [Stock] |
| DB3 | Stock | 20M | 972 B | 18.12 GB | Stock contains Product |
| DB4 | Stock | 20M | 152 B | 2.83 GB | Standalone |
| DB5 | Product | 100K | 1260 B | 120.16 MB | Product contains [Stock] |

**Observations:**
- DB2 & DB5: Stock embedded in Product → fewer documents, larger doc size
- DB3: Product embedded in Stock → more documents, much larger total
- Embedding trades document count for document size

---

## 💾 Storage Calculation Examples

### Example 1: Simple Document (DB0 - Product)
```json
{
  "IDP": 123,           // 12 (key) + 8 (number) = 20 B
  "name": "Laptop",     // 12 + 80 (string) = 92 B
  "price": 999.99,      // 12 + 8 (number) = 20 B
  "brand": "Apple",     // 12 + 80 (string) = 92 B
  "description": "..."  // 12 + 200 (longstring) = 212 B
}
// Total: 20 + 92 + 20 + 92 + 212 + nesting = ~448 B
```

### Example 2: Embedded Document (DB1 - Product with Supplier)
```json
{
  "IDP": 123,
  "name": "Laptop",
  "supplier": {         // +12 B (object overhead)
    "IDS": 456,         // 12 + 8 = 20 B
    "name": "TechCorp", // 12 + 80 = 92 B
    "SIRET": "..."      // 12 + 80 = 92 B
  }
}
// Total: base fields + 12 (nesting) + embedded fields
```

### Example 3: Embedded Array (DB1 - Product with Categories)
```json
{
  "IDP": 123,
  "categories": [       // +12 B (array overhead)
    {"title": "Electronics"}, // 12 + 12 + 80 = 104 B
    {"title": "Computers"}    // 12 + 12 + 80 = 104 B
  ]
}
// Array size = 12 + (num_elements × element_size)
```

---

## 📈 Results Storage

Results are automatically saved to `services/results_TD1.json` when using the API.

**File Structure:**
```json
{
  "DB0": {
    "description": "DB0: Prod, Cat, Supp, St, Wa, OL, Cl",
    "database_total": "1259.37 GB",
    "collections": [...]
  },
  "DB1": {...},
  ...
}
```

This file is then used by TD2 for query cost calculations.

---

## 🎓 Key Takeaways

### Storage Trade-offs

1. **Normalized (DB0)**
   - ✅ Smaller individual documents
   - ✅ No data duplication
   - ❌ More collections to manage
   - ❌ Requires joins for queries

2. **Denormalized with Embedding (DB2, DB5)**
   - ✅ Fewer collections
   - ✅ Related data together
   - ❌ Larger documents
   - ❌ Potential data duplication

3. **Inverse Embedding (DB3, DB4)**
   - ✅ Different query optimization
   - ❌ Can significantly increase storage
   - ❌ Complex to maintain

### Best Practices

- **Consider query patterns** - Embed data that's queried together
- **Watch document size** - MongoDB has 16MB document limit
- **Balance duplication** - Some duplication is OK for performance
- **Plan for growth** - Consider how data volume will scale

---

## 🔧 Testing

Run the storage calculator tests:
```bash
uv run python -c "from services.sizing import Sizer; from services.schema_client import Schema; import json; schema = Schema(json.load(open('services/JSON_schema/json-schema-DB1.json'))); sizer = Sizer(schema); print(sizer.calculate_all_sizes())"
```

---

## 📚 Related Files

- `services/JSON_schema/json-schema-DB*.json` - Database schemas
- `services/results_TD1.json` - Calculated storage results
- `services/teacher_correction_TD1.json` - Teacher reference results

---

[← Back to General README](README_GENERAL.md) | [TD2 Documentation →](README_TD2.md)
