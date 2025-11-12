# 🧮 Big Data Structure

![Python](https://img.shields.io/badge/Python-3.13-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-Latest-green)
![uv](https://img.shields.io/badge/Package_Manager-uv-purple)

A **FastAPI-based analytical tool** for estimating database storage and distribution characteristics across multiple **denormalisation strategies (DB0–DB5)**.  
It models a simplified e-commerce system and computes both:
- The **total storage footprint** of each database design.
- The **sharding distribution** (docs per server, keys per server) given infrastructure statistics.

---

## 🚀 Quick Start

### 1️⃣ Clone the repository
```bash
git clone <repository-url>
cd big_data_structure
```

### 2️⃣ Install dependencies using [`uv`](https://github.com/astral-sh/uv)
```bash
pip install uv
uv sync
```

### 3️⃣ Run the FastAPI application
```bash
uv run fastapi dev app/main.py
```

➡️ The API will be available at:  
**http://127.0.0.1:8000**

Open the interactive documentation at:  
**http://127.0.0.1:8000/docs**

---

## 🧩 Endpoints

### `/bytesCalculator`
Estimate total storage size for a selected database profile (DB0–DB5).

**Example:**
```
GET /bytesCalculator?db_signature=DB5
```

**Response:**
```json
{
  "message": "Byte calculation successful!",
  "db_signature": "DB4",
  "database_total": "5386.28 GB (Database_Total)",
  "collections": {
    "Stock": {
      "nb_docs": 20000000,
      "avg_doc_bytes": 112,
      "total_size_bytes": 2240000000,
      "total_size_human": "2.09 GB (Stock)"
    },
    "Warehouse": {
      "nb_docs": 200,
      "avg_doc_bytes": 132,
      "total_size_bytes": 26400,
      "total_size_human": "25.78 KB (Warehouse)"
    },
    "OrderLine": {
      "nb_docs": 4000000000,
      "avg_doc_bytes": 1444,
      "total_size_bytes": 5776000000000,
      "total_size_human": "5379.32 GB (OrderLine)"
    },
    "Client": {
      "nb_docs": 10000000,
      "avg_doc_bytes": 512,
      "total_size_bytes": 5120000000,
      "total_size_human": "4.77 GB (Client)"
    },
    "Categories": {
      "nb_docs": 0,
      "avg_doc_bytes": 0,
      "total_size_bytes": 0,
      "total_size_human": "0.00 GB (Categories)"
    },
    "Product": {
      "nb_docs": 100000,
      "avg_doc_bytes": 1116,
      "total_size_bytes": 111600000,
      "total_size_human": "106.43 MB (Product)"
    },
    "Price": {
      "nb_docs": 0,
      "avg_doc_bytes": 132,
      "total_size_bytes": 0,
      "total_size_human": "0.00 GB (Price)"
    },
    "Supplier": {
      "nb_docs": 0,
      "avg_doc_bytes": 440,
      "total_size_bytes": 0,
      "total_size_human": "0.00 GB (Supplier)"
    },
    "Revenue": {
      "nb_docs": 0,
      "avg_doc_bytes": 132,
      "total_size_bytes": 0,
      "total_size_human": "0.00 GB (Revenue)"
    },
    "Database_Total": {
      "total_size_bytes": 5783471626400,
      "total_size_human": "5386.28 GB (Database_Total)"
    }
  }
}
```

---

### `/shardingStats`
Compute **document and key distribution** across servers for the six predefined sharding strategies.

**Example Console Output:**
```
=== SHARDING DISTRIBUTION STATISTICS ===
Collection   Shard key     Docs/server     Keys/server    Active servers     Docs/active
----------------------------------------------------------------------------------------
Stock        #IDP              20,000          100.00             1,000          20,000
Stock        #IDW              20,000            1.00               200         100,000
OrderLine    #IDC           4,000,000       10,000.00             1,000       4,000,000
OrderLine    #IDP           4,000,000          100.00             1,000       4,000,000
Product      #IDP                 100          100.00             1,000             100
Product      #brand               100            5.00             1,000             100
----------------------------------------------------------------------------------------
Total servers available: 1,000
```

---

## 🧠 Core Components

### 🧩 `Schema` — Schema Parsing & Entity Detection
Located in **`services/schema_client.py`**

- Detects entities and nested entities from JSON schemas  
- Identifies arrays, embedded objects, and attribute types
- Estimates intrinsic document size per entity  

**Key methods:**
```python
detect_entities_and_relations()
_classify_attr_type(attr)
count_attribute_types(entity)
print_entities_and_relations()
```

---

### ⚙️ `Sizer` — Schema-Based Size Estimator
Located in **`services/sizing.py`**

Computes per-document and per-collection sizes using:
- Schema structure (fields, embedded docs, arrays)
- Dataset statistics (e.g. number of products, warehouses)

**Features**
- Adds +12 B for required keys  
- Multiplies by average multiplicities (categories per product, etc.)

**Key methods**
```python
estimate_document_size(entity)
compute_collection_sizes()
```

---

### 📊 `Statistics` — Dataset & Sharding Model
Located in **`services/statistics.py`**

Centralises:
- Dataset constants (e.g. 10 M clients, 4 B order lines)
- Byte-size mappings per type  
- Infrastructure setup (1 000 servers)
- Sharding distribution computation  

**Key methods**
```python
get_collection_count(name)
compute_sharding_stats()
```

---

### 🌐 `bytesCalculator` API
Located in **`app/routers/bytesCalculator.py`**

Provides two REST endpoints:
- `/bytesCalculator` → storage estimation by DB signature  
- `/shardingStats` → sharding statistics across servers  

**Automatically loads:**
- JSON schema (`json-schema-DBx.json`)
- Dataset stats
- Entity parser and Sizer

---

## 📦 Project Structure

```
big_data_structure/
├── app/
│   ├── main.py
│   └── routers/
│       └── bytesCalculator.py
├── services/
│   ├── schema_client.py
│   ├── sizing.py
│   ├── statistics.py
│   └── JSON_schema/
│       ├── json-schema-DB0.json
│       ├── json-schema-DB1.json
│       ├── json-schema-DB2.json
│       ├── json-schema-DB3.json
│       ├── json-schema-DB4.json
│       └── json-schema-DB5.json
├── pyproject.toml
└── README.md
```

---

## 🧰 Extending the Project

To add a **new database variant (DB6)**:
1. Create a new JSON schema in `services/JSON_schema/` (e.g. `json-schema-DB6.json`)  
2. Add its signature to `DB_SIGNATURES` in `bytesCalculator.py`  
3. The system auto-detects entities and computes new sizes dynamically  

To adjust **sharding scenarios**:
- Change parameters in `Statistics` (e.g. `nb_servers`, `nb_clients`)  
- Call `/shardingStats` to see new distribution results  

---

## 🧮 Summary

| Feature | Description |
|----------|--------------|
| **Storage estimation** | Computes total DB size (B, KB, MB, GB) |
| **Schema-driven** | Infers structure directly from JSON schemas |
| **Sharding statistics** | Calculates key and document distribution |
| **Fast & lightweight** | No external DB, just FastAPI + Python |
| **Fully extensible** | Add new DB signatures or adjust dataset scales |

---
