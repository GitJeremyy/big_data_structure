# 🧮 Big Data Structure

![Python](https://img.shields.io/badge/Python-3.13-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-Latest-green)
![uv](https://img.shields.io/badge/Package_Manager-uv-purple)

A lightweight **FastAPI** tool to **estimate database storage requirements** for different **denormalisation strategies** (DB0–DB5).  
It models an e-commerce schema (Products, Clients, Orders, etc.) and computes how much space each strategy would take — from fully normalised to fully embedded.

---

## 🚀 Quick Start

### 1️⃣ Clone the repository
```bash
git clone <repository-url>
cd big_data_structure
```

### 2️⃣ Install dependencies with `uv`
```bash
pip install uv
uv sync
```

### 3️⃣ Run the API
```bash
uv run fastapi dev app/main.py
```

➡️ The API will start at:  
**http://127.0.0.1:8000**

Open the interactive docs at:  
**http://127.0.0.1:8000/docs**

---

## ⚙️ How It Works

When you call:
```
GET /bytesCalculator?db_signature=DB5
```

the app runs this pipeline:

```
Profile (DB5)
   ↓
Schema Parser  → extracts entities, arrays, and nested objects
   ↓
Sizer          → applies denormalisation rules (fk, embed_one, embed_many)
   ↓
Statistics     → uses dataset constants and type sizes
   ↓
Response       → returns per-collection and total database size
```

---

## 🧩 Example Output

```json
{
  "message": "Byte calculation successful!",
  "db_signature": "DB5",
  "database_total": "1049.44 GB (Database_Total)",
  "collections": {
    "Product": {
      "nb_docs": 100000,
      "avg_doc_bytes": 11201476,
      "total_bytes": 1120147600000,
      "total_human": "1043.22 GB (Product)"
    },
    "Stock": {
      "nb_docs": 20000000,
      "avg_doc_bytes": 120,
      "total_bytes": 2400000000,
      "total_human": "2.24 GB (Stock)"
    },
    "Warehouse": {
      "nb_docs": 200,
      "avg_doc_bytes": 96,
      "total_bytes": 19200,
      "total_human": "18.75 KB (Warehouse)"
    },
    "Client": {
      "nb_docs": 10000000,
      "avg_doc_bytes": 428,
      "total_bytes": 4280000000,
      "total_human": "3.99 GB (Client)"
    },
    "Database_Total": {
      "total_bytes": 1126827619200,
      "total_human": "1049.44 GB (Database_Total)"
    }
  }
}
```

---

## 🧠 Core Components

### 🧩 `Schema` — Schema Parsing & Entity Detection
Located in **`services/schema_client.py`**

Handles everything related to understanding the structure of the JSON schema:
- Detects **entities**, **nested objects**, and **arrays of objects**
- Classifies attributes by type (`number`, `string`, `date`, `longstring`, `array`, etc.)
- Estimates intrinsic document size (before relationships)

**Key Methods**
```python
detect_entities_and_relations()   # Recursively extracts entities and nested entities
estimate_document_size(entity)    # Estimates average document size based on type heuristics
count_attribute_types(entity)     # Counts intrinsic types for debug & validation
```

**Used for:** establishing the logical model before applying denormalisation.

---

### ⚙️ `Sizer` — Relationship-Aware Size Calculator  
Located in **`services/sizing.py`**

Combines intrinsic entity sizes with denormalisation strategies to produce realistic storage estimates.

**Responsibilities**
- Traverses relationships from a `DenormProfile`
- Applies `fk`, `embed_one`, or `embed_many` storage rules
- Recursively computes per-entity and per-collection byte totals
- Aggregates results into a full database footprint

**Key Methods**
```python
_entity_doc_size(entity_name)     # Core recursive computation of doc size
_entity_intrinsic_size(name)      # Uses Schema to compute base size
compute_collection_sizes()        # Multiplies by document counts (from Statistics)
_entity_type_counts()             # Debug: counts types across embedded structures
```

**How it fits:** this is the “engine” that merges logical schema + physical storage model.

---

### 📊 `Statistics` — Dataset Constants & Type Sizes  
Located in **`services/statistics.py`**

Holds all constants and average values used in sizing.

**Contains:**
- Entity counts (e.g. 10M clients, 4B order lines)
- Average relationships per entity (e.g. orders per client)
- Approximate byte mappings for each type

**Example**
```python


self.nb_clients = 10**7            # 10 million customers
self.nb_products = 10**5           # 100,000 products
self.nb_orderlines = 4 * 10**9     # 4 billion order lines
self.nb_warehouses = 200           # 200 warehouses
```

**Used by:** both `Schema` and `Sizer` for every size computation.

---

### 🧱 `DenormProfile` & `RelationshipSpec` — Database Layout Definitions  
Located in **`services/relationships.py`**

Define how each database profile (DB0–DB5) physically stores entities and relationships.

**Key Classes**
```python
class RelationshipSpec:
    from_entity: str
    to_entity: str
    stored_as: str           # "fk" | "embed_one" | "embed_many"
    fk_fields: Optional[List[str]] = None
    avg_multiplicity: Optional[float] = None  # only for embed_many

class DenormProfile:
    name: str
    collections: List[str]
    relationships: List[RelationshipSpec]
```

**Example (DB5):**
```python
RelationshipSpec("Product", "OrderLine", "embed_many", avg_multiplicity=Statistics().nb_orderlines / Statistics().nb_products)
```

With nb_orderlines = 4,000,000,000 and nb_products = 100,000
That means: each product embeds 40,000 order lines → heavy denormalisation.

---

### 🧮 `bytesCalculator` API — FastAPI Route  
Located in **`app/routers/bytesCalculator.py`**

The main entry point for users.  
- Accepts a query parameter `db_signature` (DB0–DB5)  
- Loads the JSON schema, denormalisation profile, and statistics  
- Instantiates `Schema` and `Sizer`, then returns results as JSON

---

## 📦 Denormalisation Profiles

| Profile | Description |
|----------|--------------|
| **DB0** | Fully normalised — every table separate |
| **DB1** | Product embeds Categories & Supplier |
| **DB2** | Product embeds Stock entries |
| **DB3** | Stock embeds Product |
| **DB4** | OrderLine embeds Product |
| **DB5** | Product embeds OrderLines *(max denormalisation)* |

---

## 🧰 Project Structure

```
big_data_structure/
├── app/
│   ├── main.py                # FastAPI entrypoint
│   └── routers/
│       └── bytesCalculator.py # API route
├── services/
│   ├── schema_client.py       # Schema parsing
│   ├── relationships.py       # DB0–DB5 profiles
│   ├── statistics.py          # Dataset constants
│   ├── sizing.py              # Relationship-aware size calculator
│   └── JSON_schema/
        ├── DB*.json
│       └── json-schema-DB*.json
├── pyproject.toml
└── README.md
```

## 🔧 Adapting or Extending the Project

### 1️⃣ Changing the Database Signature (e.g. from DB5 to DB6)

When introducing a new **denormalisation profile** or tweaking an existing one:

**Files to modify:**
| File | Purpose | What to do |
|------|----------|------------|
| `services/relationships.py` | Defines all DB profiles | Add a new `DenormProfile` (e.g. `DB6`) or edit relationships |
| `services/JSON_schema/json-schema-DB*.json` | Schema definitions | Create a matching schema file for the new DB signature |
| `services/statistics.py` | Dataset constants | Adjust counts or multiplicities (e.g. avg orders per client) if the model logic changes |
| `app/routers/bytesCalculator.py` | API route logic | Ensure your new profile (e.g. `"DB6"`) is loaded and passed to the Sizer |
| (Optional) `Schema._extract_entities_recursive()` | Schema parsing | Add special cases if your new schema introduces new object types or naming conventions |

**Typical example (DB6):**
```python
DB6 = DenormProfile(
    name="DB6",
    collections=["Product", "OrderLine", "Client"],
    relationships=[
        RelationshipSpec("Product", "Stock", "embed_many", avg_multiplicity=200),
        RelationshipSpec("OrderLine", "Client", "fk"),
    ]
)
PROFILES["DB6"] = DB6
```

✅ That’s usually all you need for a new variant of your current e-commerce schema.

---

### 2️⃣ Changing to a Completely Different Database or Domain

If you move away from the current **e-commerce** model (Products, Clients, Orders) — e.g. into healthcare, IoT, or financial data — the following must be reconsidered.

**Files to revisit:**

| File | Purpose | Required changes |
|------|----------|------------------|
| `services/JSON_schema/*.json` | **Schema definition** | Replace with a new JSON schema describing your new domain entities, attributes, and relationships |
| `services/statistics.py` | **Dataset parameters** | Define new counts (e.g. `nb_patients`, `nb_devices`) and type averages that fit the new data scale |
| `services/relationships.py` | **DenormProfiles** | Redefine collections and relationships reflecting the new domain logic |
| `services/schema_client.py` | **Schema parsing rules** | Update heuristics for attribute naming (e.g. `"timestamp"`, `"sensor_id"`, `"value"` instead of `"price"` or `"orderLine"`) |
| `services/sizing.py` | **Sizer logic** | Optional: adjust byte weighting rules or introduce new relationship storage strategies |
| `app/routers/bytesCalculator.py` | **API behaviour** | If your endpoint or response format changes, modify accordingly |
| `tests/` | **Validation tests** | Add new test data and checks for your new schema |

**In short:**  
If you just add another denormalisation variant → edit **relationships + schema + stats**.  
If you change the domain entirely → rewrite **schema + statistics + relationships** (and maybe tweak `Schema` logic if new patterns appear).