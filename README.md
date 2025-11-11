# Big Data Structure

![Python](https://img.shields.io/badge/Python-3.13-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-Latest-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

## Overview

**Big Data Structure** is a FastAPI-based tool for estimating database storage requirements across different denormalization strategies. Given an e-commerce domain schema, it calculates storage footprint for six predefined database layouts (DB0–DB5), ranging from fully normalized to heavily denormalized designs.

Perfect for:
- Comparing storage costs of different schema designs
- Understanding denormalization trade-offs
- Planning database capacity for large-scale systems
- Validating schema assumptions before implementation

## Quick Start

### Prerequisites
- Python 3.13+
- `uv` (lightweight Python package manager)

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd big_data_structure
```

2. **Install UV** (if not already installed)
```bash
pip install uv
```

Or via install script:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. **Install dependencies**
```bash
uv sync
```

4. **Create and activate virtual environment**
```bash
uv venv --python 3.13
.\.venv\Scripts\Activate
```

5. **Start the API**
```bash
uv run fastapi dev app/main.py
```

The API will be available at `http://127.0.0.1:8000`

**Response Example:**
```json
{
  "message": "Byte calculation successful!",
  "db_signature": "DB4",
  "database_total": "4355.13 GB",
  "collections": {
    "Stock": {
      "nb_docs": 100000,
      "avg_doc_bytes": 104,
      "total_bytes": 10400000,
      "total_human": "9.92 MB"
    },
    "Warehouse": {
      "nb_docs": 200,
      "avg_doc_bytes": 96,
      "total_bytes": 19200,
      "total_human": "18.75 KB"
    },
    "OrderLine": {
      "nb_docs": 4000000000,
      "avg_doc_bytes": 1168,
      "total_bytes": 4672000000000,
      "total_human": "4351.14 GB"
    },
    "Client": {
      "nb_docs": 10000000,
      "avg_doc_bytes": 428,
      "total_bytes": 4280000000,
      "total_human": "3.99 GB"
    }
  }
}
```

## Database Profiles

The tool supports six denormalization strategies:

| Profile | Strategy | Use Case |
|---------|----------|----------|
| **DB0** | Fully normalized | Highest data integrity, most joins |
| **DB1** | Product embeds Categories & Supplier | Balance normalization |
| **DB2** | Product embeds Stock entries | Reduce collection count |
| **DB3** | Stock embeds Product | Optimize stock queries |
| **DB4** | OrderLine embeds Product | Fast order retrieval |
| **DB5** | Product embeds OrderLines | Maximize denormalization |

Choose a profile based on your query patterns and update frequency requirements.

## Architecture

### System Components

```
API Request (GET /bytesCalculator?db_signature=DB4)
    ↓
[Router] Load profile & schema
    ↓
[Schema Parser] Extract entities & relationships
    ↓
[Sizer] Calculate relationship-aware sizes
    ↓
[Statistics] Apply size constants & multipliers
    ↓
JSON Response
```

### Key Modules

#### `app/main.py` — FastAPI Application
Initializes the FastAPI app and registers the bytes calculator router.

#### `app/routers/bytesCalculator.py` — API Endpoint
- **Route**: `http://127.0.0.1:8000/docs#/default/calculate_bytes_bytesCalculator_get`
- Loads the JSON schema for the specified profile
- Builds `Schema`, `Statistics`, and denormalization profile
- Computes sizes using `Sizer`
- Outputs debug information to console

#### `services/schema_client.py` — Schema Parsing
Parses JSON Schema and detects entities, nested objects, and attributes.

**Key Functions:**
- `detect_entities_and_relations()` — Walks schema to identify all entities and their attributes
- `estimate_document_size(entity, stats)` — Computes average document size using type mappings and heuristics
- `count_attribute_types(entity)` — Classifies and counts attribute types for validation

**Sizing Heuristics:**
- Fields named `description` or `comment` → `longstring` (≈200 bytes)
- Fields containing `date` → `date` type (≈20 bytes)
- Arrays → `avg_length × item_size` (no base overhead)
- `categories` field → `avg_categories_per_product` items (default: 2)

#### `services/relationships.py` — Denormalization Profiles
Defines six `DenormProfile` configurations specifying physical collections and relationship storage:

- `fk` — Stored as numeric foreign key fields (8 bytes each)
- `embed_one` — Child document embedded once (full size)
- `embed_many` — Array of child documents (avg_multiplicity × child_size)

#### `services/statistics.py` — Dataset Constants
Holds dataset-wide counts and byte size mappings.

**Default Counts:**
- Clients: 10,000,000
- Products: 100,000
- OrderLines: 4,000,000,000
- Warehouses: 200

**Byte Mappings:**
- `number`, `integer` → 8 bytes
- `string` → 80 bytes
- `date` → 20 bytes
- `longstring` → 200 bytes
- `object`, `reference` → 12 bytes
- `array` → 0 bytes (size from items only)

#### `services/sizing.py` — Relationship-Aware Sizing
Core calculation engine that combines intrinsic entity sizes with relationship storage rules.

**Calculation Flow:**
1. Compute base entity size from field types
2. For each relationship:
   - `fk`: Add 8 bytes per foreign key
   - `embed_one`: Add full child document size
   - `embed_many`: Add `avg_multiplicity × child_size`
3. Multiply by document count to get total collection size

## How Sizing Works

### Intrinsic Field Sizing
Each field type has a default byte size from `Statistics.size_map()`:

```
Field Type        | Size      | Notes
------------------|-----------|-------------------------------------------
number/integer    | 8 bytes   | Fixed-size numeric
string            | 80 bytes  | Average string length
date              | 20 bytes  | Based on field name heuristic
longstring        | 200 bytes | For "description"/"comment" fields
object            | 12 bytes  | Small overhead per object
reference/fk      | 12 bytes  | Foreign key reference
array             | 0 bytes   | No overhead; sized by items only
```

### Relationship Storage

**Foreign Key (fk):**
```
Product ─fk→ Supplier
Adds: 8 bytes per product for supplier_id
```

**Embed One (embed_one):**
```
Product { supplier: { IDS, name, ... } }
Adds: Full supplier document size per product
```

**Embed Many (embed_many):**
```
Prod{[Cat],Supp,[St]} → Prod{[Cat],Supp,[quantity, location, IDW for each of the 200 warehouses]}
Adds: avg_multiplicity × category_size per product
```

### Example Calculation

For DB4 with 100,000 products embedded in each of 4 billion OrderLines:

```
OrderLine size = base_fields + embedded_product_size
  = 200 bytes + 512 bytes = 712 bytes per OrderLine

Total = 4,000,000,000 OrderLines × 712 bytes = ~2.85 TB
```

## API Reference

### Get Storage Estimate

**Parameters:**
- `db_signature` (string, required): Profile name (DB0–DB5)

**Response Fields:**
- `message` — Status message
- `db_signature` — Requested profile
- `database_total` — Human-readable total size
- `collections` — Object mapping collection name to:
  - `nb_docs` — Number of documents
  - `avg_doc_bytes` — Average document size
  - `total_bytes` — Total storage in bytes
  - `total_human` — Human-readable total (KB, MB, GB, TB)

**Console Output:**
After each request, the API prints per-collection type counts to help verify assumptions:

```
Collection: Product
  Type Counts: {number: 5, string: 3, date: 2, array: 1, ...}
```

## Customization

### Adjust Dataset Scale

Edit `services/statistics.py`:

```python
class Statistics:
    nb_clients = 50_000_000        # Increase client count
    nb_products = 500_000          # More products
    nb_orderlines = 10_000_000_000 # More orders
    avg_categories_per_product = 5 # More categories per product
```

### Add Field Heuristics

Modify `services/schema_client.py` in `_classify_attr_type()`:

```python
if "category" in field_name.lower():
    return "category"  # Custom type
```

### Create New Denormalization Profile

Add to `services/relationships.py`:

```python
DB6 = DenormProfile(
    name="DB6",
    collections=["Product", "OrderLine", "Client"],
    relationships=[
        RelationshipSpec(from_entity="Product", to_entity="Stock", storage="embed_many", avg_multiplicity=200),
        # ... more relationships
    ]
)

PROFILES = {
    # ... existing profiles
    "DB6": DB6,
}
```

## Troubleshooting

### Empty Collection Counts
**Issue**: A collection shows 0 documents.

**Cause**: Naming mismatch between profile definition and schema entity. The sizer uses case-insensitive matching to mitigate this, but verify spelling in `relationships.py`.

**Solution**: Check that entity names match between the schema and denormalization profile.

### Arrays Appear Too Large/Small
**Issue**: Array field sizes don't match expectations.

**Cause**: Incorrect `avg_*` values in `Statistics` or missing `items.type` in schema.

**Solution**: 
- Verify `avg_categories_per_product` and similar averages in `Statistics`
- Confirm `items.type` is set for array fields in your JSON Schema
- Remember: arrays have no base overhead; size is `avg_length × item_size` only

### Embed Many Sizes Seem Wrong
**Issue**: Embedded array sizes are larger/smaller than expected.

**Cause**: `embed_many` calculates as `avg_multiplicity × child_size` by design (no array overhead).

**Solution**: This is intentional. For DB2 with 100,000 products and avg 200 stock entries per product:
```
Stock size per product = avg_multiplicity × child_size
                       = 200 × 512 bytes = 102.4 KB per product
Total = 100,000 × 102.4 KB ≈ 10.24 GB
```

## Development

### Project Structure
```
big_data_structure/
├── app/
│   ├── main.py                 # FastAPI app
│   └── routers/
│       └── bytesCalculator.py  # API endpoint
├── services/
│   ├── schema_client.py        # Schema parsing
│   ├── relationships.py        # Denormalization profiles
│   ├── statistics.py           # Dataset constants
│   ├── sizing.py               # Size calculations
│   └── JSON_schema/            # Profile schemas
│       ├── json-schema-DB0.json
│       └── ...
├── pyproject.toml              # Project config
└── README.md
```