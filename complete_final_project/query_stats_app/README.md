# Query Cost Analyzer - Complete Documentation

## 📋 Table of Contents

1. [Overview](#overview)
2. [Features](#features)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Architecture](#architecture)
6. [How It Works](#how-it-works)
7. [Query Types Supported](#query-types-supported)
8. [Cost Calculation Formulas](#cost-calculation-formulas)
9. [Manual Overrides](#manual-overrides)
10. [File Structure](#file-structure)
11. [Dependencies](#dependencies)

---

## Overview

The **Query Cost Analyzer** is a Streamlit web application that analyzes SQL query execution costs for NoSQL databases with different sharding strategies. It calculates network volume, RAM usage, execution time, carbon footprint, and budget costs for queries across multiple database designs (DB1-DB5).

### Key Capabilities

- **SQL Query Parsing**: Parses SQL queries and extracts collections, filters, projections, joins, and aggregates
- **Cost Calculation**: Computes network volume, RAM usage, time, CO2 emissions, and budget
- **Sharding Analysis**: Analyzes costs with and without sharding, multiple sharding keys
- **Multiple Database Designs**: Supports DB1-DB5 with different denormalization strategies
- **Manual Overrides**: Allows fine-tuning of any calculated value
- **Real-time Recalculation**: Updates costs automatically when values are changed

---

## Features

### Core Features

1. **Database Selection**: Choose from DB1-DB5 configurations
2. **SQL Query Input**: Enter any SQL query (supports SELECT-FROM-WHERE, JOIN, GROUP BY)
3. **Sharding Configuration**:
   - With or without sharding
   - Multiple sharding key selection (IDP, IDW, IDC, IDS, date, brand)
4. **Index Configuration**: Toggle index existence on filter fields
5. **Detailed Analysis**:
   - Query characteristics table
   - Network volume breakdown
   - RAM volume breakdown
   - Cost metrics (Time, CO2, Budget)
6. **Manual Overrides**: Edit any value and see instant recalculation

### Query Types Supported

- **Filter Queries**: Simple SELECT-FROM-WHERE
- **Join Queries**: Multi-table JOINs with nested loop algorithm
- **Aggregate Queries**: GROUP BY with aggregate functions (SUM, COUNT, AVG, MAX, MIN)
- **Join-Aggregate Queries**: JOINs combined with aggregates

---

## Installation

### Prerequisites

- Python 3.8+
- pip or uv package manager

### Steps

1. **Install Dependencies**:
   ```bash
   pip install streamlit pandas
   # or
   uv add streamlit pandas
   ```

2. **Verify File Structure**:
   Ensure the following structure exists:
   ```
   query_stats_app/
   ├── app.py
   └── requirements.txt
   services/
   ├── __init__.py
   ├── query_parser.py
   ├── query_cost.py
   ├── calculate_stats.py
   ├── statistics.py
   ├── schema_client.py
   ├── sizing.py
   ├── manual_counts_example.py
   ├── results_TD1.json
   └── JSON_schema/
       ├── json-schema-DB1.json
       ├── json-schema-DB2.json
       ├── json-schema-DB3.json
       ├── json-schema-DB4.json
       └── json-schema-DB5.json
   ```

---

## Usage

### Running the Application

From the project root directory:

```bash
streamlit run query_stats_app/app.py
```

Or from the `query_stats_app` directory:

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

### Basic Workflow

1. **Select Database**: Choose DB1-DB5 from the sidebar
2. **Enter SQL Query**: Type your SQL query in the text area
3. **Configure Sharding**:
   - Choose "With Sharding" or "Without Sharding"
   - If with sharding, select one or more sharding keys
4. **Set Index**: Check if an index exists on filter fields
5. **Enable Manual Overrides** (optional): Toggle to edit any value
6. **Calculate**: Click "Calculate Costs" to see detailed analysis

### Example Queries

**Q1 - Stock Lookup**:
```sql
SELECT S.IDP, S.quantity, S.location 
FROM Stock S 
WHERE S.IDP = $IDP AND S.IDW = $IDW
```

**Q2 - Products by Brand**:
```sql
SELECT P.IDP, P.name, P.price 
FROM Product P 
WHERE P.brand = $brand
```

**Q3 - Orders by Date**:
```sql
SELECT O.IDP, O.quantity 
FROM OrderLine O 
WHERE O.date = $date
```

**Join Query**:
```sql
SELECT P.name, S.quantity 
FROM Stock S 
JOIN Product P ON S.IDP = P.IDP 
WHERE S.IDW = $IDW
```

**Aggregate Query**:
```sql
SELECT P.brand, SUM(O.quantity) 
FROM OrderLine O 
JOIN Product P ON O.IDP = P.IDP 
GROUP BY P.brand
```

---

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────┐
│                  Streamlit UI (app.py)                   │
│  - User Interface                                        │
│  - Configuration Sidebar                                  │
│  - Results Display                                       │
│  - Manual Overrides                                      │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│              Query Parser (query_parser.py)             │
│  - SQL Parsing                                           │
│  - Schema-based Type Inference                           │
│  - Query Structure Extraction                            │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│        Query Cost Calculator (query_cost.py)            │
│  - Cost Calculation Engine                               │
│  - Sharding Logic                                        │
│  - Volume Calculations                                    │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│      Statistics Extractor (calculate_stats.py)           │
│  - Result Formatting                                     │
│  - Metric Extraction                                     │
└─────────────────────────────────────────────────────────┘
```

### Supporting Services

- **statistics.py**: Constants (byte sizes, bandwidth, CO2 factors, prices)
- **schema_client.py**: JSON schema parsing and entity detection
- **sizing.py**: Document size estimation
- **manual_counts_example.py**: Manual field count overrides

---

## How It Works

### Complete Operation Flow

#### 1. **Query Parsing** (`query_parser.py`)

When a SQL query is entered:

1. **Schema Loading**: Loads JSON schema for selected database
2. **Type Inference**: Builds field type lookup from schema
3. **Query Analysis**: Detects query type (filter, join, aggregate)
4. **Field Extraction**:
   - Parses SELECT clause → `project_fields`
   - Parses WHERE clause → `filter_fields`
   - Parses JOIN clauses → `join_conditions`
   - Parses GROUP BY → `group_by_fields`
   - Parses aggregate functions → `aggregate_functions`
5. **Type Assignment**: Infers field types from schema

**Returns**: Structured dict with query components

#### 2. **Query Characteristics Extraction** (`calculate_stats.py`)

Extracts metrics for display:

1. **Field Counts**: Counts filter/projection fields by type (integer, string, date)
2. **Size Calculation**: 
   - `query_size` (size_input): Filter + projection keys + key overhead
   - `output_size` (size_msg): Projection values + key overhead
3. **Key Count**: Total number of keys (filter + projection)

**Returns**: Dict with `filter_counts`, `proj_counts`, `nb_keys`, `query_size`, `output_size`

#### 3. **Cost Calculation** (`query_cost.py`)

The core calculation engine:

**For Filter Queries**:

1. **Collection Resolution**: Maps logical to physical collection (handles embedded collections)
2. **Selectivity Calculation**: Estimates fraction of documents matching filter
3. **Server Count (S)**: 
   - If filtering on sharding key: S = 1
   - Otherwise: S = 1000 (all servers)
4. **Volume Calculations**:
   - Network: `S × size_input + res_q × size_msg`
   - RAM per server: `index × 1MB + (res_q/S) × size_doc`
   - RAM total: Different formula based on S
5. **Cost Calculations**:
   - Time: `vol_network/BANDWIDTH_NETWORK + vol_RAM/BANDWIDTH_RAM`
   - CO2: `vol_network × CO2_NETWORK + vol_ram_total × CO2_RAM`
   - Budget: `vol_network × NETWORK_PRICE`

**For Join Queries**:

1. **Embedded Check**: If collections map to same physical collection, treat as single query
2. **Two-Phase Join**:
   - **Op1**: Filter on driving collection (collection1)
   - **Op2**: Point lookup on collection2 for each Op1 result
3. **Combined Metrics**: `total = op1 + nb_iter_op2 × op2_single`

**For Aggregate Queries**:

1. **Group Calculation**: Estimates number of groups from GROUP BY fields
2. **Filter Query Logic**: Uses filter query calculation but outputs groups
3. **Output Adjustment**: Adjusts output size for aggregate function results

**Returns**: Complete cost breakdown with all metrics

#### 4. **Result Display** (`app.py`)

Displays results in organized sections:

1. **Query Characteristics Table**: Field counts, sizes, keys
2. **Cost Breakdown Tables**:
   - Network section: S, query size, nb output, output size, Network Vol
   - RAM section: Index, pointers, doc size, RAM Vol per server, RAM Vol total
   - Costs section: Time, kgCO2eq, Budget
3. **Comparison Table**: If multiple sharding keys, shows side-by-side comparison
4. **Summary Metrics**: Key metrics as cards

---

## Query Types Supported

### 1. Filter Queries

Simple SELECT-FROM-WHERE queries:

```sql
SELECT field1, field2 
FROM Collection 
WHERE field3 = value AND field4 = value
```

**Processing**:
- Single collection lookup
- Filter on WHERE conditions
- Project selected fields

### 2. Join Queries

Multi-table JOINs:

```sql
SELECT T1.field1, T2.field2 
FROM Table1 T1 
JOIN Table2 T2 ON T1.id = T2.id 
WHERE T1.field = value
```

**Processing**:
- Two-phase nested loop join
- Op1: Filter on driving collection
- Op2: Point lookup for each Op1 result
- Handles embedded collections (same physical collection)

### 3. Aggregate Queries

GROUP BY with aggregate functions:

```sql
SELECT field1, SUM(field2), COUNT(*) 
FROM Collection 
WHERE field3 = value 
GROUP BY field1
```

**Processing**:
- Estimates number of groups
- Calculates costs for grouped output
- Handles multiple aggregate functions

### 4. Join-Aggregate Queries

JOINs combined with aggregates:

```sql
SELECT T1.field1, SUM(T2.field2) 
FROM Table1 T1 
JOIN Table2 T2 ON T1.id = T2.id 
GROUP BY T1.field1
```

**Processing**:
- Combines join and aggregate logic
- Groups results after join

---

## Cost Calculation Formulas

### Network Volume

```
vol_network = S × size_input + res_q × size_msg
```

Where:
- `S`: Number of servers accessed
- `size_input`: Query size in bytes
- `res_q`: Number of result documents
- `size_msg`: Output message size in bytes

### RAM Volume (Per Server)

```
vol_RAM = index × 1,000,000 + (res_q / S) × size_doc
```

Where:
- `index`: 1 if index exists, 0 otherwise
- `size_doc`: Collection document size in bytes

### RAM Volume (Total)

**Case 1 (S = 1)**:
```
vol_ram_total = vol_RAM
```

**Case 2 (S > 1)**:
```
vol_ram_total = nb_srv_working × vol_RAM + (S - nb_srv_working) × index × 1,000,000
```

Where `nb_srv_working = 50` when S > 1

### Time Cost

```
time = vol_network / BANDWIDTH_NETWORK + vol_RAM / BANDWIDTH_RAM
```

Where:
- `BANDWIDTH_NETWORK = 100,000,000 bytes/s` (100 MB/s)
- `BANDWIDTH_RAM = 25,000,000,000 bytes/s` (25 GB/s)

### CO2 Emissions

```
co2_kg = vol_network × CO2_NETWORK + vol_ram_total × CO2_RAM
```

Where:
- `CO2_NETWORK = 1.10e-11 kgCO2eq/byte`
- `CO2_RAM = 2.80e-11 kgCO2eq/byte`

### Budget Cost

```
budget = vol_network × NETWORK_PRICE
```

Where:
- `NETWORK_PRICE = 1.10e-11 €/byte`

### Query Size Calculation

**Input Size (size_input)**:
```
size_input = filter_size + proj_keys_size + key_costs
```

Where:
- `filter_size = filter_ints × 8 + filter_strings × 80 + filter_dates × 20`
- `proj_keys_size = (proj_ints + proj_strings + proj_dates) × 8`
- `key_costs = nb_keys × 12`

**Output Size (size_msg)**:
```
size_msg = proj_values_size + proj_key_costs
```

Where:
- `proj_values_size = proj_ints × 8 + proj_strings × 80 + proj_dates × 20`
- `proj_key_costs = (proj_ints + proj_strings + proj_dates) × 12`

---

## Manual Overrides

The application supports comprehensive manual value overrides for fine-tuning calculations.

### Enabling Overrides

Check "Allow Manual Value Overrides" in the sidebar.

### Override Types

#### 1. **Field Type Overrides**

Edit field types via dropdowns:
- Filter fields: Change type (integer, string, date, longstring)
- Projection fields: Change type

**Impact**: Affects size calculations (different types have different byte sizes)

#### 2. **Query Characteristics Overrides**

Edit in the Query Characteristics table:
- Filter counts by type (integer, string, date)
- Projection counts by type
- `nb_keys`: Total number of keys
- `query_size`: Query input size in bytes
- `output_size`: Output message size in bytes

**Impact**: Directly affects query and output size calculations

#### 3. **Cost Overrides**

Edit in Cost Breakdown tables:

**Network Section**:
- `S`: Number of servers
- `query size`: Query input size
- `output size`: Output message size

**RAM Section**:
- `Index`: 0 or 1
- `Nb pointers per working`: Number of result pointers per server
- `collection doc size`: Document size in bytes

**Impact**: Triggers automatic recalculation of:
- Network volume
- RAM volumes
- Time cost
- CO2 emissions
- Budget

### Recalculation

After editing values:
1. Changes are saved to session state
2. Click "Calculate Costs" again to see updated results
3. All derived values are recalculated automatically

### Clearing Overrides

Click "🔄 Clear All Overrides" to reset all manual values.

---

## File Structure

### Main Application

```
query_stats_app/
├── app.py                 # Main Streamlit application
├── requirements.txt       # Python dependencies
└── README_COMPLETE.md     # This documentation
```

### Services

```
services/
├── __init__.py                    # Package initialization
├── query_parser.py                # SQL query parser
├── query_cost.py                  # Cost calculation engine
├── calculate_stats.py             # Statistics extraction helpers
├── statistics.py                  # Constants and dataset statistics
├── schema_client.py               # JSON schema parser
├── sizing.py                      # Document size estimator
├── manual_counts_example.py       # Manual field count overrides
├── results_TD1.json              # Collection size data
└── JSON_schema/                   # Database schemas
    ├── json-schema-DB1.json
    ├── json-schema-DB2.json
    ├── json-schema-DB3.json
    ├── json-schema-DB4.json
    └── json-schema-DB5.json
```

### Key Files Explained

#### `app.py`
- Main Streamlit application
- UI components and user interaction
- Orchestrates query processing flow
- Handles manual overrides

#### `query_parser.py`
- Parses SQL queries into structured format
- Infers field types from JSON schema
- Supports filter, join, and aggregate queries

#### `query_cost.py`
- Core cost calculation engine
- Implements formulas from `formulas_TD2.tex`
- Handles sharding logic
- Calculates volumes, time, CO2, budget

#### `calculate_stats.py`
- Helper functions for extracting statistics
- Formats numbers for display
- Extracts cost breakdowns from results

#### `statistics.py`
- Centralized constants:
  - Byte sizes (integer=8, string=80, date=20, etc.)
  - Bandwidth (network=100MB/s, RAM=25GB/s)
  - CO2 factors
  - Prices
- Dataset statistics (nb_products, nb_warehouses, etc.)

#### `schema_client.py`
- Parses JSON schema files
- Detects entities, attributes, relationships
- Classifies attribute types

#### `sizing.py`
- Estimates document sizes from schema
- Supports manual field count overrides
- Used for embedded object size calculations

#### `results_TD1.json`
- Collection size data for each database
- Contains: `num_docs`, `doc_size_bytes`, `avg_doc_bytes`
- Loaded by `QueryCostCalculator`

---

## Dependencies

### Python Packages

- **streamlit** (>=1.28.0): Web application framework
- **pandas** (>=2.0.0): Data manipulation and display

### Standard Library

- `pathlib`: File path handling
- `json`: JSON file parsing
- `sys`: System path manipulation
- `re`: Regular expressions for SQL parsing
- `typing`: Type hints

### Data Files

- **JSON Schema Files**: Database structure definitions
- **results_TD1.json**: Collection size data

---

## Database Designs (DB1-DB5)

The application supports five different database designs with different denormalization strategies:

### DB1: Product-Centric with Categories and Supplier
- Product contains embedded Categories and Supplier
- Stock is separate collection

### DB2: Product-Centric with Everything Embedded
- Product contains embedded Categories, Supplier, and Stock array

### DB3: Stock-Centric
- Stock contains embedded Product (which contains Categories and Supplier)

### DB4: OrderLine-Centric
- OrderLine contains embedded Product (which contains Categories and Supplier)

### DB5: Product-Centric with OrderLine Array
- Product contains embedded Categories, Supplier, Stock array, and OrderLine array

The application automatically handles collection mapping for embedded collections.

---

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure `services` directory is in Python path
2. **Schema Not Found**: Verify JSON schema files exist in `services/JSON_schema/`
3. **Collection Not Found**: Check that collection name matches schema
4. **Calculation Errors**: Verify `results_TD1.json` contains data for selected database

### Error Messages

- **"Schema file not found"**: Missing JSON schema file for selected database
- **"Collection not found"**: Collection name doesn't exist in database
- **"Invalid query"**: SQL query syntax error or unsupported feature

---

## Advanced Usage

### Custom Field Counts

To use custom field counts, modify `services/manual_counts_example.py` or pass `manual_counts` to `QueryCostCalculator`.

### Custom Collection Sizes

Modify `services/results_TD1.json` to update collection sizes.

### Adding New Query Types

1. Extend `query_parser.py` to parse new query type
2. Add calculation method in `query_cost.py`
3. Update `calculate_stats.py` to extract new metrics

---

## License

This project is part of a Big Data Structures course assignment.

---

## Contact

For questions or issues, refer to the course materials or contact the course instructor.

