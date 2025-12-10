# Big Data Structure - Database Analysis Tool

![Python](https://img.shields.io/badge/Python-3.13-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-Latest-green)
![uv](https://img.shields.io/badge/Package_Manager-uv-purple)

A FastAPI-based analytical tool for analyzing database storage, distribution, and query costs across multiple denormalization strategies in NoSQL systems.

---

## 🎯 Project Overview

This project is a comprehensive educational tool designed to understand the impact of different database denormalization strategies on:
- **Storage footprint** (TD1)
- **Data distribution** (TD1)
- **Query execution costs** (TD2)
- **Environmental impact** (TD2)

The system models a simplified e-commerce database with 6 different denormalization strategies (DB0-DB5), allowing comparison of how design choices affect performance and resource consumption.

---

## 🗂️ Database Designs

The project analyzes 6 different database schemas:

| Design | Description | Structure |
|--------|-------------|-----------|
| **DB0** | Fully normalized | Prod, Cat, Supp, St, Wa, OL, Cl |
| **DB1** | Categories & Supplier in Product | Prod{[Cat],Supp}, St, Wa, OL, Cl |
| **DB2** | + Stock in Product | Prod{[Cat],Supp,[St]}, Wa, OL, Cl |
| **DB3** | Product hierarchy in Stock | St{Prod{[Cat],Supp}}, Wa, OL, Cl |
| **DB4** | Product hierarchy in OrderLine | St, Wa, OL{Prod{[Cat],Supp}}, Cl |
| **DB5** | OrderLine in Product | Prod{[Cat],Supp,[OL]}, St, Wa, Cl |

**Legend:** `[Collection]` = embedded array, `Collection` = embedded object

---

## 🚀 Quick Start

### 1. Clone and Install

```bash
git clone <repository-url>
cd big_data_structure
pip install uv
uv sync
```

### 2. Run the API

```bash
uv run fastapi dev app/main.py
```

The API will be available at **http://127.0.0.1:8000**

### 3. Access Documentation

Open **http://127.0.0.1:8000/docs** for interactive API documentation

---

## 📡 API Endpoints

### TD1 - Storage Analysis

#### `/TD1/bytesCalculator`
Calculate total storage size for each database design based on JSON schemas.

**Example:**
```
GET /TD1/bytesCalculator?db_signature=DB1
```

**Returns:**
- Total database size
- Per-collection document count and size
- Human-readable size formats (KB, MB, GB)

#### `/TD1/shardingStats`
Calculate data distribution across servers for sharding strategies.

**Example:**
```
GET /TD1/shardingStats?db_signature=DB1
```

**Returns:**
- Documents per server
- Sharding keys per server
- Distribution statistics

---

### TD2 - Query Cost Analysis

#### `/TD2/queryParserTest`
Test the SQL query parser with predefined examples (Q1, Q2, Q3).

**Example:**
```
GET /TD2/queryParserTest?example=Q1
```

#### `/TD2/queryCalculateCost`
Complete query analysis: parse SQL and calculate execution costs.

**Example:**
```
GET /TD2/queryCalculateCost?sql=SELECT S.quantity FROM Stock S WHERE S.IDP=1&db_signature=DB1&has_index=true
```

**Returns:**
- Number of servers accessed (S)
- Selectivity and result count
- Network and RAM volumes
- Execution time
- Carbon footprint (gCO2)

---

## 🧪 Example Queries

### Q1: Stock Lookup by Product and Warehouse
```sql
SELECT S.quantity, S.location 
FROM Stock S 
WHERE S.IDP = $IDP AND S.IDW = $IDW
```
**Use Case:** Check product availability at specific warehouse

### Q2: Products by Brand
```sql
SELECT P.name, P.price 
FROM Product P 
WHERE P.brand = 'Apple'
```
**Use Case:** Find all products from a specific brand

### Q3: Orders by Date
```sql
SELECT O.IDP, O.quantity 
FROM OrderLine O 
WHERE O.date = '2024-01-01'
```
**Use Case:** Retrieve orders from a specific date

---

## 📊 Dataset Statistics

- **Clients:** 10 million
- **Products:** 100,000
- **Order Lines:** 4 billion
- **Warehouses:** 200
- **Stock Records:** 20 million (100k products × 200 warehouses)
- **Servers:** 1,000 (for sharding calculations)

---

## 🛠️ Technology Stack

- **FastAPI** - Modern web framework for APIs
- **Python 3.13** - Latest Python version
- **uv** - Fast Python package manager
- **Pydantic** - Data validation
- **JSON Schema** - Database schema definitions

---

## 📚 Project Structure

```
big_data_structure/
├── app/
│   ├── main.py                    # FastAPI application
│   └── routers/
│       ├── bytesCalculator.py     # TD1 endpoints
│       └── queryParser.py         # TD2 endpoints
├── services/
│   ├── schema_client.py           # JSON schema parser
│   ├── sizing.py                  # Storage calculations
│   ├── statistics.py              # Dataset statistics
│   ├── query_parser.py            # SQL parser
│   ├── query_cost.py              # Cost calculator
│   ├── results_TD1.json           # Student results
│   ├── teacher_correction_TD1.json # Teacher correction
│   └── JSON_schema/               # Database schemas (DB0-DB5)
├── test_query_cost.py             # Query cost tests
├── test_query_parser.py           # Parser tests
├── README_GENERAL.md              # This file
├── README_TD1.md                  # TD1 detailed documentation
└── README_TD2.md                  # TD2 detailed documentation
```

---

## 🎓 Educational Goals

### TD1: Understanding Storage Impact
- Learn how denormalization affects storage requirements
- Understand document embedding and arrays
- Calculate realistic storage needs for NoSQL databases

### TD2: Analyzing Query Performance
- Compare query costs across different designs
- Understand the role of indexes and sharding
- Learn about network vs RAM trade-offs
- Consider environmental impact (carbon footprint)

---

## 📖 Further Reading

- **[TD1 Documentation](README_TD1.md)** - Detailed storage analysis guide
- **[TD2 Documentation](README_TD2.md)** - Detailed query cost analysis guide
- **[API Documentation](http://127.0.0.1:8000/docs)** - Interactive Swagger UI

---

## 🤝 Contributing

This is an educational project. For questions or improvements, please contact the project maintainers.

---

## 📝 License

Educational use only.
