# Query Cost Analyzer - Final Project

This folder contains everything needed to run the Query Cost Analyzer application.

## 📁 Folder Structure

```
complete_final_project/
├── README.md                    # This file
├── query_stats_app/
│   ├── app.py                   # Main Streamlit application
│   ├── requirements.txt         # Python dependencies
│   └── README.md                # Complete documentation
└── services/
    ├── __init__.py              # Package initialization
    ├── query_parser.py          # SQL query parser
    ├── query_cost.py            # Cost calculation engine
    ├── calculate_stats.py       # Statistics extraction helpers
    ├── statistics.py            # Constants and dataset statistics
    ├── schema_client.py         # JSON schema parser
    ├── sizing.py                # Document size estimator
    ├── manual_counts_example.py # Manual field count overrides
    ├── results_TD1.json        # Collection size data
    └── JSON_schema/             # Database schemas
        ├── json-schema-DB1.json
        ├── json-schema-DB2.json
        ├── json-schema-DB3.json
        ├── json-schema-DB4.json
        └── json-schema-DB5.json
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r query_stats_app/requirements.txt
```

Or using uv:
```bash
uv pip install -r query_stats_app/requirements.txt
```

### 2. Run the Application

From the `complete_final_project` directory:

```bash
streamlit run query_stats_app/app.py
```

The app will open in your browser at `http://localhost:8501`

## 📖 Documentation

For complete documentation, see:
- **`query_stats_app/README.md`** - Comprehensive documentation covering:
  - Architecture and how it works
  - All query types supported
  - Cost calculation formulas
  - Manual overrides system
  - Troubleshooting guide

## 🎯 What This Application Does

The Query Cost Analyzer is a Streamlit web application that:

1. **Parses SQL Queries**: Supports SELECT-FROM-WHERE, JOINs, and GROUP BY queries
2. **Calculates Costs**: Computes network volume, RAM usage, execution time, CO2 emissions, and budget
3. **Analyzes Sharding**: Compares costs with and without sharding, multiple sharding keys
4. **Supports Multiple DBs**: Works with DB1-DB5 database designs
5. **Manual Overrides**: Allows fine-tuning of any calculated value

## 📋 Example Usage

1. Select a database (DB1-DB5) from the sidebar
2. Enter a SQL query, for example:
   ```sql
   SELECT S.IDP, S.quantity, S.location 
   FROM Stock S 
   WHERE S.IDP = $IDP AND S.IDW = $IDW
   ```
3. Configure sharding (with/without, select keys)
4. Set index configuration
5. Click "Calculate Costs" to see detailed analysis

## 🔧 Requirements

- Python 3.8+
- streamlit >= 1.28.0
- pandas >= 2.0.0

## 📝 Key Features

- ✅ SQL Query Parsing (filter, join, aggregate queries)
- ✅ Cost Calculation (network, RAM, time, CO2, budget)
- ✅ Sharding Analysis (multiple sharding keys)
- ✅ Manual Value Overrides
- ✅ Real-time Recalculation
- ✅ Multiple Database Designs (DB1-DB5)

## 🗂️ File Descriptions

### Application Files

- **`query_stats_app/app.py`**: Main Streamlit application with UI and orchestration
- **`query_stats_app/requirements.txt`**: Python package dependencies

### Service Files

- **`services/query_parser.py`**: Parses SQL queries and extracts structure
- **`services/query_cost.py`**: Core cost calculation engine implementing formulas
- **`services/calculate_stats.py`**: Helper functions for extracting and formatting statistics
- **`services/statistics.py`**: Centralized constants (byte sizes, bandwidth, CO2 factors, prices)
- **`services/schema_client.py`**: Parses JSON schema files and detects entities
- **`services/sizing.py`**: Estimates document sizes from schema
- **`services/manual_counts_example.py`**: Manual field count overrides for fine-tuning

### Data Files

- **`services/results_TD1.json`**: Collection size data (num_docs, doc_size_bytes) for each database
- **`services/JSON_schema/*.json`**: Database schema definitions for DB1-DB5

## 🐛 Troubleshooting

### Import Errors
Ensure you're running from the `complete_final_project` directory so Python can find the `services` package.

### Schema Not Found
Verify that all JSON schema files exist in `services/JSON_schema/`.

### Collection Not Found
Check that the collection name in your query matches the schema for the selected database.

## 📚 For More Information

See `query_stats_app/README.md` for:
- Complete architecture explanation
- Detailed operation flow
- All cost calculation formulas
- Manual overrides guide
- Advanced usage examples

---

**Note**: This is a complete, self-contained project folder. All dependencies and data files are included.

