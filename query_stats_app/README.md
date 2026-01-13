# Query Stats App

Streamlit application for analyzing query costs with different sharding strategies.

## Installation

Make sure you have Streamlit installed:

```bash
pip install streamlit
# or
uv add streamlit
```

## Running the App

From the project root directory:

```bash
streamlit run query_stats_app/app.py
```

Or from the `query_stats_app` directory:

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

## Features

- **Database Selection**: Choose from DB1-DB5 configurations
- **SQL Query Input**: Enter any SQL query (supports SELECT-FROM-WHERE and JOIN)
- **Sharding Configuration**: 
  - With or without sharding
  - Multiple sharding key selection
- **Index Configuration**: Toggle index existence
- **Detailed Analysis**: 
  - Query characteristics table
  - Network volume breakdown
  - RAM volume breakdown
  - Cost metrics (Time, CO2, Budget)

## Usage

1. Select your database configuration (DB1-DB5)
2. Enter your SQL query in the text area
3. Choose sharding type and keys
4. Set index configuration
5. Click "Calculate Costs" to see the analysis

## Output Tables

The app displays two main tables:

1. **Query Characteristics**: Shows field counts by type, number of keys, query size, and output size
2. **Cost Breakdown**: Detailed breakdown of Network, RAM, and Cost metrics

