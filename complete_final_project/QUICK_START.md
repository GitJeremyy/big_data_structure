# Quick Start Guide

Get the Query Cost Analyzer running in 3 steps!

## Step 1: Install Dependencies

```bash
pip install -r query_stats_app/requirements.txt
```

Or if you prefer uv:
```bash
uv pip install -r query_stats_app/requirements.txt
```

## Step 2: Run the Application

From the `complete_final_project` directory:

```bash
streamlit run query_stats_app/app.py
```

## Step 3: Use the Application

1. The app opens in your browser at `http://localhost:8501`
2. Select a database (DB1-DB5) from the sidebar
3. Enter a SQL query, for example:
   ```sql
   SELECT S.IDP, S.quantity, S.location 
   FROM Stock S 
   WHERE S.IDP = $IDP AND S.IDW = $IDW
   ```
4. Configure sharding and index settings
5. Click "Calculate Costs" to see the analysis

## Example Queries to Try

### Simple Filter Query
```sql
SELECT P.name, P.price 
FROM Product P 
WHERE P.brand = $brand
```

### Join Query
```sql
SELECT P.name, S.quantity 
FROM Stock S 
JOIN Product P ON S.IDP = P.IDP 
WHERE S.IDW = $IDW
```

### Aggregate Query
```sql
SELECT P.brand, SUM(O.quantity) 
FROM OrderLine O 
JOIN Product P ON O.IDP = P.IDP 
GROUP BY P.brand
```

## Troubleshooting

**Problem**: Import errors when running  
**Solution**: Make sure you're in the `complete_final_project` directory

**Problem**: Schema file not found  
**Solution**: Verify `services/JSON_schema/json-schema-DB*.json` files exist

**Problem**: Collection not found  
**Solution**: Check that your collection name matches the schema for the selected database

## Need More Help?

See `query_stats_app/README.md` for complete documentation.

