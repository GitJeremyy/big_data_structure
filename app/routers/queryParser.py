from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Literal
from services.query_parser import QueryParser, parse_query
from services.query_cost import QueryCostCalculator
from services.statistics import Statistics
import json

router = APIRouter(prefix="/TD2", tags=["TD2 - Query Cost Analysis"])


class FilterField(BaseModel):
    name: str = Field(..., description="Field name")
    type: str = Field(..., description="Field type (number, integer, string, date, longstring, etc.)")


class ProjectField(BaseModel):
    name: str = Field(..., description="Field name")
    type: str = Field(default="boolean", description="Type is always 'boolean' for project fields")


class ParsedQuery(BaseModel):
    collection: str = Field(..., description="Collection name from FROM clause")
    filter_fields: List[FilterField] = Field(..., description="Fields from WHERE clause")
    project_fields: List[ProjectField] = Field(..., description="Fields from SELECT clause")


@router.get(
    "/queryParserTest",
    summary="Test Query Parser with Examples",
    description="Test the query parser with predefined example queries (Q1, Q2, Q3)"
)
async def test_query_parser(
    example: str = Query(
        default="Q1",
        description="Which example to test (Q1, Q2, or Q3)"
    )
):
    """
    Test the query parser with example queries.
    
    **Q1**: Stock query with IDP and IDW (DB1)
    
    **Q2**: Product query by brand (DB3)
    
    **Q3**: OrderLine query by date (DB4)
    """
    examples = {
        "Q1": {
            "sql": "SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW",
            "db_signature": "DB1",
            "description": "Stock query with IDP and IDW"
        },
        "Q2": {
            "sql": "SELECT P.name, P.price FROM Product P WHERE P.brand = $brand",
            "db_signature": "DB0",
            "description": "Product query by brand"
        },
        "Q3": {
            "sql": "SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = $date",
            "db_signature": "DB0",
            "description": "OrderLine query by date"
        }
    }
    
    if example.upper() not in examples:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid example. Choose from: {', '.join(examples.keys())}"
        )
    
    example_data = examples[example.upper()]
    
    try:
        result = parse_query(
            sql=example_data["sql"],
            db_signature=example_data["db_signature"]
        )
        
        return {
            "message": f"Test {example.upper()} executed successfully!",
            "description": example_data["description"],
            "sql": example_data["sql"],
            "db_signature": example_data["db_signature"],
            "parsed_query": result
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Test failed: {str(e)}")


@router.get(
    "/queryCalculateCost",
    summary="Parse SQL and Calculate Query Cost",
    description="""
    Complete query analysis: Parse SQL query and calculate execution costs.
    
    This endpoint combines:
    1. SQL parsing (collection, filter_fields, project_fields)
    2. Query cost calculation (network, RAM, time, carbon footprint)
    
    All parameters are dropdowns except the SQL query:
    - sql: The only text field - your SQL query
    - db_signature: Dropdown to select database design
    - collection_size_file: Dropdown to choose data source
    - sharding_key: Dropdown for sharding field
    - has_index: Dropdown (Yes/No) for index existence
    
    Note: Index size is automatically set to 1MB (from statistics.py) when has_index=True
    
    Note: teacher_correction_TD1.json only has DB1-DB5 (no DB0)
    
    Default Configuration (Q1 - Stock Query):
    - SQL: `SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW;`
    - DB: DB1
    - Sharding key: IDP
    - Collection file: results_TD1.json
    - Has index: True
    
    Outputs:
    - S: Number of servers accessed
    - Selectivity: Fraction of documents matching filter
    - Network volume: Data transferred over network
    - RAM volume: Data processed in memory
    - Time cost: Query execution time
    - Carbon footprint: Environmental impact (gCO2)
    
    Examples:
    
    Q1: `SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = 1 AND S.IDW = 2` (DB1, IDP sharding, has_index=true)
    
    Q2: `SELECT P.name, P.price FROM Product P WHERE P.brand = 'Apple'` (DB0, IDP sharding, has_index=true)
    
    Q3: `SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = '2024-01-01'` (DB0, date sharding, has_index=true)
    """
)
async def calculate_query_cost(
    sql: str = Query(
        default="SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW;",
        description="SQL query to analyze (only text field to type)",
        examples=["SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = 1 AND S.IDW = 2"]
    ),
    db_signature: Literal["DB0", "DB1", "DB2", "DB3", "DB4", "DB5"] = Query(
        default="DB1",
        description="Database signature (DB0-DB5). Note: teacher_correction_TD1.json only has DB1-DB5"
    ),
    collection_size_file: Literal["results_TD1.json", "teacher_correction_TD1.json"] = Query(
        default="results_TD1.json",
        description="Collection size file: student results or teacher correction"
    ),
    sharding_key: Optional[Literal["IDP", "IDC", "IDW", "IDS", "date"]] = Query(
        default="IDP",
        description="Sharding key (if filtering on this key, only 1 server accessed). 'date' is used for OrderLine primary key"
    ),
    has_index: bool = Query(
        default=True,
        description="Does an index exist on filter fields? Index size automatically set to 1MB if True"
    )
):
    """
    Parse SQL query and calculate complete execution cost.
    All parameters are dropdowns except the SQL query.
    """
    try:
        # Validate DB signature based on collection file
        if collection_size_file == "teacher_correction_TD1.json" and db_signature == "DB0":
            raise HTTPException(
                status_code=400,
                detail="teacher_correction_TD1.json does not contain DB0. Please select DB1-DB5 or use results_TD1.json"
            )
        
        # Step 1: Parse the SQL query
        parsed = parse_query(
            sql=sql,
            db_signature=db_signature,
            type_overrides=None
        )
        
        # Step 2: Build query dict for cost calculator
        query_dict = {
            "collection": parsed["collection"],
            "filter_fields": parsed["filter_fields"],
            "project_fields": parsed["project_fields"],
            "sharding_key": sharding_key,
            "has_index": has_index,
        }
        
        # Automatically add index size from Statistics if has_index=True
        if has_index:
            query_dict["index_size"] = Statistics.DEFAULT_INDEX_SIZE
        
        # Step 3: Calculate query cost
        calculator = QueryCostCalculator(
            db_signature=db_signature,
            collection_size_file=collection_size_file
        )
        
        result = calculator.calculate_query_cost(query_dict)
        
        # Remove the duplicate query info from cost_analysis (it's already in parsed_query)
        result.pop("query", None)
        
        return {
            "message": "Query cost calculated successfully!",
            "sql": sql,
            "db_signature": db_signature,
            "collection_size_file": collection_size_file,
            "parsed_query": parsed,
            "cost_analysis": result
        }
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Calculation error: {str(e)}")
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"File not found: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
