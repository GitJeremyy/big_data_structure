"""
Helper functions for extracting and formatting statistics from query cost calculations.
These functions work with results from QueryCostCalculator.
"""

from typing import Dict, List
from services.query_cost import QueryCostCalculator
from services.statistics import Statistics


def format_scientific(value: float) -> str:
    """Format number in scientific notation"""
    return f"{value:.2e}"


def calculate_budget(network_vol: float) -> float:
    """
    Calculate budget cost from network volume.
    
    Args:
        network_vol: Network volume in bytes
        
    Returns:
        Budget cost in euros
    """
    PRICE_PER_BYTE = 1.1e-11  # €/B
    return network_vol * PRICE_PER_BYTE


def extract_field_counts_by_type(
    fields: List[Dict], 
    collection: str, 
    calculator: QueryCostCalculator
) -> Dict[str, int]:
    """
    Extract counts of fields by type (integer, string, date).
    Uses calculator.infer_type() to determine actual field types.
    
    Args:
        fields: List of field dicts with 'name' and optionally 'type' and 'collection'
        collection: Collection name for type inference (used if field doesn't have 'collection')
        calculator: QueryCostCalculator instance for type inference
    
    Returns:
        Dict with 'integer', 'string', 'date' counts
    """
    counts = {"integer": 0, "string": 0, "date": 0}
    
    for field in fields:
        field_name = field.get("name")
        # For join queries, field may have 'collection' key
        field_collection = field.get("collection", collection)
        
        # Use type from field if available, otherwise infer from schema
        field_type = field.get("type", "").lower()
        
        if not field_type or field_type == "boolean":
            # Infer actual type from schema using calculator
            actual_type = calculator.infer_type(field_collection, field_name).lower()
            field_type = actual_type
        
        if field_type in ["integer", "number", "boolean"]:
            counts["integer"] += 1
        elif field_type == "string":
            counts["string"] += 1
        elif field_type == "date":
            counts["date"] += 1
    
    return counts


def extract_ram_vol_per_server(result: Dict, has_index: bool) -> float:
    """
    Extract RAM volume per server from query cost result.
    Formula: index * index_size + nb_pointers_per_working_server * collection_doc_size
    
    Args:
        result: Result dict from calculate_query_cost()
        has_index: Whether index exists
        
    Returns:
        RAM volume per server in bytes
    """
    index = 1 if has_index else 0
    index_size = 1_000_000  # Always 1.00E+06
    
    # Extract values from result dict structure
    distribution = result.get("distribution", {})
    res_q = distribution.get("res_q_results", distribution.get("nb_output", 0))
    S = distribution.get("S_servers", distribution.get("S", 1))
    size_doc = int(result["sizes"]["size_doc_bytes"].split()[0])
    
    nb_pointers_per_working_server = res_q / S if S > 0 else res_q
    ram_per_server = index * index_size + nb_pointers_per_working_server * size_doc
    
    return ram_per_server


def extract_projection_counts_by_type(fields: List[Dict]) -> Dict[str, int]:
    """
    Extract counts of projection fields by type.
    Uses the type from parsed field dict (already inferred from schema by query_parser).
    
    Args:
        fields: List of field dicts with 'name' and 'type' (types from parser)
    
    Returns:
        Dict with 'integer', 'string', 'date' counts
    """
    counts = {"integer": 0, "string": 0, "date": 0}
    
    for field in fields:
        # Use type from parsed field dict (already inferred from schema by parser)
        field_type = field.get("type", "integer").lower()
        
        if field_type in ["integer", "number", "boolean", "reference"]:
            counts["integer"] += 1
        elif field_type == "string":
            counts["string"] += 1
        elif field_type == "date":
            counts["date"] += 1
    
    return counts


def extract_query_characteristics(
    parsed_query: Dict,
    calculator: QueryCostCalculator
) -> Dict:
    """
    Extract query characteristics for display in table format.
    Uses calculator methods to get field counts and sizes.
    Supports filter, join, and aggregate queries.
    
    Args:
        parsed_query: Parsed query dict from parse_query()
        calculator: QueryCostCalculator instance
        
    Returns:
        Dict with field counts, nb_keys, query_size, output_size
    """
    query_type = parsed_query.get("query_type", "filter")
    
    if query_type == "join" or query_type == "join_aggregate":
        # Handle join queries
        collections = parsed_query["collections"]
        filter_fields = parsed_query.get("filter_fields", [])
        project_fields = parsed_query.get("project_fields", [])
        
        # Aggregate filter counts across all collections
        filter_counts = {"integer": 0, "string": 0, "date": 0}
        for field in filter_fields:
            field_collection = field.get("collection", collections[0] if collections else None)
            if field_collection:
                field_counts = extract_field_counts_by_type([field], field_collection, calculator)
                filter_counts["integer"] += field_counts["integer"]
                filter_counts["string"] += field_counts["string"]
                filter_counts["date"] += field_counts["date"]
        
        # Aggregate projection counts
        proj_counts = extract_projection_counts_by_type(project_fields)
        
        # For join queries, use first collection for size calculation (will be updated in cost calculator)
        primary_collection = collections[0] if collections else None
        if primary_collection:
            size_input, size_msg = calculator.calculate_join_sizes(
                collections,
                filter_fields,
                project_fields
            )
        else:
            size_input, size_msg = 0, 0
        
        nb_keys = len(filter_fields) + len(project_fields)
        
        result = {
            "filter_counts": filter_counts,
            "proj_counts": proj_counts,
            "nb_keys": nb_keys,
            "query_size": size_input,
            "output_size": size_msg
        }
        
        # Add aggregate-specific info if present
        if query_type == "join_aggregate" and "aggregate_functions" in parsed_query:
            result["aggregate_functions"] = parsed_query["aggregate_functions"]
            result["group_by_fields"] = parsed_query.get("group_by_fields", [])
        
        return result
    
    elif query_type == "aggregate":
        # Handle aggregate queries
        collection = parsed_query["collection"]
        filter_fields = parsed_query.get("filter_fields", [])
        project_fields = parsed_query.get("project_fields", [])
        aggregate_functions = parsed_query.get("aggregate_functions", [])
        group_by_fields = parsed_query.get("group_by_fields", [])
        
        # Get field counts
        filter_counts = extract_field_counts_by_type(filter_fields, collection, calculator)
        proj_counts = extract_projection_counts_by_type(project_fields)
        
        # Add aggregate function results to projection counts
        for agg_func in aggregate_functions:
            agg_type = agg_func.get("type", "integer").lower()
            if agg_type in ["integer", "number", "boolean"]:
                proj_counts["integer"] += 1
            elif agg_type == "string":
                proj_counts["string"] += 1
            elif agg_type == "date":
                proj_counts["date"] += 1
        
        # Calculate sizes (aggregate queries may have different output sizes)
        size_input, size_msg = calculator.calculate_query_sizes(
            collection,
            filter_fields,
            project_fields + [{"name": f["field"], "type": f["type"]} for f in aggregate_functions]
        )
        
        nb_keys = len(filter_fields) + len(project_fields) + len(aggregate_functions)
        
        return {
            "filter_counts": filter_counts,
            "proj_counts": proj_counts,
            "nb_keys": nb_keys,
            "query_size": size_input,
            "output_size": size_msg,
            "aggregate_functions": aggregate_functions,
            "group_by_fields": group_by_fields
        }
    
    else:
        # Handle filter queries (original logic)
        collection = parsed_query["collection"]
        filter_fields = parsed_query.get("filter_fields", [])
        project_fields = parsed_query.get("project_fields", [])
        
        # Get field counts - filter uses inference, projection uses type from dict only
        filter_counts = extract_field_counts_by_type(filter_fields, collection, calculator)
        proj_counts = extract_projection_counts_by_type(project_fields)
        
        # Calculate sizes using calculator method
        size_input, size_msg = calculator.calculate_query_sizes(
            collection,
            filter_fields,
            project_fields
        )
        
        # Calculate nb_keys (filter + projection)
        nb_keys = len(filter_fields) + len(project_fields)
        
        return {
            "filter_counts": filter_counts,
            "proj_counts": proj_counts,
            "nb_keys": nb_keys,
            "query_size": size_input,
            "output_size": size_msg
        }


def extract_cost_breakdown(
    result: Dict,
    sharding_key: str,
    has_index: bool
) -> Dict:
    """
    Extract cost breakdown from query cost result.
    All calculations come from the result dict returned by calculate_query_cost().
    Supports filter, join, and aggregate queries.
    
    Args:
        result: Result dict from calculate_query_cost()
        sharding_key: Sharding key used
        has_index: Whether index exists
    
    Returns:
        Dict with all cost breakdown metrics
    """
    query_type = result.get("query", {}).get("query_type", "filter")
    
    # Extract values from result (already calculated by QueryCostCalculator)
    S = result["distribution"]["S_servers"]
    size_input = int(result["sizes"]["size_input_bytes"].split()[0])
    size_msg = int(result["sizes"]["size_msg_bytes"].split()[0])
    nb_output = result["distribution"].get("res_q_results", result["distribution"].get("nb_docs", 0))
    vol_network = float(result["volumes"]["vol_network"].split()[0])
    vol_ram_total = float(result["volumes"]["vol_ram_total"].split()[0])
    time_cost = float(result["costs"]["time"].split()[0])
    co2_total = float(result["costs"]["carbon_total"].split()[0])
    # CO2 values are already in kgCO2eq (no conversion needed)
    co2_kg = co2_total
    budget = calculate_budget(vol_network)
    
    # Calculate RAM per server using helper function
    vol_ram_per_server = extract_ram_vol_per_server(result, has_index)
    
    # Get doc size
    size_doc = int(result["sizes"]["size_doc_bytes"].split()[0])
    
    breakdown = {
        "sharding_key": sharding_key or "None",
        "S": S,
        "size_input": size_input,
        "size_msg": size_msg,
        "nb_output": nb_output,
        "vol_network": vol_network,
        "vol_ram_per_server": vol_ram_per_server,
        "vol_ram_total": vol_ram_total,
        "time_cost": time_cost,
        "co2_kg": co2_kg,
        "budget": budget,
        "size_doc": size_doc,
        "has_index": has_index,
        "query_type": query_type
    }
    
    # Add join-specific metrics if present
    if query_type == "join" or query_type == "join_aggregate":
        breakdown["S_servers_outer"] = result["distribution"].get("S_servers_outer", S)
        breakdown["S_servers_inner"] = result["distribution"].get("S_servers_inner", S)
    
    # Add aggregate-specific metrics if present
    if query_type == "aggregate" or query_type == "join_aggregate":
        breakdown["nb_groups"] = result["distribution"].get("nb_groups", nb_output)
    
    return breakdown

