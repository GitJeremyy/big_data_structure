"""
Streamlit App for Query Cost Analysis
Allows users to input SQL queries and analyze costs with different sharding strategies
Uses only external calculation methods from query_cost.py and calculate_stats.py
Supports comprehensive manual value overrides with automatic recalculation
"""

import streamlit as st
import sys
from pathlib import Path
import os

# Add parent directory to path to import services
# Get the absolute path of the current file's parent's parent (project root)
_app_file = Path(__file__).resolve()
_project_root = _app_file.parent.parent

# Add project root to Python path if not already there
_project_root_str = str(_project_root)
if _project_root_str not in sys.path:
    sys.path.insert(0, _project_root_str)

# Verify services can be imported
try:
    import services
except ImportError:
    # If still can't import, try adding current directory
    _current_dir = Path.cwd()
    if str(_current_dir) not in sys.path:
        sys.path.insert(0, str(_current_dir))
    # Try parent of current directory
    _parent_dir = _current_dir.parent
    if str(_parent_dir) not in sys.path:
        sys.path.insert(0, str(_parent_dir))

from services.query_parser import parse_query
from services.query_cost import QueryCostCalculator
from services.statistics import Statistics
from services.calculate_stats import (
    format_scientific,
    calculate_budget,
    extract_query_characteristics,
    extract_cost_breakdown
)
from services.manual_counts_example import get_manual_counts_for_db
import pandas as pd


def get_nb_srv_working(S: int) -> int:
    """
    Get nb_srv_working constant based on number of servers.
    
    Args:
        S: Number of servers (sharding)
        
    Returns:
        nb_srv_working: 1 if S=1, 50 if S>1
    """
    if S == 1:
        return 1
    else:
        return 50  # Always 50 when S > 1


def calculate_ram_vol_total(S: int, vol_ram_per_server: float, has_index: bool = True) -> float:
    """
    Calculate RAM Vol (total) based on number of servers.
    
    - Case 1 (S=1): ram_vol_total = ram_vol_per_server
    - Case 2 (S>1): nb_srv_working * ram_vol_per_server + (S - nb_srv_working) * index * 1.00E+06
    
    Args:
        S: Number of servers (sharding)
        vol_ram_per_server: RAM volume per server
        has_index: Whether index exists (default True)
        
    Returns:
        RAM Vol (total)
    """
    if S == 1:
        return vol_ram_per_server
    else:
        nb_srv_working = get_nb_srv_working(S)  # Always 50 when S > 1
        index = 1 if has_index else 0
        multiplier = 1_000_000  # 1.00E+06
        return nb_srv_working * vol_ram_per_server + (S - nb_srv_working) * index * multiplier


def initialize_session_state():
    """Initialize session state for manual overrides"""
    if 'manual_overrides' not in st.session_state:
        st.session_state.manual_overrides = {
            'field_types': {},
            'field_counts': {},
            'doc_sizes': {},
            'query_values': {},
            'cost_values': {}
        }


def main():
    st.set_page_config(
        page_title="Query Cost Analyzer",
        page_icon="📊",
        layout="wide"
    )
    
    initialize_session_state()
    
    st.title("📊 Query Cost Analyzer")
    st.markdown("Analyze query costs with different sharding strategies")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        
        # Database selection
        db_signature = st.selectbox(
            "Database",
            ["DB1", "DB2", "DB3", "DB4", "DB5"],
            index=0,
            help="Select the database configuration"
        )
        
        # SQL Query input
        st.header("Query")
        sql_query = st.text_area(
            "SQL Query",
            value="SELECT S.IDP, S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW",
            height=100,
            help="Enter your SQL query"
        )
        
        # Sharding configuration
        st.header("Sharding")
        sharding_type = st.radio(
            "Sharding Type",
            ["With Sharding", "Without Sharding"],
            index=0
        )
        
        # Sharding key selection (only if with sharding)
        sharding_keys = []
        if sharding_type == "With Sharding":
            available_keys = ["IDP", "IDW", "IDC", "IDS", "date", "brand"]
            sharding_keys = st.multiselect(
                "Sharding Key(s)",
                available_keys,
                default=["IDP"],
                help="Select one or more sharding keys. If multiple, the first one will be used for routing."
            )
        
        # Index configuration
        has_index = st.checkbox("Has Index", value=True, help="Whether an index exists on filter fields")
        
        # Manual overrides section
        st.header("Manual Overrides")
        allow_manual_overrides = st.checkbox(
            "Allow Manual Value Overrides",
            value=False,
            help="Enable manual editing of any value. Edit values in tables and click 'Calculate Costs' to recalculate."
        )
        
        if allow_manual_overrides:
            st.info("💡 **Tip**: After editing values in the tables below, click 'Calculate Costs' again to see updated results.")
            if st.button("🔄 Clear All Overrides", help="Clear all manually overridden values"):
                if 'manual_overrides' in st.session_state:
                    st.session_state.manual_overrides = {
                        'field_types': {},
                        'field_counts': {},
                        'doc_sizes': {},
                        'query_values': {},
                        'cost_values': {}
                    }
                st.rerun()
        
        # Get available collections for this DB
        try:
            from services.schema_client import Schema
            import json
            from pathlib import Path
            schema_path = Path(__file__).parent.parent / 'services' / 'JSON_schema' / f'json-schema-{db_signature}.json'
            if schema_path.exists():
                with open(schema_path, 'r', encoding='utf-8') as f:
                    schema_data = json.load(f)
                schema = Schema(schema_data)
                er = schema.detect_entities_and_relations()
                available_collections = [e["name"] for e in er["entities"]]
            else:
                available_collections = ["Product", "Stock", "Warehouse", "OrderLine", "Client"]
        except:
            available_collections = ["Product", "Stock", "Warehouse", "OrderLine", "Client"]
        
        # Calculate button
        calculate_button = st.button("Calculate Costs", type="primary", use_container_width=True)
    
    # Main content area
    if calculate_button or (allow_manual_overrides and 'last_result' in st.session_state):
        if not sql_query.strip():
            st.error("Please enter a SQL query")
            return
        
        if sharding_type == "With Sharding" and not sharding_keys:
            st.error("Please select at least one sharding key when using 'With Sharding'")
            return
        
        try:
            # Step 1: Parse the SQL query
            with st.spinner("Parsing SQL query..."):
                parsed = parse_query(sql_query, db_signature=db_signature)
            
            query_type = parsed.get("query_type", "filter")
            st.success(f"✅ Query parsed successfully (Type: {query_type})")
            
            # Display query type information
            if query_type == "join" or query_type == "join_aggregate":
                st.info(f"🔗 **Join Query**: {', '.join(parsed.get('collections', []))}")
                if query_type == "join_aggregate":
                    st.info(f"📊 **Aggregate Functions**: {len(parsed.get('aggregate_functions', []))} function(s)")
            elif query_type == "aggregate":
                st.info(f"📊 **Aggregate Query** with {len(parsed.get('aggregate_functions', []))} aggregate function(s)")
            
            # Step 2: Allow manual field type editing if overrides enabled
            if allow_manual_overrides:
                st.subheader("✏️ Edit Field Types")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Filter Fields:**")
                    for idx, f in enumerate(parsed.get("filter_fields", [])):
                        field_name = f['name']
                        collection_name = f.get('collection', parsed.get('collection', 'Unknown'))
                        display_name = f"{collection_name}.{field_name}" if 'collection' in f else field_name
                        current_type = f.get('type', 'unknown')
                        field_key = f"filter_{field_name}_{idx}"
                        
                        if field_key not in st.session_state.manual_overrides['field_types']:
                            st.session_state.manual_overrides['field_types'][field_key] = current_type
                        
                        new_type = st.selectbox(
                            f"{display_name}",
                            ["integer", "string", "date", "longstring"],
                            index=["integer", "string", "date", "longstring"].index(
                                st.session_state.manual_overrides['field_types'].get(field_key, current_type)
                            ) if st.session_state.manual_overrides['field_types'].get(field_key, current_type) in ["integer", "string", "date", "longstring"] else 0,
                            key=f"type_filter_{field_key}"
                        )
                        st.session_state.manual_overrides['field_types'][field_key] = new_type
                        parsed["filter_fields"][idx]['type'] = new_type
                
                with col2:
                    st.write("**Project Fields:**")
                    for idx, f in enumerate(parsed.get("project_fields", [])):
                        field_name = f['name']
                        collection_name = f.get('collection', parsed.get('collection', 'Unknown'))
                        display_name = f"{collection_name}.{field_name}" if 'collection' in f else field_name
                        current_type = f.get('type', 'unknown')
                        field_key = f"project_{field_name}_{idx}"
                        
                        if field_key not in st.session_state.manual_overrides['field_types']:
                            st.session_state.manual_overrides['field_types'][field_key] = current_type
                        
                        new_type = st.selectbox(
                            f"{display_name}",
                            ["integer", "string", "date", "longstring"],
                            index=["integer", "string", "date", "longstring"].index(
                                st.session_state.manual_overrides['field_types'].get(field_key, current_type)
                            ) if st.session_state.manual_overrides['field_types'].get(field_key, current_type) in ["integer", "string", "date", "longstring"] else 0,
                            key=f"type_project_{field_key}"
                        )
                        st.session_state.manual_overrides['field_types'][field_key] = new_type
                        parsed["project_fields"][idx]['type'] = new_type
            
            # Step 3: Initialize calculator
            # Get manual counts and doc sizes from overrides if enabled
            manual_counts = None
            manual_doc_sizes = None
            
            if allow_manual_overrides:
                # Check if manual field counts are set
                if st.session_state.manual_overrides.get('field_counts'):
                    manual_counts = st.session_state.manual_overrides['field_counts']
                
                # Check if manual doc sizes are set
                if st.session_state.manual_overrides.get('doc_sizes'):
                    manual_doc_sizes = st.session_state.manual_overrides['doc_sizes']
            
            calculator = QueryCostCalculator(
                db_signature=db_signature,
                collection_size_file="results_TD1.json",
                manual_counts=manual_counts,
                manual_doc_sizes=manual_doc_sizes
            )
            
            # Step 4: Apply field type overrides before calculating characteristics
            if allow_manual_overrides:
                # Apply field type overrides to parsed query before calculating
                for idx, f in enumerate(parsed.get("filter_fields", [])):
                    field_key = f"filter_{f['name']}_{idx}"
                    if field_key in st.session_state.manual_overrides.get('field_types', {}):
                        parsed["filter_fields"][idx]['type'] = st.session_state.manual_overrides['field_types'][field_key]
                
                for idx, f in enumerate(parsed.get("project_fields", [])):
                    field_key = f"project_{f['name']}_{idx}"
                    if field_key in st.session_state.manual_overrides.get('field_types', {}):
                        parsed["project_fields"][idx]['type'] = st.session_state.manual_overrides['field_types'][field_key]
            
            # Step 5: Extract query characteristics
            query_chars = extract_query_characteristics(parsed, calculator)
            
            # Step 5: Apply stored overrides if they exist
            if allow_manual_overrides and 'query_char_overrides' in st.session_state.manual_overrides:
                override_data = st.session_state.manual_overrides['query_char_overrides']
                query_chars["filter_counts"]["integer"] = override_data.get('filter_integer', query_chars["filter_counts"]["integer"])
                query_chars["filter_counts"]["string"] = override_data.get('filter_string', query_chars["filter_counts"]["string"])
                query_chars["filter_counts"]["date"] = override_data.get('filter_date', query_chars["filter_counts"]["date"])
                query_chars["proj_counts"]["integer"] = override_data.get('proj_integer', query_chars["proj_counts"]["integer"])
                query_chars["proj_counts"]["string"] = override_data.get('proj_string', query_chars["proj_counts"]["string"])
                query_chars["proj_counts"]["date"] = override_data.get('proj_date', query_chars["proj_counts"]["date"])
                query_chars["nb_keys"] = override_data.get('nb_keys', query_chars["nb_keys"])
                query_chars["query_size"] = override_data.get('query_size', query_chars["query_size"])
                query_chars["output_size"] = override_data.get('output_size', query_chars["output_size"])
            
            # Step 7: Allow manual override of query characteristics
            if allow_manual_overrides:
                st.subheader("✏️ Edit Query Characteristics")
                
                # Initialize override values if not present
                if 'query_char_overrides' not in st.session_state.manual_overrides:
                    st.session_state.manual_overrides['query_char_overrides'] = {
                        'filter_integer': query_chars["filter_counts"]["integer"],
                        'filter_string': query_chars["filter_counts"]["string"],
                        'filter_date': query_chars["filter_counts"]["date"],
                        'proj_integer': query_chars["proj_counts"]["integer"],
                        'proj_string': query_chars["proj_counts"]["string"],
                        'proj_date': query_chars["proj_counts"]["date"],
                        'nb_keys': query_chars["nb_keys"],
                        'query_size': query_chars["query_size"],
                        'output_size': query_chars["output_size"]
                    }
                
                # Create editable table
                override_data = st.session_state.manual_overrides['query_char_overrides']
                
                col1, col2 = st.columns(2)
                with col1:
                    override_data['filter_integer'] = st.number_input(
                        "Filter - Integer", 
                        min_value=0, 
                        value=int(override_data.get('filter_integer', query_chars["filter_counts"]["integer"])),
                        key="override_filter_int"
                    )
                    override_data['filter_string'] = st.number_input(
                        "Filter - String", 
                        min_value=0, 
                        value=int(override_data.get('filter_string', query_chars["filter_counts"]["string"])),
                        key="override_filter_str"
                    )
                    override_data['filter_date'] = st.number_input(
                        "Filter - Date", 
                        min_value=0, 
                        value=int(override_data.get('filter_date', query_chars["filter_counts"]["date"])),
                        key="override_filter_date"
                    )
                    override_data['proj_integer'] = st.number_input(
                        "Projection - Integer", 
                        min_value=0, 
                        value=int(override_data.get('proj_integer', query_chars["proj_counts"]["integer"])),
                        key="override_proj_int"
                    )
                    override_data['proj_string'] = st.number_input(
                        "Projection - String", 
                        min_value=0, 
                        value=int(override_data.get('proj_string', query_chars["proj_counts"]["string"])),
                        key="override_proj_str"
                    )
                    override_data['proj_date'] = st.number_input(
                        "Projection - Date", 
                        min_value=0, 
                        value=int(override_data.get('proj_date', query_chars["proj_counts"]["date"])),
                        key="override_proj_date"
                    )
                
                with col2:
                    override_data['nb_keys'] = st.number_input(
                        "Nb Keys", 
                        min_value=0, 
                        value=int(override_data.get('nb_keys', query_chars["nb_keys"])),
                        key="override_nb_keys"
                    )
                    override_data['query_size'] = st.number_input(
                        "Query Size (bytes)", 
                        min_value=0, 
                        value=int(override_data.get('query_size', query_chars["query_size"])),
                        key="override_query_size"
                    )
                    override_data['output_size'] = st.number_input(
                        "Output Size (bytes)", 
                        min_value=0, 
                        value=int(override_data.get('output_size', query_chars["output_size"])),
                        key="override_output_size"
                    )
                
                # Update query_chars with overrides
                query_chars["filter_counts"]["integer"] = override_data['filter_integer']
                query_chars["filter_counts"]["string"] = override_data['filter_string']
                query_chars["filter_counts"]["date"] = override_data['filter_date']
                query_chars["proj_counts"]["integer"] = override_data['proj_integer']
                query_chars["proj_counts"]["string"] = override_data['proj_string']
                query_chars["proj_counts"]["date"] = override_data['proj_date']
                query_chars["nb_keys"] = override_data['nb_keys']
                query_chars["query_size"] = override_data['query_size']
                query_chars["output_size"] = override_data['output_size']
            
            # ============================================================
            # TABLE 1: Query Characteristics
            # ============================================================
            st.header("📐 Query Characteristics")
            
            # Create DataFrame for query characteristics
            query_char_data = {
                "": ["Base Values", "Query Values"],
                "filter - integer": [8, query_chars["filter_counts"]["integer"]],
                "filter - string": [80, query_chars["filter_counts"]["string"]],
                "filter - date": [20, query_chars["filter_counts"]["date"]],
                "Projection keys (output) - integer": [8, query_chars["proj_counts"]["integer"]],
                "Projection keys (output) - string": [80, query_chars["proj_counts"]["string"]],
                "Projection keys (output) - date": [20, query_chars["proj_counts"]["date"]],
                "nb keys": [12, query_chars["nb_keys"]],
                "query size": ["", query_chars["query_size"]],
                "output size": ["", query_chars["output_size"]]
            }
            
            df_query = pd.DataFrame(query_char_data)
            
            # Make table editable if overrides enabled
            if allow_manual_overrides:
                edited_df = st.data_editor(
                    df_query,
                    use_container_width=True,
                    hide_index=True,
                    num_rows="fixed",
                    column_config={
                        "": st.column_config.TextColumn("", width="small", disabled=True),
                        "filter - integer": st.column_config.NumberColumn("filter - integer", width="small"),
                        "filter - string": st.column_config.NumberColumn("filter - string", width="small"),
                        "filter - date": st.column_config.NumberColumn("filter - date", width="small"),
                        "Projection keys (output) - integer": st.column_config.NumberColumn("Projection keys (output) - integer", width="medium"),
                        "Projection keys (output) - string": st.column_config.NumberColumn("Projection keys (output) - string", width="medium"),
                        "Projection keys (output) - date": st.column_config.NumberColumn("Projection keys (output) - date", width="medium"),
                        "nb keys": st.column_config.NumberColumn("nb keys", width="small"),
                        "query size": st.column_config.NumberColumn("query size", width="small"),
                        "output size": st.column_config.NumberColumn("output size", width="small"),
                    },
                    key="query_char_editor"
                )
                
                # Update query_chars from edited dataframe and store in session state
                if not edited_df.empty and len(edited_df) >= 2:
                    new_values = {
                        'filter_integer': int(edited_df.iloc[1]["filter - integer"]),
                        'filter_string': int(edited_df.iloc[1]["filter - string"]),
                        'filter_date': int(edited_df.iloc[1]["filter - date"]),
                        'proj_integer': int(edited_df.iloc[1]["Projection keys (output) - integer"]),
                        'proj_string': int(edited_df.iloc[1]["Projection keys (output) - string"]),
                        'proj_date': int(edited_df.iloc[1]["Projection keys (output) - date"]),
                        'nb_keys': int(edited_df.iloc[1]["nb keys"]),
                        'query_size': int(edited_df.iloc[1]["query size"]),
                        'output_size': int(edited_df.iloc[1]["output size"])
                    }
                    # Store in session state
                    st.session_state.manual_overrides['query_char_overrides'] = new_values
                    # Update query_chars
                    query_chars["filter_counts"]["integer"] = new_values['filter_integer']
                    query_chars["filter_counts"]["string"] = new_values['filter_string']
                    query_chars["filter_counts"]["date"] = new_values['filter_date']
                    query_chars["proj_counts"]["integer"] = new_values['proj_integer']
                    query_chars["proj_counts"]["string"] = new_values['proj_string']
                    query_chars["proj_counts"]["date"] = new_values['proj_date']
                    query_chars["nb_keys"] = new_values['nb_keys']
                    query_chars["query_size"] = new_values['query_size']
                    query_chars["output_size"] = new_values['output_size']
            else:
                st.dataframe(
                    df_query,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "": st.column_config.TextColumn("", width="small"),
                        "filter - integer": st.column_config.NumberColumn("filter - integer", width="small"),
                        "filter - string": st.column_config.NumberColumn("filter - string", width="small"),
                        "filter - date": st.column_config.NumberColumn("filter - date", width="small"),
                        "Projection keys (output) - integer": st.column_config.NumberColumn("Projection keys (output) - integer", width="medium"),
                        "Projection keys (output) - string": st.column_config.NumberColumn("Projection keys (output) - string", width="medium"),
                        "Projection keys (output) - date": st.column_config.NumberColumn("Projection keys (output) - date", width="medium"),
                        "nb keys": st.column_config.NumberColumn("nb keys", width="small"),
                        "query size": st.column_config.NumberColumn("query size", width="small"),
                        "output size": st.column_config.NumberColumn("output size", width="small"),
                    }
                )
            
            # ============================================================
            # Calculate costs for each sharding key
            # ============================================================
            all_results = []
            if sharding_type == "Without Sharding":
                keys_to_process = [None]
            else:
                keys_to_process = sharding_keys if sharding_keys else [None]
            
            for idx_key, sharding_key in enumerate(keys_to_process):
                with st.spinner(f"Calculating costs for sharding key: {sharding_key or 'None'}..."):
                    # Build query dict based on query type
                    if query_type == "join" or query_type == "join_aggregate":
                        query_dict = {
                            "query_type": query_type,
                            "collections": parsed["collections"],
                            "join_conditions": parsed.get("join_conditions", []),
                            "filter_fields": parsed.get("filter_fields", []),
                            "project_fields": parsed.get("project_fields", []),
                            "sharding_key": sharding_key,
                            "has_index": has_index,
                        }
                        if query_type == "join_aggregate":
                            query_dict["aggregate_functions"] = parsed.get("aggregate_functions", [])
                            query_dict["group_by_fields"] = parsed.get("group_by_fields", [])
                    elif query_type == "aggregate":
                        query_dict = {
                            "query_type": "aggregate",
                            "collection": parsed["collection"],
                            "filter_fields": parsed.get("filter_fields", []),
                            "project_fields": parsed.get("project_fields", []),
                            "aggregate_functions": parsed.get("aggregate_functions", []),
                            "group_by_fields": parsed.get("group_by_fields", []),
                            "sharding_key": sharding_key,
                            "has_index": has_index,
                        }
                    else:
                        query_dict = {
                            **parsed,
                            "sharding_key": sharding_key,
                            "has_index": has_index,
                            "index_size": 1_000_000  # Always 1.00E+06
                        }
                    
                    result = calculator.calculate_query_cost(query_dict)
                    
                    # Apply manual overrides to result BEFORE extracting cost breakdown
                    if allow_manual_overrides:
                        # Override query sizes if manually set
                        if 'query_char_overrides' in st.session_state.manual_overrides:
                            override_data = st.session_state.manual_overrides['query_char_overrides']
                            if 'query_size' in override_data:
                                result["sizes"]["size_input_bytes"] = f"{override_data['query_size']} B"
                            if 'output_size' in override_data:
                                result["sizes"]["size_msg_bytes"] = f"{override_data['output_size']} B"
                    
                    cost_breakdown = extract_cost_breakdown(result, sharding_key, has_index)
                    
                    # Apply stored cost overrides if they exist (from previous edits)
                    if allow_manual_overrides:
                        cost_key = f"cost_overrides_{idx_key}"
                        if cost_key in st.session_state.manual_overrides:
                            cost_overrides = st.session_state.manual_overrides[cost_key]
                            # Apply overrides to cost_breakdown
                            if 'S' in cost_overrides:
                                cost_breakdown["S"] = cost_overrides['S']
                            if 'size_input' in cost_overrides:
                                cost_breakdown["size_input"] = cost_overrides['size_input']
                            if 'size_msg' in cost_overrides:
                                cost_breakdown["size_msg"] = cost_overrides['size_msg']
                            if 'nb_output' in cost_overrides:
                                cost_breakdown["nb_output"] = cost_overrides['nb_output']
                            if 'size_doc' in cost_overrides:
                                cost_breakdown["size_doc"] = cost_overrides['size_doc']
                            
                            # Recalculate derived values from overrides
                            cost_breakdown["vol_network"] = cost_breakdown["S"] * cost_breakdown["size_input"] + cost_breakdown["nb_output"] * cost_breakdown["size_msg"]
                            
                            # Recalculate RAM volumes
                            index_val = 1 if has_index else 0
                            index_size = 1_000_000
                            nb_pointers = cost_breakdown["nb_output"] / cost_breakdown["S"] if cost_breakdown["S"] > 0 else cost_breakdown["nb_output"]
                            cost_breakdown["vol_ram_per_server"] = index_val * index_size + nb_pointers * cost_breakdown["size_doc"]
                            cost_breakdown["vol_ram_total"] = calculate_ram_vol_total(cost_breakdown["S"], cost_breakdown["vol_ram_per_server"], has_index)
                            
                            # Recalculate time and costs
                            vol_network = cost_breakdown["vol_network"]
                            vol_ram_total = cost_breakdown["vol_ram_total"]
                            # Time uses ram_vol_per_server, not ram_vol_total
                            cost_breakdown["time_cost"] = vol_network / Statistics.BANDWIDTH_NETWORK + cost_breakdown["vol_ram_per_server"] / Statistics.BANDWIDTH_RAM
                            cost_breakdown["co2_kg"] = vol_network * Statistics.CO2_NETWORK + vol_ram_total * Statistics.CO2_RAM
                            cost_breakdown["budget"] = calculate_budget(vol_network)
                    
                    # Apply stored cost overrides if they exist
                    if allow_manual_overrides:
                        cost_key = f"cost_overrides_{idx}"
                        if cost_key in st.session_state.manual_overrides:
                            cost_overrides = st.session_state.manual_overrides[cost_key]
                            if 'S' in cost_overrides:
                                cost_breakdown["S"] = cost_overrides['S']
                            if 'size_input' in cost_overrides:
                                cost_breakdown["size_input"] = cost_overrides['size_input']
                            if 'size_msg' in cost_overrides:
                                cost_breakdown["size_msg"] = cost_overrides['size_msg']
                            if 'nb_output' in cost_overrides:
                                cost_breakdown["nb_output"] = cost_overrides['nb_output']
                            if 'size_doc' in cost_overrides:
                                cost_breakdown["size_doc"] = cost_overrides['size_doc']
                            if 'vol_ram_per_server' in cost_overrides:
                                cost_breakdown["vol_ram_per_server"] = cost_overrides['vol_ram_per_server']
                            
                            # Recalculate derived values
                            cost_breakdown["vol_network"] = cost_breakdown["S"] * cost_breakdown["size_input"] + cost_breakdown["nb_output"] * cost_breakdown["size_msg"]
                            
                            # Recalculate RAM volumes
                            index_val = 1 if has_index else 0
                            index_size = 1_000_000
                            nb_pointers = cost_breakdown["nb_output"] / cost_breakdown["S"] if cost_breakdown["S"] > 0 else cost_breakdown["nb_output"]
                            cost_breakdown["vol_ram_per_server"] = index_val * index_size + nb_pointers * cost_breakdown["size_doc"]
                            cost_breakdown["vol_ram_total"] = calculate_ram_vol_total(cost_breakdown["S"], cost_breakdown["vol_ram_per_server"], has_index)
                            
                            # Recalculate time and costs
                            vol_network = cost_breakdown["vol_network"]
                            vol_ram_total = cost_breakdown["vol_ram_total"]
                            # Time uses ram_vol_per_server, not ram_vol_total
                            cost_breakdown["time_cost"] = vol_network / Statistics.BANDWIDTH_NETWORK + cost_breakdown["vol_ram_per_server"] / Statistics.BANDWIDTH_RAM
                            cost_breakdown["co2_kg"] = vol_network * Statistics.CO2_NETWORK + vol_ram_total * Statistics.CO2_RAM
                            cost_breakdown["budget"] = calculate_budget(vol_network)
                    cost_breakdown["result"] = result
                    
                    all_results.append(cost_breakdown)
            
            # Store results for recalculation
            st.session_state.last_result = all_results
            st.session_state.last_parsed = parsed
            st.session_state.last_query_chars = query_chars
            
            # ============================================================
            # TABLE 2: Cost Breakdown for each sharding key
            # ============================================================
            st.header("💰 Cost Breakdown")
            
            # Get collection(s) based on query type
            if query_type == "join" or query_type == "join_aggregate":
                collections_display = ", ".join(parsed.get("collections", []))
            else:
                collections_display = parsed.get("collection", "Unknown")
            
            for idx, res in enumerate(all_results):
                sharding_key = res["sharding_key"]
                
                if len(all_results) > 1:
                    st.subheader(f"Sharding Key: {sharding_key}")
                
                # Display in sections
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write("**Database:**", db_signature)
                with col2:
                    st.write("**Query:**", query_type.upper())
                with col3:
                    st.write("**Collection(s):**", collections_display)
                with col4:
                    st.write("**Algo:**", "Index scan" if has_index else "Full scan")
                
                # --------------------------------------------------------
                # For JOIN queries: show subquery 1, subquery 2, then combined
                # --------------------------------------------------------
                if query_type in ("join", "join_aggregate"):
                    base_result = res.get("result", {})
                    op1 = base_result.get("op1")
                    op2 = base_result.get("op2")
                    nb_iter_op2 = base_result.get("nb_iter_op2", None)
                    
                    if op1 is not None:
                        st.markdown("##### 🔹 Subquery 1 (driving filter)")
                        op1_breakdown = extract_cost_breakdown(
                            op1,
                            op1.get("query", {}).get("sharding_key"),
                            has_index
                        )
                        df_op1 = pd.DataFrame({
                            "Metric": ["S", "query size", "output size", "nb output", "Network Vol (B)", "RAM Vol (total)", "Time (s)", "kgCO2eq", "Budget (€)"],
                            "Value": [
                                str(op1_breakdown["S"]),
                                str(op1_breakdown["size_input"]),
                                str(op1_breakdown["size_msg"]),
                                f"{op1_breakdown['nb_output']:.2e}",
                                format_scientific(op1_breakdown["vol_network"]),
                                format_scientific(op1_breakdown["vol_ram_total"]),
                                format_scientific(op1_breakdown["time_cost"]),
                                format_scientific(op1_breakdown["co2_kg"]),
                                format_scientific(op1_breakdown["budget"]),
                            ],
                        })
                        st.dataframe(df_op1, use_container_width=True, hide_index=True)
                    
                    if op2 is not None:
                        st.markdown("##### 🔹 Subquery 2 (point lookup, per iteration)")
                        op2_breakdown = extract_cost_breakdown(
                            op2,
                            op2.get("query", {}).get("sharding_key"),
                            has_index
                        )
                        # Get CO2 and budget from base_result if available (per iteration)
                        op2_carbon = base_result.get("op2_carbon_total", op2_breakdown["co2_kg"])
                        op2_budget = base_result.get("op2_budget", op2_breakdown["budget"])
                        
                        df_op2 = pd.DataFrame({
                            "Metric": ["S", "query size", "output size", "nb output (per iter)", "Network Vol (B, per iter)", "RAM Vol (total, per iter)", "Time (s, per iter)", "kgCO2eq (per iter)", "Budget (€, per iter)"],
                            "Value": [
                                str(op2_breakdown["S"]),
                                str(op2_breakdown["size_input"]),
                                str(op2_breakdown["size_msg"]),
                                f"{op2_breakdown['nb_output']:.2e}",
                                format_scientific(op2_breakdown["vol_network"]),
                                format_scientific(op2_breakdown["vol_ram_total"]),
                                format_scientific(op2_breakdown["time_cost"]),
                                format_scientific(op2_carbon),
                                format_scientific(op2_budget),
                            ],
                        })
                        st.dataframe(df_op2, use_container_width=True, hide_index=True)
                        
                        if nb_iter_op2 is not None:
                            st.info(f"Subquery 2 is executed **{format_scientific(nb_iter_op2)}** times. Combined costs below already include this factor.")
                
                # Network section
                st.markdown("#### 📡 Network")
                network_data = {
                    "Sharding Key": [res["sharding_key"]],
                    "Sharding (S)": [res["S"]],
                    "query size": [res["size_input"]],
                    "nb output": [f"{res['nb_output']:.2e}"],
                    "output size": [res["size_msg"]],
                    "Network Vol (B)": [format_scientific(res["vol_network"])]
                }
                df_network = pd.DataFrame(network_data)
                
                if allow_manual_overrides:
                    edited_network = st.data_editor(
                        df_network,
                        use_container_width=True,
                        hide_index=True,
                        num_rows="fixed",
                        key=f"network_editor_{idx}",
                        column_config={
                            "Sharding Key": st.column_config.TextColumn("Sharding Key", disabled=True),
                            "Sharding (S)": st.column_config.NumberColumn("Sharding (S)", width="small"),
                            "query size": st.column_config.NumberColumn("query size", width="small"),
                            "nb output": st.column_config.TextColumn("nb output", width="small"),
                            "output size": st.column_config.NumberColumn("output size", width="small"),
                            "Network Vol (B)": st.column_config.TextColumn("Network Vol (B)", width="medium"),
                        }
                    )
                    # Update values from edited dataframe and store in session state
                    if not edited_network.empty:
                        cost_key = f"cost_overrides_{idx}"
                        if cost_key not in st.session_state.manual_overrides:
                            st.session_state.manual_overrides[cost_key] = {}
                        
                        new_S = int(edited_network.iloc[0]["Sharding (S)"])
                        new_size_input = int(edited_network.iloc[0]["query size"])
                        new_size_msg = int(edited_network.iloc[0]["output size"])
                        
                        # Store overrides
                        st.session_state.manual_overrides[cost_key]['S'] = new_S
                        st.session_state.manual_overrides[cost_key]['size_input'] = new_size_input
                        st.session_state.manual_overrides[cost_key]['size_msg'] = new_size_msg
                        
                        # Update res dict
                        res["S"] = new_S
                        res["size_input"] = new_size_input
                        res["size_msg"] = new_size_msg
                        # Recalculate network volume
                        res["vol_network"] = res["S"] * res["size_input"] + res["nb_output"] * res["size_msg"]
                        
                        # Show message that recalculation will happen on next "Calculate Costs" click
                        st.info("💡 Changes saved. Click 'Calculate Costs' again to see updated results.")
                else:
                    st.dataframe(df_network, use_container_width=True, hide_index=True)
                
                # RAM / Server Side section
                st.markdown("#### 💾 RAM / Server Side")
                nb_pointers_per_working = res["nb_output"] / res["S"] if res["S"] > 0 else res["nb_output"]
                # Calculate nb_srv_working: 1 if S=1, 50 if S>1
                nb_srv_working = get_nb_srv_working(res["S"])
                ram_data = {
                    "Index": [1 if res["has_index"] else 0],
                    "Local Index": [res["sharding_key"]],
                    "Nb pointers per working": [nb_pointers_per_working],
                    "collection doc size": [res["size_doc"]],
                    "RAM Vol per server": [format_scientific(res["vol_ram_per_server"])],
                    "nb srv working": [nb_srv_working],
                    "RAM Vol (total)": [format_scientific(res["vol_ram_total"])]
                }
                df_ram = pd.DataFrame(ram_data)
                
                if allow_manual_overrides:
                    edited_ram = st.data_editor(
                        df_ram,
                        use_container_width=True,
                        hide_index=True,
                        num_rows="fixed",
                        key=f"ram_editor_{idx}",
                        column_config={
                            "Index": st.column_config.NumberColumn("Index", width="small"),
                            "Local Index": st.column_config.TextColumn("Local Index", width="small", disabled=True),
                            "Nb pointers per working": st.column_config.NumberColumn("Nb pointers per working", width="medium"),
                            "collection doc size": st.column_config.NumberColumn("collection doc size", width="medium"),
                            "RAM Vol per server": st.column_config.TextColumn("RAM Vol per server", width="medium"),
                            "nb srv working": st.column_config.NumberColumn("nb srv working", width="small"),
                            "RAM Vol (total)": st.column_config.TextColumn("RAM Vol (total)", width="medium"),
                        }
                    )
                    # Update values and recalculate
                    if not edited_ram.empty:
                        cost_key = f"cost_overrides_{idx}"
                        if cost_key not in st.session_state.manual_overrides:
                            st.session_state.manual_overrides[cost_key] = {}
                        
                        index_val = int(edited_ram.iloc[0]["Index"])
                        nb_pointers = float(edited_ram.iloc[0]["Nb pointers per working"])
                        doc_size = int(edited_ram.iloc[0]["collection doc size"])
                        nb_srv = int(edited_ram.iloc[0]["nb srv working"])
                        
                        # Store overrides
                        st.session_state.manual_overrides[cost_key]['size_doc'] = doc_size
                        st.session_state.manual_overrides[cost_key]['S'] = nb_srv
                        # Calculate nb_output from nb_pointers
                        new_nb_output = nb_pointers * nb_srv
                        st.session_state.manual_overrides[cost_key]['nb_output'] = new_nb_output
                        
                        # Recalculate RAM volume per server
                        index_size = 1_000_000
                        vol_ram_per_server = index_val * index_size + nb_pointers * doc_size
                        res["vol_ram_per_server"] = vol_ram_per_server
                        # has_index is determined by index_val (1 if has_index, 0 if not)
                        has_index_for_calc = (index_val == 1)
                        res["vol_ram_total"] = calculate_ram_vol_total(nb_srv, vol_ram_per_server, has_index_for_calc)
                        res["size_doc"] = doc_size
                        res["S"] = nb_srv
                        res["nb_output"] = new_nb_output
                        
                        # Recalculate network volume with new nb_output
                        res["vol_network"] = res["S"] * res["size_input"] + res["nb_output"] * res["size_msg"]
                        
                        # Recalculate time and costs
                        # Time uses ram_vol_per_server, not ram_vol_total
                        res["time_cost"] = res["vol_network"] / Statistics.BANDWIDTH_NETWORK + res["vol_ram_per_server"] / Statistics.BANDWIDTH_RAM
                        res["co2_kg"] = res["vol_network"] * Statistics.CO2_NETWORK + res["vol_ram_total"] * Statistics.CO2_RAM
                        res["budget"] = calculate_budget(res["vol_network"])
                        
                        # Show message that recalculation will happen on next "Calculate Costs" click
                        st.info("💡 Changes saved. Click 'Calculate Costs' again to see updated results.")
                else:
                    st.dataframe(df_ram, use_container_width=True, hide_index=True)
                
                # Costs section
                st.markdown("#### 💰 Costs")
                costs_data = {
                    "Time (s)": [format_scientific(res["time_cost"])],
                    "kgCO2eq": [format_scientific(res["co2_kg"])],
                    "Budget (€)": [format_scientific(res["budget"])]
                }
                df_costs_final = pd.DataFrame(costs_data)
                
                if allow_manual_overrides:
                    edited_costs = st.data_editor(
                        df_costs_final,
                        use_container_width=True,
                        hide_index=True,
                        num_rows="fixed",
                        key=f"costs_editor_{idx}",
                        column_config={
                            "Time (s)": st.column_config.TextColumn("Time (s)", width="small"),
                            "kgCO2eq": st.column_config.TextColumn("kgCO2eq", width="small"),
                            "Budget (€)": st.column_config.TextColumn("Budget (€)", width="small"),
                        }
                    )
                else:
                    st.dataframe(df_costs_final, use_container_width=True, hide_index=True)
                
                if idx < len(all_results) - 1:
                    st.divider()
            
            # ============================================================
            # Comparison Table (if multiple sharding keys)
            # ============================================================
            if len(all_results) > 1:
                st.header("📊 Comparison: Multiple Sharding Keys")
                
                comparison_data = {
                    "Sharding Key": [r["sharding_key"] for r in all_results],
                    "S (Servers)": [r["S"] for r in all_results],
                    "Network Vol (B)": [format_scientific(r["vol_network"]) for r in all_results],
                    "RAM Vol (total)": [format_scientific(r["vol_ram_total"]) for r in all_results],
                    "Time (s)": [format_scientific(r["time_cost"]) for r in all_results],
                    "kgCO2eq": [format_scientific(r["co2_kg"]) for r in all_results],
                    "Budget (€)": [format_scientific(r["budget"]) for r in all_results]
                }
                
                df_comparison = pd.DataFrame(comparison_data)
                st.dataframe(df_comparison, use_container_width=True, hide_index=True)
            
            # ============================================================
            # Summary Metrics
            # ============================================================
            if len(all_results) == 1:
                res = all_results[0]
                st.header("📊 Summary Metrics")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Network Volume", format_scientific(res["vol_network"]) + " B")
                
                with col2:
                    st.metric("RAM Volume (Total)", format_scientific(res["vol_ram_total"]) + " B")
                
                with col3:
                    st.metric("Time Cost", format_scientific(res["time_cost"]) + " s")
                
                with col4:
                    st.metric("CO2 Impact", format_scientific(res["co2_kg"]) + " kgCO2eq")
                
                col5, col6 = st.columns(2)
                
                with col5:
                    st.metric("Budget Cost", format_scientific(res["budget"]) + " €")
                
                with col6:
                    st.metric("Servers Accessed", res["S"])
            
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.exception(e)
    
    else:
        # Show instructions when no calculation has been performed
        st.info("👈 Configure your query in the sidebar and click 'Calculate Costs' to see the analysis")
        
        st.markdown("""
        ### How to use:
        1. **Select Database**: Choose DB1-DB5 configuration
        2. **Enter SQL Query**: Type your SQL query (supports SELECT-FROM-WHERE and JOIN)
        3. **Configure Sharding**: 
           - Choose "With Sharding" or "Without Sharding"
           - If with sharding, select one or more sharding keys
        4. **Set Index**: Check if an index exists on filter fields
        5. **Manual Overrides** (optional): Enable to edit any value and see instant recalculation
        6. **Calculate**: Click the button to see detailed cost analysis
        
        ### Example Queries:
        - **Q1**: `SELECT S.IDP, S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW`
        - **Q2**: `SELECT P.IDP, P.name, P.price FROM Product P WHERE P.brand = $brand`
        - **Q3**: `SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = $date`
        """)


if __name__ == "__main__":
    main()
