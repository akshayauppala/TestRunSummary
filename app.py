"""
Streamlit application for InfluxDB Flux Query Generator.
Uses design patterns: Configuration Management, Factory Pattern, Service Layer.
"""
import streamlit as st
import pandas as pd
import re
from typing import Optional
from config import config
from services import OpenAIQueryGenerationService
from summary_service import SummaryService

# =========================
# STREAMLIT UI
# =========================

st.set_page_config(page_title="InfluxDB Flux Query Generator", page_icon="", layout="wide")

if "result" not in st.session_state:
    st.session_state.result = None

# Example queries
with st.expander(" Query Examples - All Auto-Generated"):
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Basic (staging, last 24h):**")
        st.markdown("""
        - Show failed tests
        - Count tests by status
        - Top 10 slowest tests
        - Tests by Akshaya
        """)
    
    with col2:
        st.markdown("**Environment Specific:**")
        st.markdown("""
        - Show failed tests in CSE
        - Count tests by owner in production
        - Flaky tests in staging
        """)
        
        st.markdown("**Historical (all time):**")
        st.markdown("""
        - Show all time flaky tests
        - Performance metrics from beginning
        """)
    
    with col3:
        st.markdown("**Build Comparison:**")
        st.markdown("""
        - Compare build X vs Y
        - Tests failing only today
        - New failures in current build
        """)
        
        st.markdown("**Advanced:**")
        st.markdown("""
        - Tests slower than 30s
        - Failed tests with error messages
        """)

# Main input
user_query = st.text_input(
    "Ask anything about your test execution data",
    placeholder="e.g., Show me failed tests in CSE environment",
    key="user_input"
)

generate_button = st.button("Generate & Validate", type="primary", use_container_width=True)

# Helper function to detect summary requests
def detect_summary_request(query: str) -> tuple[str, Optional[str]]:
    """
    Detect if the query is asking for a summary.
    Returns: (summary_type, script_name_or_execution)
    Summary types: 'build_summary', 'script_summary', 'flaky_summary', None
    """
    query_lower = query.lower()
    
    # Build summary detection - MUST come before script summary to avoid conflicts
    build_summary_patterns = [
        r"(?:give\s+me\s+)?about\s+build\s+(\d+)",
        r"(?:give\s+me\s+)?about\s+execution\s+(\d+)",
        r"(?:tell\s+me\s+)?about\s+build\s+(\d+)",
        r"(?:tell\s+me\s+)?about\s+execution\s+(\d+)",
        r"build\s+summary",
        r"summary\s+of\s+build",
        r"build\s+(\d+)\s+summary",
        r"execution\s+(\d+)\s+summary",
        r"latest\s+build\s+summary",
        r"current\s+build\s+summary",
        r"show\s+me\s+build\s+(\d+)",
        r"show\s+me\s+execution\s+(\d+)",
        r"info\s+about\s+build\s+(\d+)",
        r"info\s+about\s+execution\s+(\d+)"
    ]
    for pattern in build_summary_patterns:
        match = re.search(pattern, query_lower)
        if match:
            # Extract execution number if mentioned
            if match.groups():
                exec_num = match.group(1)
            else:
                exec_match = re.search(r"(?:build|execution)\s+(\d+)", query_lower)
                exec_num = exec_match.group(1) if exec_match else None
            return ("build_summary", exec_num)
    
    # Script summary detection - improved to handle various formats
    # First, try patterns with explicit keywords
    script_patterns = [
        r"about\s+(?:script|test)\s+([A-Za-z][A-Za-z0-9_]*)",
        r"summary\s+of\s+(?:script|test)\s+([A-Za-z][A-Za-z0-9_]*)",
        r"analyze\s+(?:script|test)\s+([A-Za-z][A-Za-z0-9_]*)",
        r"tell\s+me\s+about\s+([A-Za-z][A-Za-z0-9_]*)",
        r"give\s+me\s+about\s+([A-Za-z][A-Za-z0-9_]*)",
        r"what\s+about\s+([A-Za-z][A-Za-z0-9_]*)",
        r"explain\s+([A-Za-z][A-Za-z0-9_]*)"
    ]
    for pattern in script_patterns:
        match = re.search(pattern, query_lower)
        if match:
            script_name = match.group(1)
            return ("script_summary", script_name)
    
    # Handle "summary about <testname>" or "give me summary about <testname>"
    # Use \S+ to capture all non-whitespace characters (handles long camelCase names)
    summary_about_pattern = r"(?:give\s+me\s+)?summary\s+about\s+(\S+)"
    match = re.search(summary_about_pattern, query_lower)
    if match:
        # Extract from original query to preserve camelCase
        original_match = re.search(r"(?:give\s+me\s+)?summary\s+about\s+(\S+)", query, re.IGNORECASE)
        if original_match:
            script_name = original_match.group(1).strip()
        else:
            script_name = match.group(1).strip()
        return ("script_summary", script_name)
    
    # Handle "about <testname>" without "script" or "test" keyword (must be a valid test name)
    # BUT exclude if it's about a build/execution number
    about_match = re.search(r"about\s+([A-Za-z][A-Za-z0-9_]*)", query_lower, re.IGNORECASE)
    if about_match and not re.search(r"about\s+(?:script|test|build|execution)", query_lower):
        # Check if the word after "about" is followed by a number (build/execution pattern)
        # If so, skip this match as it's likely a build query
        after_about = query_lower[about_match.end():].strip()
        if not re.search(r"^\d+", after_about):  # Not followed by a number
            # Get the actual test name from original query to preserve camelCase
            original_match = re.search(r"about\s+([A-Za-z][A-Za-z0-9_]*)", query, re.IGNORECASE)
            if original_match:
                script_name = original_match.group(1)
                return ("script_summary", script_name)
    
    # Top 10 flaky scripts detection - using contains for flexibility
    if "top" in query_lower and "flaky" in query_lower:
        # Extract number if present
        number_match = re.search(r"top\s+(\d+)", query_lower)
        limit = int(number_match.group(1)) if number_match else 10
        return ("top_flaky", limit)
    
    # Top 10 failing scripts detection - using contains for flexibility
    if "top" in query_lower and "failing" in query_lower:
        # Extract number if present
        number_match = re.search(r"top\s+(\d+)", query_lower)
        limit = int(number_match.group(1)) if number_match else 10
        return ("top_failing", limit)
    
    # Flaky scripts summary detection
    flaky_patterns = [
        r"flaky\s+scripts?\s+summary",
        r"summary\s+of\s+flaky\s+scripts?",
        r"flaky\s+tests?\s+summary",
        r"unstable\s+scripts?\s+summary"
    ]
    for pattern in flaky_patterns:
        if re.search(pattern, query_lower):
            return ("flaky_summary", None)
    
    # Build comparison detection
    comparison_patterns = [
        r"compare\s+builds?\s+(\d+)\s+(?:and|vs|with)\s+(\d+)",
        r"compare\s+builds?\s+(\d+)\s*,\s*(\d+)",
        r"compare\s+executions?\s+(\d+)\s+(?:and|vs|with)\s+(\d+)",
        r"compare\s+executions?\s+(\d+)\s*,\s*(\d+)",
        r"builds?\s+(\d+)\s+(?:and|vs|with)\s+builds?\s+(\d+)",
        r"executions?\s+(\d+)\s+(?:and|vs|with)\s+executions?\s+(\d+)",
        r"compare\s+build\s+(\d+)\s+vs\s+(\d+)",
        r"compare\s+execution\s+(\d+)\s+vs\s+(\d+)",
        r"build\s+(\d+)\s+vs\s+build\s+(\d+)",
        r"execution\s+(\d+)\s+vs\s+execution\s+(\d+)",
        r"compare\s+build",
        r"compare\s+execution",
        r"compare\s+builds?",
        r"build\s+comparison",
        r"compare\s+yesterday\s+vs\s+today",
        r"yesterday\s+vs\s+today",
        r"compare\s+previous\s+vs\s+current",
        r"previous\s+build\s+vs\s+current",
        r"last\s+build\s+vs\s+current"
    ]
    for pattern in comparison_patterns:
        match = re.search(pattern, query_lower)
        if match:
            if len(match.groups()) == 2:
                # Two execution numbers specified
                return ("build_comparison", (match.group(1), match.group(2)))
            else:
                # Time-based or implicit comparison
                return ("build_comparison", None)
    
    return (None, None)

# Use configuration from environment variables
execution_number = config.DEFAULT_EXECUTION_NUMBER
if generate_button and user_query.strip():
    # Check if this is a summary request
    summary_type, param = detect_summary_request(user_query)
    
    if summary_type:
        with st.spinner(f"Generating summary..."):
            if summary_type == "build_summary":
                summary_result = SummaryService.generate_build_summary(param)
                if summary_result["success"]:
                    st.session_state.result = {
                        "query": f"Build Summary for Execution #{summary_result['execution_number']}",
                        "success": True,
                        "data": None,
                        "error": None,
                        "attempts": 1,
                        "row_count": summary_result["total_scripts"],
                        "summary": summary_result["summary"],
                        "is_summary": True
                    }
                else:
                    st.session_state.result = {
                        "query": "Build Summary",
                        "success": False,
                        "data": None,
                        "error": summary_result.get("error", "Failed to generate summary"),
                        "attempts": 1,
                        "row_count": 0,
                        "is_summary": True
                    }
            elif summary_type == "script_summary":
                summary_result = SummaryService.generate_script_summary(param)
                if summary_result["success"]:
                    st.session_state.result = {
                        "query": f"Script Summary for {summary_result['script_name']}",
                        "success": True,
                        "data": summary_result.get("executions", []),  # Include execution data for table display
                        "error": None,
                        "attempts": 1,
                        "row_count": summary_result["total_executions"],
                        "summary": summary_result["summary"],
                        "is_summary": True
                    }
                else:
                    st.session_state.result = {
                        "query": f"Script Summary for {param}",
                        "success": False,
                        "data": None,
                        "error": summary_result.get("error", "Failed to generate summary"),
                        "attempts": 1,
                        "row_count": 0,
                        "is_summary": True
                    }
            elif summary_type == "top_flaky":
                limit = param if isinstance(param, int) else 10
                summary_result = SummaryService.generate_top_flaky_scripts(limit)
                if summary_result["success"]:
                    st.session_state.result = {
                        "query": f"Top {limit} Flaky Scripts",
                        "success": True,
                        "data": summary_result.get("scripts", []),
                        "error": None,
                        "attempts": 1,
                        "row_count": summary_result.get("total", 0),
                        "summary": None,
                        "is_summary": False  # Display as regular table, not summary
                    }
                else:
                    st.session_state.result = {
                        "query": f"Top {limit} Flaky Scripts",
                        "success": False,
                        "data": None,
                        "error": summary_result.get("error", "Failed to generate query"),
                        "attempts": 1,
                        "row_count": 0,
                        "is_summary": False
                    }
            elif summary_type == "top_failing":
                limit = param if isinstance(param, int) else 10
                summary_result = SummaryService.generate_top_failing_scripts(limit)
                if summary_result["success"]:
                    st.session_state.result = {
                        "query": f"Top {limit} Failing Scripts",
                        "success": True,
                        "data": summary_result.get("scripts", []),
                        "error": None,
                        "attempts": 1,
                        "row_count": summary_result.get("total", 0),
                        "summary": None,
                        "is_summary": False  # Display as regular table, not summary
                    }
                else:
                    st.session_state.result = {
                        "query": f"Top {limit} Failing Scripts",
                        "success": False,
                        "data": None,
                        "error": summary_result.get("error", "Failed to generate query"),
                        "attempts": 1,
                        "row_count": 0,
                        "is_summary": False
                    }
            elif summary_type == "flaky_summary":
                summary_result = SummaryService.generate_flaky_scripts_summary()
                if summary_result["success"]:
                    st.session_state.result = {
                        "query": "Flaky Scripts Summary",
                        "success": True,
                        "data": None,
                        "error": None,
                        "attempts": 1,
                        "row_count": summary_result["total_flaky"],
                        "summary": summary_result["summary"],
                        "is_summary": True
                    }
                else:
                    st.session_state.result = {
                        "query": "Flaky Scripts Summary",
                        "success": False,
                        "data": None,
                        "error": summary_result.get("error", "Failed to generate summary"),
                        "attempts": 1,
                        "row_count": 0,
                        "is_summary": True
                    }
            elif summary_type == "build_comparison":
                # Handle build comparison
                exec1, exec2 = None, None
                if param and isinstance(param, tuple) and len(param) == 2:
                    exec1, exec2 = param
                elif param is None:
                    # Time-based or implicit comparison
                    exec1, exec2 = None, None
                
                summary_result = SummaryService.generate_build_comparison_summary(exec1, exec2)
                if summary_result["success"]:
                    st.session_state.result = {
                        "query": f"Build Comparison: {summary_result['execution1']} vs {summary_result['execution2']}",
                        "success": True,
                        "data": summary_result.get("changed_tests", []),  # Include test data for table display
                        "error": None,
                        "attempts": 1,
                        "row_count": summary_result["total_changed"],
                        "summary": summary_result["summary"],
                        "is_summary": True
                    }
                else:
                    st.session_state.result = {
                        "query": "Build Comparison",
                        "success": False,
                        "data": None,
                        "error": summary_result.get("error", "Failed to generate summary"),
                        "attempts": 1,
                        "row_count": 0,
                        "is_summary": True
                    }
    else:
        # Regular query generation with summary
        with st.spinner(f"Generating query and summary..."):
            result = OpenAIQueryGenerationService.generate_query_with_summary(
                user_query, 
                execution_number, 
                config.MAX_RETRIES
            )
            st.session_state.result = result

# Display results - flux query and table
if st.session_state.result:
    result = st.session_state.result
    
    # Check if this is a summary result
    if result.get("is_summary"):
        st.markdown("---")
        if result.get("summary"):
            st.markdown("### 📊 Summary")
            # Display summary - Streamlit markdown supports markdown syntax
            st.markdown(result["summary"])
        
        # Display table if data is available (for build comparison and script summary)
        if result.get("data") and len(result.get("data", [])) > 0:
            # Determine table title based on query type
            if "Build Comparison" in result.get("query", ""):
                st.markdown("### 📋 Changed Tests Table")
            else:
                st.markdown("### 📋 Test Execution History")
            
            try:
                import pandas as pd
                df = pd.DataFrame(result["data"])
                
                # Deduplicate testnames for non-build-comparison queries
                is_build_comparison = "Build Comparison" in result.get("query", "")
                if not is_build_comparison and "testname" in df.columns:
                    # Check if there are duplicate testnames
                    if df["testname"].duplicated().any():
                        # Build aggregation dictionary for non-testname columns
                        agg_dict = {}
                        for col in df.columns:
                            if col == "testname":
                                continue
                            elif df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                                # For numeric columns, take max
                                agg_dict[col] = 'max'
                            else:
                                # For string/object columns, take first
                                agg_dict[col] = 'first'
                        
                        # If agg_dict is empty (only testname column exists), use drop_duplicates
                        original_count = len(df)
                        if not agg_dict:
                            # Only testname column - just remove duplicates
                            df = df.drop_duplicates(subset=["testname"], keep='first')
                        else:
                            # Group by testname and aggregate
                            df = df.groupby("testname", as_index=False).agg(agg_dict)
                        
                        # Add count column to show occurrences
                        if len(df) < original_count:
                            original_df = pd.DataFrame(result["data"])
                            counts = original_df["testname"].value_counts().to_dict()
                            df["occurrence_count"] = df["testname"].map(counts)
                
                # Prioritize columns based on query type
                if is_build_comparison:
                    priority_columns = ["testname", "previous_status", "current_status", "current_failure_stack"]
                else:
                    # For script summary
                    priority_columns = ["_time", "testname", "status", "execution_number", "owner", "failure_stack", "occurrence_count"]
                
                existing_priority = [col for col in priority_columns if col in df.columns]
                other_columns = [col for col in df.columns if col not in priority_columns]
                ordered_columns = existing_priority + sorted(other_columns)
                
                df_display = df[ordered_columns] if ordered_columns else df
                st.dataframe(df_display, use_container_width=True, height=400)
                
                # Show appropriate row count message
                if is_build_comparison:
                    st.info(f"Total changed tests: {result.get('row_count', len(result['data']))}")
                else:
                    deduplicated_count = len(df)
                    original_count = result.get('row_count', len(result['data']))
                    if deduplicated_count < original_count:
                        st.info(f"Total: {deduplicated_count} unique testnames (from {original_count} total rows)")
                    else:
                        st.info(f"Total executions: {original_count}")
            except Exception as e:
                st.error(f"Error displaying table: {str(e)}")
                with st.expander("View Raw Data"):
                    st.json(result["data"][:10] if len(result["data"]) > 10 else result["data"])
        
        elif result["success"]:
            st.info("✅ Summary generated successfully but no summary text available.")
            if result.get("row_count") is not None:
                st.info(f"Processed {result['row_count']} items.")
        else:
            error_msg = result.get('error', 'Failed to generate summary')
            st.error(f"**❌ Error:** {error_msg}")
            if result.get("query"):
                st.info(f"Query attempted: {result['query']}")
    else:
        # Regular query results - Display Summary first, then Table
        if result["success"]:
            # Display Summary first if available
            if result.get("summary"):
                st.markdown("---")
                st.markdown("### 📊 Summary")
                st.markdown(result["summary"])
            
            # Display Table with Results
            if result.get("data") is not None:
                if result["row_count"] > 0:
                    st.markdown("### 📋 Query Results")
                    try:
                        df = pd.DataFrame(result["data"])
                        
                        # Deduplicate testnames if they exist
                        if "testname" in df.columns:
                            # Check if there are duplicate testnames
                            if df["testname"].duplicated().any():
                                # Build aggregation dictionary for non-testname columns
                                agg_dict = {}
                                for col in df.columns:
                                    if col == "testname":
                                        continue
                                    elif df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                                        # For numeric columns, take max (useful for counts, scores, durations)
                                        agg_dict[col] = 'max'
                                    else:
                                        # For string/object columns, take first
                                        agg_dict[col] = 'first'
                                
                                # If agg_dict is empty (only testname column exists), use drop_duplicates
                                if not agg_dict:
                                    # Only testname column - just remove duplicates
                                    original_count = len(df)
                                    df = df.drop_duplicates(subset=["testname"], keep='first')
                                    # Add count column to show occurrences
                                    if len(df) < original_count:
                                        original_df = pd.DataFrame(result["data"])
                                        counts = original_df["testname"].value_counts().to_dict()
                                        df["occurrence_count"] = df["testname"].map(counts)
                                else:
                                    # Group by testname and aggregate
                                    original_count = len(df)
                                    df = df.groupby("testname", as_index=False).agg(agg_dict)
                                    
                                    # Add count column to show how many times each testname appeared
                                    if len(df) < original_count:
                                        # Count occurrences of each testname in original data
                                        original_df = pd.DataFrame(result["data"])
                                        counts = original_df["testname"].value_counts().to_dict()
                                        df["occurrence_count"] = df["testname"].map(counts)
                        
                        # Display all columns that are actually in the data
                        # Prioritize common columns first, then show all others
                        priority_columns = ["testname", "status", "previous_status", "current_status", 
                                          "duration", "owner", "failure_stack", "current_failure_stack", 
                                          "occurrence_count"]
                        
                        # Get priority columns that exist in the dataframe
                        existing_priority = [col for col in priority_columns if col in df.columns]
                        
                        # Get all other columns not in priority list
                        other_columns = [col for col in df.columns if col not in priority_columns]
                        
                        # Combine: priority columns first, then others
                        ordered_columns = existing_priority + sorted(other_columns)
                        
                        # Select columns in the desired order
                        df_display = df[ordered_columns] if ordered_columns else df
                        
                        # Display the table with all available columns
                        st.dataframe(df_display, use_container_width=True, height=400)
                        
                        # Show row count (original vs deduplicated)
                        original_count = result['row_count']
                        deduplicated_count = len(df)
                        if original_count > deduplicated_count:
                            st.info(f"Total rows: {deduplicated_count} unique testnames (from {original_count} total rows)")
                        else:
                            st.info(f"Total rows: {result['row_count']}")
                    except Exception as e:
                        st.error(f"Error displaying table: {str(e)}")
                        # Show raw data if DataFrame creation fails
                        with st.expander("View Raw Data"):
                            st.json(result["data"][:10] if len(result["data"]) > 10 else result["data"])
                else:
                    st.info("✓ Query executed successfully but returned no data.")
            else:
                st.warning("Query executed but no data was returned.")
        else:
            # Query failed
            if result.get("error"):
                st.error(f"**Query Error:** {result['error']}")
            else:
                st.error("Query execution failed.")
