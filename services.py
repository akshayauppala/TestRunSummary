"""
Service layer module implementing business logic.
Separates business logic from UI and data access layers.
"""
from typing import Dict, Any, Optional
from clients import ClientFactory
from config import config
from query_logger import query_logger


class FluxQueryService:
    """
    Service class for executing Flux queries against InfluxDB.
    Implements Repository pattern for data access.
    """
    
    @staticmethod
    def execute_flux_query(query: str, execution_number: str = None) -> Dict[str, Any]:
        """
        Execute Flux query against InfluxDB and return results or error.
        
        Args:
            query: Flux query string
            execution_number: Optional execution number to replace in query
            
        Returns:
            Dictionary with success status, data, error, and row_count
        """
        if execution_number is None:
            execution_number = config.DEFAULT_EXECUTION_NUMBER
            
        try:
            client = ClientFactory.get_influx_client()
            if not client:
                return {
                    "success": False,
                    "data": None,
                    "error": "InfluxDB client not initialized",
                    "row_count": 0
                }
            
            query_api = client.query_api()
            # Ensure execution_number is a string
            processed_query = query.replace("${execution_number}", str(execution_number))
            tables = query_api.query(processed_query, org=config.INFLUX_ORG)
            
            results = []
            # Metadata columns to exclude
            excluded_columns = {'result', 'table', '_start', '_stop'}
            
            for table in tables:
                for record in table.records:
                    # Filter out unwanted metadata columns
                    filtered_values = {k: v for k, v in record.values.items() if k not in excluded_columns}
                    results.append(filtered_values)
            
            return {
                "success": True,
                "data": results,
                "error": None,
                "row_count": len(results)
            }
            
        except Exception as e:
            error_msg = str(e)
            if "runtime error" in error_msg.lower():
                lines = error_msg.split("\n")
                for line in lines:
                    if "runtime error" in line.lower():
                        error_msg = line.strip()
                        break
            
            return {
                "success": False,
                "data": None,
                "error": error_msg,
                "row_count": 0
            }


class OpenAIQueryGenerationService:
    """
    Service class for generating Flux queries using OpenAI.
    Implements Strategy pattern for query generation with retry logic.
    """
    
    QUERY_ANALYSIS_PROMPT = """
You are an expert InfluxDB 2.x and Flux specialist. Generate syntactically correct Flux queries from natural language.

SCHEMA:
- Bucket: testexecution, Measurement: testmethod
- TAGS (direct access): testname, status (PASS/FAIL/SKIP), owner, execution_number, environment
- FIELDS (filter by _field first): duration (always present), start_time, failure_message, failure_stack (LARGE)

CRITICAL RULES:
1. Tags: r.status, r.testname (direct access). Fields: |> filter(fn: (r) => r._field == "duration")
2. NEVER: r._field == "status" or r.duration (wrong access)
3. ALWAYS: |> keep(columns: [...]) to exclude metadata (result, table, _start, _stop)
4. Pivot: Only when multiple fields in same row. NOT for aggregations.
5. Group before join: |> group(columns: ["testname"]) on BOTH tables

QUERY INTENT MAPPING:

Build/Execution: Number after "build"/"execution" → filter by execution_number
Script/Test: CamelCase name → filter by testname
Owner: "of X", "by X", "owned by X", "X's" → filter by owner (use regex /(?i)X/ for partial matches, case-insensitive: r.owner =~ /(?i)akshaya/)
Status lookup: "status of X" → return status (don't filter by it)
Singular ("most", "the", "one") → limit(n: 1)
Plural → limit(n: N) or no limit
Comparison ("vs", "compare") → join on testname, find status changes
Count/Aggregate → group() + count()/sum()/mean()/max()/min()
Flaky → reduce() to count PASS/FAIL, filter where both > 0
"slower than X" → duration > X (group by testname, max(), then filter)
"faster than X" → duration < X (group by testname, max(), then filter)
Failure reason search ("failing due to X", "errors about X"): Filter by status == "FAIL", then filter by _field == "failure_message" or _field == "failure_stack", then search _value with regex /(?i)X/ for case-insensitive match. Group by testname to get unique tests.

OPERATIONS:
- Sort: "slowest"/"fastest" → sort by duration, "most failed" → sort by count
- Limit: Singular → limit(n: 1), "top N" → limit(n: N)
- Aggregation: For cross-build queries, group by testname first, then aggregate
- Filter: "slower than 30s" → group by testname, max(), filter where max > 30

PATTERNS:

Status: |> filter(fn: (r) => r.testname == "X") |> filter(fn: (r) => r.execution_number == "Y") |> filter(fn: (r) => r._field == "duration") |> keep(columns: ["testname", "status", "execution_number"])

Failed: |> filter(fn: (r) => r.status == "FAIL") |> filter(fn: (r) => r._field == "duration") |> keep(columns: ["testname", "status", "_value"])

Count: |> filter(fn: (r) => r._field == "duration") |> group(columns: ["status"]) |> count() |> rename(columns: {_value: "count"})

Top N: |> filter(fn: (r) => r._field == "duration") |> sort(columns: ["_value"], desc: true) |> limit(n: N) |> keep(columns: ["testname", "_value"])

Most failed (singular): |> filter(fn: (r) => r.status == "FAIL") |> filter(fn: (r) => r._field == "duration") |> group(columns: ["testname"]) |> count() |> rename(columns: {_value: "failure_count"}) |> group() |> sort(columns: ["failure_count"], desc: true) |> limit(n: 1) |> keep(columns: ["testname", "failure_count"])

Owner filter (partial match, case-insensitive): |> filter(fn: (r) => exists r.owner and r.owner =~ /(?i)akshaya/) |> filter(fn: (r) => r._field == "duration") |> keep(columns: ["testname", "owner", "_value"])

Most failed by owner: |> filter(fn: (r) => r.status == "FAIL") |> filter(fn: (r) => r._field == "duration") |> filter(fn: (r) => exists r.owner and r.owner =~ /(?i)akshaya/) |> group(columns: ["testname"]) |> count() |> rename(columns: {_value: "failure_count"}) |> group() |> sort(columns: ["failure_count"], desc: true) |> limit(n: 1) |> keep(columns: ["testname", "failure_count"])

Failure reason search (toast): |> filter(fn: (r ) => r.status == "FAIL") |> filter(fn: (r) => r._field == "failure_message" or r._field == "failure_stack") |> filter(fn: (r) => r._value =~ /(?i)toast/) |> group(columns: ["testname"]) |> keep(columns: ["testname"])

Slower than X: |> filter(fn: (r) => r._field == "duration") |> group(columns: ["testname"]) |> max(column: "_value") |> rename(columns: {_value: "max_duration"}) |> filter(fn: (r) => r.max_duration > X) |> group() |> sort(columns: ["max_duration"], desc: true) |> keep(columns: ["testname", "max_duration"])

Flaky (singular): |> filter(fn: (r) => r._field == "duration") |> group(columns: ["testname"]) |> reduce(identity: {testname: "", pass_count: 0, fail_count: 0, total: 0}, fn: (r, accumulator) => ({testname: r.testname, pass_count: accumulator.pass_count + (if r.status == "PASS" then 1 else 0), fail_count: accumulator.fail_count + (if r.status == "FAIL" then 1 else 0), total: accumulator.total + 1})) |> filter(fn: (r) => r.pass_count > 0 and r.fail_count > 0) |> map(fn: (r) => ({testname: r.testname, flakiness_score: float(v: r.fail_count) / float(v: r.total)})) |> group() |> sort(columns: ["flakiness_score"], desc: true) |> limit(n: 1) |> keep(columns: ["testname", "flakiness_score"])

OUTPUT: Only Flux query, no markdown/comments. Start with: from(bucket: "testexecution")
"""

    SUMMARY_GENERATION_PROMPT = """
You are a data analyst expert specializing in test execution analysis.

TASK: Analyze the provided data and generate an accurate, insightful summary that reflects the ACTUAL data, not templates.

CRITICAL RULES:
1. **COUNT ACCURATELY**: Count the actual number of rows/items in the data. If row_count is provided, use it. If counting unique testnames, count them correctly.
2. **ANALYZE THE DATA**: Look at the actual columns and values in the data provided. Don't assume patterns.
3. **BE DATA-DRIVEN**: Generate insights based on what's actually in the data, not on query type assumptions.
4. **FAILURE CATEGORIZATION**: If failure_stack or failure_message columns exist, analyze and categorize failures by error type (AssertionError, TimeoutException, NullPointerException, ElementNotFound, Toast notification issues, etc.)

INPUT:
- User Query: {user_query}
- Flux Query: {flux_query}
- Result Data: {data} (NOTE: This may be a sample. Use row_count for total.)
- Row Count: {row_count} (THIS IS THE TOTAL - USE THIS FOR COUNTS, NOT THE DATA ARRAY SIZE)

ANALYSIS PROCESS:

1. **EXAMINE THE DATA STRUCTURE**:
   - What columns are present? (testname, status, duration, owner, execution_number, failure_stack, failure_message, etc.)
   - **CRITICAL**: The row_count ({row_count}) is the TOTAL number of rows. The data array may only contain a sample.
   - **ALWAYS use row_count for total counts**, not len(data).
   - If counting unique testnames, analyze the data sample but report totals based on row_count.
   - If the query is about failures, the row_count represents the total failures.

2. **ANALYZE FAILURES** (if status=FAIL or failure data exists):
   - **Total failures = row_count** (use this number, not the data array size)
   - If failure_stack or failure_message exists in the data sample:
     * Analyze the sample for failure patterns
     * Categorize failures by error type found in the sample:
       - AssertionError / Assertion failures (look for "AssertionError", "assert", "expected")
       - TimeoutException / Timeout issues (look for "TimeoutException", "timeout", "waiting for")
       - ElementNotFound / Element visibility issues (look for "NoSuchElementException", "element not found", "not visible")
       - Toast notification issues (look for "toast", "notification", "message")
       - Network/API issues (look for "ConnectionException", "HTTP", "API", "network")
       - NullPointerException
       - Other error types
     * Count occurrences of each category in the sample
     * Extrapolate to total failures using row_count (e.g., if 5 out of 20 sample failures are toast-related, estimate ~25% of total)
   - Identify most common failure categories
   - Extract specific error patterns and root causes

3. **CALCULATE METRICS**:
   - **Total counts: ALWAYS use row_count** (this is the accurate total)
   - Unique testnames: Count from data sample, but note if data is a sample
   - Averages: Calculate from sample data, but use row_count for denominators
   - Percentages: Calculate based on row_count
   - Min/Max values: From sample data
   - **IMPORTANT**: If reporting "X failures", use row_count, not len(data)

4. **IDENTIFY PATTERNS**:
   - Most frequent failures
   - Slowest tests
   - Common owners
   - Execution numbers involved
   - Any trends or anomalies

5. **GENERATE SUMMARY**:
   - Start with accurate headline based on user query
   - Include correct counts (total failures, unique tests, etc.)
   - Add failure categorization if failure data exists
   - Highlight key findings from the actual data
   - Provide actionable recommendations

OUTPUT FORMAT:
- Use markdown formatting
- Be comprehensive but concise
- Include accurate numbers from the data
- Use emojis sparingly (✅ PASS, ❌ FAIL, ⚠️ SKIP, 🐛 Flaky, 🐢 Slow)
- Structure flexibly based on what the data shows

IMPORTANT:
- DO NOT use hardcoded patterns or templates
- DO NOT assume counts - count from the actual data
- DO analyze failure_stack/failure_message if present
- DO categorize failures by error type when applicable
- DO adapt the summary structure to what the data contains

NOW ANALYZE THE DATA AND GENERATE THE SUMMARY.
"""
    
    @staticmethod
    def generate_flux_query_only(
        user_query: str, 
        execution_number: str = None, 
        max_retries: int = None
    ) -> Dict[str, Any]:
        """
        Generate Flux query with validation and retry logic.
        Returns only the query without executing it.
        
        Args:
            user_query: Natural language query from user
            execution_number: Optional execution number for query substitution
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dictionary with query, success status, error, and attempts
        """
        if execution_number is None:
            execution_number = config.DEFAULT_EXECUTION_NUMBER
        if max_retries is None:
            max_retries = config.MAX_RETRIES
        
        messages = [
            {"role": "system", "content": OpenAIQueryGenerationService.QUERY_ANALYSIS_PROMPT},
            {"role": "user", "content": user_query}
        ]
        
        for attempt in range(1, max_retries + 1):
            try:
                openai_client = ClientFactory.get_openai_client()
                response = openai_client.chat.completions.create(
                    model=config.OPENAI_MODEL,
                    temperature=0,
                    messages=messages
                )
                
                flux_query = response.choices[0].message.content.strip()
                flux_query = flux_query.replace("```flux", "").replace("```", "").strip()
                
                if flux_query.startswith("ERROR:"):
                    return {
                        "query": flux_query,
                        "success": False,
                        "error": flux_query,
                        "attempts": attempt
                    }
                
                # Test the query
                result = FluxQueryService.execute_flux_query(flux_query, execution_number)
                
                if result["success"]:
                    return {
                        "query": flux_query,
                        "success": True,
                        "error": None,
                        "attempts": attempt
                    }
                else:
                    if attempt < max_retries:
                        error_feedback = f"""
The query failed with this error:
{result['error']}

FAILED QUERY:
{flux_query}

Analyze the error and generate a corrected query. Common fixes:
1. Filter by _field BEFORE pivot/group (schema collision error)
2. Check 'exists r.owner' before filtering by owner
3. Status values must be uppercase: "FAIL", "PASS", "SKIP"
4. Never mix numeric and string fields in group/pivot
5. Group by correct columns before join

Generate the corrected query:
"""
                        messages.append({"role": "assistant", "content": flux_query})
                        messages.append({"role": "user", "content": error_feedback})
                    else:
                        return {
                            "query": flux_query,
                            "success": False,
                            "error": result["error"],
                            "attempts": attempt
                        }
            
            except Exception as e:
                if attempt == max_retries:
                    return {
                        "query": "",
                        "success": False,
                        "error": f"Generation error: {str(e)}",
                        "attempts": attempt
                    }
        
        return {
            "query": "",
            "success": False,
            "error": "Max retries reached",
            "attempts": max_retries
        }

    @staticmethod
    def generate_summary(
        user_query: str,
        flux_query: str,
        data: list,
        row_count: int,
        execution_number: str = None
    ) -> str:
        """
        Generate a natural language summary from query results using LLM.
        Fetches failure_stack when needed for failure analysis.
        
        Args:
            user_query: Original user query
            flux_query: The Flux query that was executed
            data: Query result data
            row_count: Number of rows returned
            execution_number: Optional execution number for fetching additional data
            
        Returns:
            Markdown-formatted summary string
        """
        try:
            # Limit data sent to LLM to avoid token overflow, but use larger sample for better analysis
            # Use up to 50 rows for better failure categorization, but cap at 50 to manage tokens
            sample_size = min(100, len(data))
            sample_data = data[:sample_size] if len(data) > sample_size else data
            
            # Check if we need to fetch failure_stack for FAIL status
            needs_failure_analysis = False
            if sample_data and len(sample_data) > 0:
                # Check if query is about failures or status query that returned FAIL
                for row in sample_data[:5]:  # Check first 5 rows
                    if row.get('status') == 'FAIL':
                        needs_failure_analysis = True
                        break
            
            # Fetch failure_stack if needed and not already in data
            enhanced_data = sample_data
            if needs_failure_analysis:
                # Check if failure_stack is already in the data
                has_failure_stack = any('failure_stack' in row for row in sample_data if row)
                
                if not has_failure_stack and sample_data:
                    # Fetch failure_stack for failed tests
                    try:
                        testnames = list(set([row.get('testname') for row in sample_data[:10] if row.get('testname')]))
                        if testnames and execution_number:
                            # Build a query to fetch failure_stack
                            testname_filter = ' or '.join([f'r.testname == "{t}"' for t in testnames[:5]])
                            failure_query = f'''
from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.execution_number == "{execution_number}")
  |> filter(fn: (r) => {testname_filter})
  |> filter(fn: (r) => r.status == "FAIL")
  |> filter(fn: (r) => r._field == "failure_stack")
  |> keep(columns: ["testname", "failure_stack", "_value"])
'''
                            result = FluxQueryService.execute_flux_query(failure_query, execution_number)
                            if result["success"] and result["data"]:
                                # Merge failure_stack into sample_data
                                failure_map = {row.get('testname'): row.get('_value', '') for row in result["data"]}
                                enhanced_data = []
                                for row in sample_data:
                                    new_row = row.copy()
                                    testname = row.get('testname')
                                    if testname in failure_map:
                                        new_row['failure_stack'] = failure_map[testname]
                                    enhanced_data.append(new_row)
                    except Exception as e:
                        # If failure_stack fetch fails, continue with original data
                        enhanced_data = sample_data
            
            # Format the prompt
            prompt = OpenAIQueryGenerationService.SUMMARY_GENERATION_PROMPT.format(
                user_query=user_query,
                flux_query=flux_query,
                data=enhanced_data,
                row_count=row_count
            )
            
            openai_client = ClientFactory.get_openai_client()
            response = openai_client.chat.completions.create(
                model=config.OPENAI_MODEL,
                temperature=0.3,  # Slightly higher for more natural summaries
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": "Generate the summary based on the data provided."}
                ]
            )
            
            summary = response.choices[0].message.content.strip()
            return summary
            
        except Exception as e:
            return f"**Summary Generation Failed**\n\nQuery returned {row_count} rows. Error: {str(e)}"

    @staticmethod
    def generate_query_with_summary(
        user_query: str,
        execution_number: str = None,
        max_retries: int = None
    ) -> Dict[str, Any]:
        """
        Generate Flux query, execute it, and generate both table results and summary.
        This is the main method to use for complete query handling.
        
        Args:
            user_query: Natural language query from user
            execution_number: Optional execution number for query substitution
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dictionary with:
            - query: The generated Flux query
            - success: Whether query generation and execution succeeded
            - data: Query result data (table)
            - summary: Natural language summary of results
            - error: Error message if any
            - attempts: Number of attempts needed
            - row_count: Number of rows returned
        """
        if execution_number is None:
            execution_number = config.DEFAULT_EXECUTION_NUMBER
        if max_retries is None:
            max_retries = config.MAX_RETRIES
        
        # Step 1: Generate the Flux query with retry logic
        query_result = OpenAIQueryGenerationService.generate_flux_query_only(
            user_query,
            execution_number,
            max_retries
        )
        
        if not query_result["success"]:
            # Query generation failed
            query_logger.log_query(
                user_query=user_query,
                flux_query=query_result.get("query", ""),
                execution_number=execution_number,
                success=False,
                row_count=0,
                error=query_result.get("error"),
                attempts=query_result.get("attempts", 0)
            )
            return {
                "query": query_result.get("query", ""),
                "success": False,
                "data": None,
                "summary": None,
                "error": query_result.get("error"),
                "attempts": query_result.get("attempts", 0),
                "row_count": 0
            }
        
        flux_query = query_result["query"]
        attempts = query_result["attempts"]
        
        # Step 2: Execute the query
        exec_result = FluxQueryService.execute_flux_query(flux_query, execution_number)
        
        if not exec_result["success"]:
            # Query execution failed
            query_logger.log_query(
                user_query=user_query,
                flux_query=flux_query,
                execution_number=execution_number,
                success=False,
                row_count=0,
                error=exec_result.get("error"),
                attempts=attempts
            )
            return {
                "query": flux_query,
                "success": False,
                "data": None,
                "summary": None,
                "error": exec_result.get("error"),
                "attempts": attempts,
                "row_count": 0
            }
        
        data = exec_result["data"]
        row_count = exec_result["row_count"]
        
        # Step 3: Generate summary
        summary = None
        if row_count > 0:
            summary = OpenAIQueryGenerationService.generate_summary(
                user_query,
                flux_query,
                data,
                row_count,
                execution_number
            )
        else:
            summary = "**No Results Found**\n\nThe query executed successfully but returned no data. This could mean:\n- No tests match the specified criteria\n- The execution number doesn't exist\n- The test name is misspelled"
        
        # Log successful query
        query_logger.log_query(
            user_query=user_query,
            flux_query=flux_query,
            execution_number=execution_number,
            success=True,
            row_count=row_count,
            attempts=attempts
        )
        
        return {
            "query": flux_query,
            "success": True,
            "data": data,
            "summary": summary,
            "error": None,
            "attempts": attempts,
            "row_count": row_count
        }
    
    @staticmethod
    def generate_flux_with_validation(
        user_query: str, 
        execution_number: str = None, 
        max_retries: int = None
    ) -> Dict[str, Any]:
        """
        Generate Flux query and validate against InfluxDB with retry logic.
        Legacy method - use generate_query_with_summary() for new implementations.
        
        Args:
            user_query: Natural language query from user
            execution_number: Optional execution number for query substitution
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dictionary with query, success status, data, error, attempts, and row_count
        """
        if execution_number is None:
            execution_number = config.DEFAULT_EXECUTION_NUMBER
        if max_retries is None:
            max_retries = config.MAX_RETRIES
        
        messages = [
            {"role": "system", "content": OpenAIQueryGenerationService.QUERY_ANALYSIS_PROMPT},
            {"role": "user", "content": user_query}
        ]
        
        for attempt in range(1, max_retries + 1):
            try:
                openai_client = ClientFactory.get_openai_client()
                response = openai_client.chat.completions.create(
                    model=config.OPENAI_MODEL,
                    temperature=0,
                    messages=messages
                )
                
                flux_query = response.choices[0].message.content.strip()
                flux_query = flux_query.replace("```flux", "").replace("```", "").strip()
                
                if flux_query.startswith("ERROR:"):
                    response = {
                        "query": flux_query,
                        "success": False,
                        "data": None,
                        "error": flux_query,
                        "attempts": attempt,
                        "row_count": 0
                    }
                    # Log error query
                    query_logger.log_query(
                        user_query=user_query,
                        flux_query=flux_query,
                        execution_number=execution_number,
                        success=False,
                        row_count=0,
                        error=flux_query,
                        attempts=attempt
                    )
                    return response
                
                result = FluxQueryService.execute_flux_query(flux_query, execution_number)
                
                if result["success"]:
                    response = {
                        "query": flux_query,
                        "success": True,
                        "data": result["data"],
                        "error": None,
                        "attempts": attempt,
                        "row_count": result["row_count"]
                    }
                    # Log successful query
                    query_logger.log_query(
                        user_query=user_query,
                        flux_query=flux_query,
                        execution_number=execution_number,
                        success=True,
                        row_count=result["row_count"],
                        attempts=attempt
                    )
                    return response
                else:
                    if attempt < max_retries:
                        error_feedback = f"""
The query failed with this error:
{result['error']}

FAILED QUERY:
{flux_query}

Analyze the error and generate a corrected query following these rules:
1. Filter by _field BEFORE pivot/group to avoid schema collision
2. Check 'exists r.owner' before filtering by owner
3. Status values must be uppercase: "FAIL", "PASS", "SKIP"
4. Never mix numeric and string fields

Generate the corrected query:
"""
                        messages.append({"role": "assistant", "content": flux_query})
                        messages.append({"role": "user", "content": error_feedback})
                    else:
                        response = {
                            "query": flux_query,
                            "success": False,
                            "data": None,
                            "error": result["error"],
                            "attempts": attempt,
                            "row_count": 0
                        }
                        # Log failed query after all retries exhausted
                        query_logger.log_query(
                            user_query=user_query,
                            flux_query=flux_query,
                            execution_number=execution_number,
                            success=False,
                            row_count=0,
                            error=result["error"],
                            attempts=attempt
                        )
                        return response
            
            except Exception as e:
                if attempt == max_retries:
                    response = {
                        "query": "",
                        "success": False,
                        "data": None,
                        "error": f"Generation error: {str(e)}",
                        "attempts": attempt,
                        "row_count": 0
                    }
                    # Log generation error
                    query_logger.log_query(
                        user_query=user_query,
                        flux_query="",
                        execution_number=execution_number,
                        success=False,
                        row_count=0,
                        error=f"Generation error: {str(e)}",
                        attempts=attempt
                    )
                    return response
        
        response = {
            "query": "",
            "success": False,
            "data": None,
            "error": "Max retries reached",
            "attempts": max_retries,
            "row_count": 0
        }
        # Log max retries error
        query_logger.log_query(
            user_query=user_query,
            flux_query="",
            execution_number=execution_number,
            success=False,
            row_count=0,
            error="Max retries reached",
            attempts=max_retries
        )
        return response

