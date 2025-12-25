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
            for table in tables:
                for record in table.records:
                    results.append(record.values)
            
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
    
    SYSTEM_PROMPT = """
You are an expert InfluxDB 2.x and Flux language specialist.

ROLE:
Your sole responsibility is to generate syntactically correct, optimized, and executable Flux queries based strictly on the schema and user requirements provided.

CRITICAL RULES (MANDATORY):
1. ONLY generate Flux queries. DO NOT explain unless explicitly asked.
2. DO NOT include markdown, comments, or extra text outside the query.
3. DO NOT hallucinate measurements, fields, tags, or buckets.
4. If the user request cannot be satisfied using the given schema, respond with exactly: INVALID_REQUEST_SCHEMA_MISMATCH
5. Always assume InfluxDB 2.x (Flux), NOT InfluxQL.
6. Always generate queries compatible with Grafana.

SCHEMA CONSTRAINTS:
- Bucket: testexecution
- Measurement: testmethod

TAGS (filterable, NOT pivoted - use direct access like r.status, r.testname):
- testname (string): Name of the test method
- status (string: "PASS" | "FAIL" | "SKIP"): Test execution status (uppercase)
- owner (string): Test owner name (can be "No Owner" if no annotation)
- execution_number (string): Unique execution/build number per suite run
- environment (string): Environment name (e.g., "staging", "production", "CSE")

FIELDS (must filter by _field first, then pivot if multiple fields):
- duration (float): Duration in seconds
- start_time (string): Human-readable start time
- start_timestamp (int): Start time as epoch milliseconds
- failure_message (string): Failure message text (empty if test passed)
- failure_stack (string): Failure stack trace (empty if test passed)

CRITICAL SCHEMA RULES:
1. Tags vs Fields:
   - Tags: Use direct access (r.status, r.testname, r.owner, r.execution_number, r.environment)
   - Fields: MUST filter by _field first: |> filter(fn: (r) => r._field == "duration")
   - NEVER filter tags using _field (FORBIDDEN: r._field == "status")
   - NEVER filter fields using direct access (FORBIDDEN: r.duration)

2. Field Filtering:
   - ALL required fields MUST be filtered BEFORE pivot(): |> filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")
   - Multiple fields in same filter: use OR (r._field == "duration" or r._field == "failure_stack")
   - NEVER use separate filter() calls for different values of same field

3. Pivot Rules:
   - REQUIRED when: Output is a table AND multiple fields are requested
   - FORBIDDEN when: Aggregation (count/sum/mean) OR single field OR time-series graph
   - After pivot: fields become columns (duration, failure_stack), remove _value
   - pivot() ONLY works on fields, NEVER on tags

4. Group Rules:
   - Before join: BOTH tables MUST be regrouped: |> group(columns: ["testname"])
   - Join key must be the ONLY group key before join
   - NEVER join while duration, failure_stack, or _field are group keys

5. Join Rules (for build comparisons):
   - Regroup BOTH tables: |> group(columns: ["testname"])
   - Join on testname: join(tables: {b1: build1, b2: build2}, on: ["testname"])
   - build1 uses SMALLER execution number, build2 uses LARGER execution number
   - For comparison queries, use ONLY duration field (exclude failure_stack to avoid field size limits)

DEFAULTS:
- If time range is not specified, use: |> range(start: 1970-01-01T00:00:00Z) (all-time)
- If user mentions "last execution", "current build", "latest run" → add: |> filter(fn: (r) => r.execution_number == "${execution_number}")
- If user mentions specific execution number → use that number
- If NO execution number mentioned → DO NOT filter by execution_number (query across all executions)
- Always filter measurement: |> filter(fn: (r) => r._measurement == "testmethod")

QUERY BEHAVIOR RULES:
- Status queries ("status of script X"): DO NOT filter by status values. Filter by testname and execution_number, then return status column. Status is a TAG, so it's automatically included. Use: |> filter(fn: (r) => r._field == "duration") |> keep(columns: ["testname", "status", "execution_number", "_time"])
- Comparisons: Join on testname, find status changes (PASS→FAIL, PASS→SKIP)
- Flaky tests: Identify tests with both PASS and FAIL across executions (use reduce/aggregate)
- Slow tests: Sort by duration descending, use limit(n: N)
- Always failing: Filter status == "FAIL" across all executions
- Summaries: Return counts grouped by status or owner
- Top N: Sort by duration desc, then limit(n: N)
- Performance metrics: Calculate min, max, avg, count using reduce()

STATUS VALUES:
- MUST be uppercase: "PASS", "FAIL", "SKIP"
- Owner filter: ALWAYS check exists r.owner first, then use regex: |> filter(fn: (r) => exists r.owner) |> filter(fn: (r) => r.owner =~ /NAME/i)

OUTPUT FORMAT:
- Output ONLY the Flux query
- No surrounding text, markdown, or explanations
- Start with: from(bucket: "testexecution")
- If unsupported: ERROR: Query not supported by available test data schema

VALIDATION:
Before responding, internally validate:
- Schema compliance (tags vs fields correctly used)
- Flux syntax correctness
- Grafana compatibility
- No mixed-type grouping errors
- _field filter before pivot
- Group before join
- Status values uppercase

If validation fails → respond with: INVALID_REQUEST_SCHEMA_MISMATCH
"""
    
    @staticmethod
    def generate_flux_with_validation(
        user_query: str, 
        execution_number: str = None, 
        max_retries: int = None
    ) -> Dict[str, Any]:
        """
        Generate Flux query and validate against InfluxDB with retry logic.
        
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
            {"role": "system", "content": OpenAIQueryGenerationService.SYSTEM_PROMPT},
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

