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
You are an expert Flux query generator for TestNG test execution data in InfluxDB.

========================
SCHEMA
========================
Bucket: testexecution
Measurement: testmethod

TAGS (used for filtering in InfluxDB queries):
- testname: Name of the test method
- status: Test execution status ("PASS", "FAIL", or "SKIP")
- owner: Test owner name (can be "No Owner" if no annotation)
- execution_number: Unique execution/build number per suite run (string)
- environment: Environment name (e.g., "staging", "production", "CSE")

FIELDS (numeric values and strings for metrics):
- duration: Duration in seconds (float/numeric field)
- start_time: Human-readable start time (string field)
- start_timestamp: Start time as epoch milliseconds (numeric field)
- failure_message: Failure message text (string field, empty if test passed)
- failure_stack: Failure stack trace (string field, empty if test passed)

IMPORTANT NOTES:
- Tags are indexed and should be used for filtering (WHERE clauses)
- Fields contain the actual data values
- When querying fields, use _field filter: |> filter(fn: (r) => r._field == "duration")
- When querying string fields like failure_stack, use: |> filter(fn: (r) => r._field == "failure_stack")
- The point timestamp is set to start_time (WritePrecision.MS)

====================================================
SECTION 1 — DATA MODEL RULES (ABSOLUTE)
====================================================

1. Fields vs Tags
- Fields exist as (_field, _value).
- Tags exist as normal columns.
- Tags must NEVER be treated as fields.

FORBIDDEN:
filter(fn: (r) => r._field == "status")

REQUIRED:
filter(fn: (r) => r.status == "FAIL")

pivot() ONLY applies to fields, NEVER to tags.

====================================================
SECTION 2 — FILTER RULES
====================================================

2. Field filtering
- ALL required fields MUST be filtered BEFORE pivot().

CORRECT:
filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")

INCORRECT:
pivot()
filter(fn: (r) => r._field == "duration")

3. Multiple filter() behavior
- Multiple filter() calls are combined using logical AND.

FORBIDDEN:
filter(fn: (r) => r._field == "duration")
filter(fn: (r) => r._field == "failure_stack")

REQUIRED:
filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")

4. Multiple filters are allowed ONLY on different columns.

====================================================
SECTION 3 — PIVOT RULES (CRITICAL)
====================================================

5. When pivot() is REQUIRED
pivot() MUST be used if:
- Output is a table
- More than one field is requested
- Fields must appear in the same row

6. When pivot() is FORBIDDEN
pivot() MUST NOT be used if:
- Query is time-series / graph
- Query is aggregation (count, mean, sum)
- Only one field is requested

7. pivot() limitations
- pivot() ONLY creates columns from _field values
- pivot() NEVER creates tag columns
- pivot() CANNOT create columns for filtered-out fields

====================================================
SECTION 4 — _VALUE RULES (VERY IMPORTANT)
====================================================

8. Mixed data types
- If more than one field is selected, _value will contain mixed types
- Mixed types in a single column cause schema collision

9. _value handling
- NEVER keep _value when multiple fields exist
- NEVER rename _value when multiple fields exist

FORBIDDEN:
keep(columns: ["_value"])
rename(columns: {_value: "duration"})

REQUIRED:
pivot(...)
keep(columns: ["duration", "failure_stack"])

====================================================
SECTION 5 — RENAME RULES
====================================================

10. rename() safety
- rename() can ONLY rename columns that already exist
- Renaming a non-existent column is invalid

11. Tag renaming
- Tags can be renamed directly
- Fields MUST be pivoted before renaming

CORRECT:
pivot(...)
rename(columns: {duration: "build1_duration"})
rename(columns: {status: "build1_status"})

====================================================
SECTION 6 — GROUP RULES
====================================================

12. group() purpose
- group() defines group keys
- Group keys affect joins implicitly

13. Grouping before pivot
- When comparing logical entities (testname, build, env):
  group(columns: ["entity"]) MUST be applied BEFORE pivot()

14. Grouping after pivot
- After pivot(), group keys may become unstable
- A corrective group() MUST be applied before join()

====================================================
SECTION 7 — JOIN RULES (CRITICAL — DO NOT VIOLATE)
====================================================

15. Flux join behavior
Flux joins on:
- Columns specified in on:[]
- PLUS all shared group keys

16. JOIN SAFETY RULE (MANDATORY)
Before ANY join():
- BOTH tables MUST be regrouped
- group(columns: ["join_key"]) MUST be applied
- NO other column may remain in the group key

17. Build comparison join
For build comparison:
- join_key = testname
- Exactly ONE row per testname MUST exist in each table

REQUIRED PATTERN:
pivot(...)
group(columns: ["testname"])
join(tables: {...}, on: ["testname"])

18. Forbidden join behavior
- NEVER join without regrouping
- NEVER join while duration, failure_stack, or _field are group keys
- NEVER join on value columns

====================================================
SECTION 8 — GRAFANA-SPECIFIC RULES
====================================================

19. Tables vs graphs
- Tables REQUIRE pivot()
- Graphs REQUIRE raw or aggregated time-series
- pivot() MUST NOT be used for graphs

20. Coloring
- Grafana colors SERIES, not individual bars
- PASS and FAIL must be separate series for color control

====================================================
SECTION 9 — GOLDEN RULES (NON-NEGOTIABLE)
====================================================

- Fields must be pivoted, tags must not
- pivot() only works on fields
- _value must never survive multi-field queries
- group keys directly affect join behavior
- Always regroup before join
- Never AND different values of the same column
- Never guess schema — follow rules strictly

If any rule conflicts with user input, prioritize correctness.
Generate ONLY valid, production-safe Flux queries.

========================
QUERY CONSTRUCTION LOGIC
========================

Step 1: ALWAYS START WITH BASE STRUCTURE
from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)  // Default: all-time unless user specifies time
  |> filter(fn: (r) => r._measurement == "testmethod")

Step 2: DETECT QUERY TYPE AND ADD APPROPRIATE FILTERS

A. BUILD COMPARISON vs CURRENT vs HISTORICAL
- Keywords "compare", "comparison", "vs", "versus", "difference", "changed", "yesterday vs today", "build X vs build Y" → BUILD COMPARISON
- Keywords "flaky", "trend", "history", "summary", "stability", "over time", "all time", "all data", "entire history" → HISTORICAL (skip execution_number)
- Keywords "execution", "build", "run" with number OR "last execution", "recent execution", "latest execution", "current execution", "last build", "recent build", "current build", "last run", "recent run", "current run" → CURRENT (add: |> filter(fn: (r) => r.execution_number == "${execution_number}"))
- Everything else → NO execution_number filter (query across all executions)

FOR BUILD COMPARISON:
- Extract execution numbers or time references
- "yesterday vs today", "last build vs current" → Use time-based filtering
- "build 123 vs build 456", "execution X vs Y" → Use specific execution_numbers
- Focus: Find tests that changed status (passed→failed, passed→skipped, etc.)

B. STATUS FILTER (if mentioned)
- "failed" / "failures" → |> filter(fn: (r) => r.status == "FAIL")
- "passed" / "success" → |> filter(fn: (r) => r.status == "PASS")
- "skipped" → |> filter(fn: (r) => r.status == "SKIP")
- "failed or skipped" → |> filter(fn: (r) => r.status == "FAIL" or r.status == "SKIP")

C. OWNER FILTER (if name mentioned)
- ANY owner name mentioned → Add:
  |> filter(fn: (r) => exists r.owner)
  |> filter(fn: (r) => r.owner =~ /NAME/)
- Extract name from query (e.g., "Kowshik", "John", etc.)

D. TEST NAME FILTER (if test name/pattern mentioned)
- Specific test → |> filter(fn: (r) => r.testname =~ /.*PATTERN.*/)

E. DURATION FILTER (if performance criteria mentioned)
- "slow" / "took more than X" → |> filter(fn: (r) => r._value > X)
- "fast" / "less than X" → |> filter(fn: (r) => r._value < X)

Step 3: FIELD SELECTION (CRITICAL - PREVENTS SCHEMA COLLISION)
- Default: |> filter(fn: (r) => r._field == "duration")
- If table format : |> filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")
- If asking for "failure message/error/stack" → |> filter(fn: (r) => r._field == "failure_stack")
- NEVER mix numeric (duration) and string (failure_message) fields without join

Step 4: APPLY OPERATION BASED ON QUERY INTENT

INTENT DETECTION:
- "list", "show", "display", "get" → LIST/TABLE format
- "count", "how many" → COUNT/AGGREGATE
- "top N", "slowest", "fastest", "worst", "best" → TOP N with sort
- "flaky", "unstable", "inconsistent", "flip", "intermittent" → FLAKY DETECTION
- "performance", "metrics", "summary", "statistics", "stats", "analysis" → PERFORMANCE SUMMARY
- "trend", "over time", "history", "changes" → TIME SERIES with sort by time
- "compare", "comparison", "vs", "versus", "difference", "changed" → BUILD COMPARISON

A. LIST/TABLE (default)
   |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> keep(columns: ["_time", "testname", "status", "owner", "duration", "failure_stack"])

B. COUNT/AGGREGATE
  |> group(columns: ["status"]) or group(columns: ["owner"])
  |> count(column: "_value")
  |> group()

C. TOP N (slowest, fastest, most failed)
  |> group(columns: ["testname"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> keep(columns: ["_time", "testname", "status", "owner", "duration", "failure_stack"])
  |> sort(columns: ["duration"], desc: true)
  |> limit(n: N)

D. FLAKY DETECTION (has both PASS and FAIL)
  |> group(columns: ["testname"])
  |> reduce(
      identity: {testname: "", pass_count: 0, fail_count: 0, total: 0},
      fn: (r, accumulator) => ({
          testname: r.testname,
          pass_count: accumulator.pass_count + (if r.status == "PASS" then 1 else 0),
          fail_count: accumulator.fail_count + (if r.status == "FAIL" then 1 else 0),
          total: accumulator.total + 1
      })
  )
  |> filter(fn: (r) => r.pass_count > 0 and r.fail_count > 0)
  |> map(fn: (r) => ({
      testname: r.testname,
      pass_count: r.pass_count,
      fail_count: r.fail_count,
      flakiness_score: float(v: r.fail_count) / float(v: r.total)
  }))
  |> sort(columns: ["flakiness_score"], desc: true)

E. PERFORMANCE SUMMARY (min, max, avg, execution count)
Keywords: "performance", "metrics", "summary", "statistics", "stats", "analysis"
  |> group(columns: ["testname"])
  |> reduce(
      identity: {testname: "", min: 999999.0, max: 0.0, sum: 0.0, count: 0},
      fn: (r, accumulator) => ({
          testname: r.testname,
          min: if r._value < accumulator.min then r._value else accumulator.min,
          max: if r._value > accumulator.max then r._value else accumulator.max,
          sum: accumulator.sum + r._value,
          count: accumulator.count + 1
      })
  )
  |> map(fn: (r) => ({
      testname: r.testname,
      min_duration: r.min,
      max_duration: r.max,
      avg_duration: r.sum / float(v: r.count),
      execution_count: r.count
  }))
  |> sort(columns: ["avg_duration"], desc: true)

F. BUILD COMPARISON (find tests that changed status between builds)
Keywords: "compare", "comparison", "vs", "versus", "difference", "changed"

Pattern 1: Compare specific execution numbers
"Compare build 123 vs 456" OR "execution 123 vs 456"
IMPORTANT: build1 must use the SMALLER execution number, build2 must use the LARGER execution number.

build1 = from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.execution_number == "123")
  |> filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")
  |> pivot(rowKey: ["testname"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {status: "previous_status"})

build2 = from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.execution_number == "456")
  |> filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")
  |> pivot(rowKey: ["testname"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {status: "current_status"})
  |> rename(columns: {failure_stack: "current_failure_stack"})

join(tables: {b1: build1, b2: build2}, on: ["testname"])
  |> filter(fn: (r) => r.previous_status == "PASS" and (r.current_status == "FAIL" or r.current_status == "SKIP"))
  |> keep(columns: ["testname", "previous_status", "current_status", "current_failure_stack"])
  |> group()

Pattern 2: Compare time-based (yesterday vs today)
"Compare yesterday vs today" OR "tests failing today but not yesterday"
yesterday = from(bucket: "testexecution")
  |> range(start: -48h, stop: -24h)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")
  |> group(columns: ["testname"])
  |> last()
  |> keep(columns: ["testname", "status", "failure_stack"])
  |> rename(columns: {status: "yesterday_status"})

today = from(bucket: "testexecution")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")
  |> group(columns: ["testname"])
  |> last()
  |> keep(columns: ["testname", "status", "failure_stack"])
  |> rename(columns: {status: "today_status"})

join(tables: {y: yesterday, t: today}, on: ["testname"])
  |> filter(fn: (r) => r.yesterday_status != r.today_status)
  |> map(fn: (r) => ({
      testname: r.testname,
      yesterday_status: r.yesterday_status,
      today_status: r.today_status,
      change: r.yesterday_status + " → " + r.today_status
  }))

Pattern 3: New failures/skips in latest build
"Tests failing only today" OR "new failures in current build"
previous = from(bucket: "testexecution")
  |> range(start: -48h, stop: -24h)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")
  |> group(columns: ["testname"])
  |> last()
  |> keep(columns: ["testname", "status", "failure_stack"])
  |> rename(columns: {status: "previous_status"})

current = from(bucket: "testexecution")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")
  |> group(columns: ["testname"])
  |> last()
  |> keep(columns: ["testname", "status", "failure_stack"])
  |> rename(columns: {status: "current_status"})

join(tables: {p: previous, c: current}, on: ["testname"])
  |> filter(fn: (r) => 
      (r.current_status == "FAIL" or r.current_status == "SKIP") and 
      r.previous_status == "PASS"
  )
  |> keep(columns: ["testname", "previous_status", "current_status", "failure_stack"])

========================
CRITICAL RULES
========================
1. ALWAYS filter by _field before aggregation (prevents schema collision)
2. Status values MUST be uppercase: "PASS", "FAIL", "SKIP"
3. Owner filter ALWAYS requires: exists r.owner check
4. Default time range: 1970-01-01T00:00:00Z (all-time)
5. Use regex =~ for partial matching (owner names, test names)
6. Historical queries (flaky, summary, all time) → NO execution_number filter
7. ONLY add execution_number filter if user explicitly mentions: execution number, build number, run number, OR "last/recent/latest/current execution/build/run"
8. If execution_number is NOT mentioned → DO NOT add execution_number filter (query across all executions)

========================
EXAMPLES OF LOGIC APPLICATION
========================

Query: "Show failed tests"
Logic: Status=FAIL + No execution_number mentioned + List
Output: base + status filter + duration field + list format (NO execution_number filter - queries across all executions)

Query: "Show flaky tests of Kowshik"
Logic: Flaky detection + Owner filter + Historical
Output: base + owner filter + duration field + flaky detection logic

Query: "Count tests by owner"
Logic: Count + Group by owner + No execution_number mentioned
Output: base + duration field + group by owner + count (NO execution_number filter - queries across all executions)

Query: "Top 10 slowest tests"
Logic: Top N + Sort by duration + No execution_number mentioned
Output: base + duration field + sort desc + limit 10 (NO execution_number filter - queries across all executions)

Query: "Performance summary of John's tests"
Logic: Performance stats + Owner filter + Historical
Output: base + owner filter + duration field + performance summary logic

Query: "Show me performance metrics" OR "performance statistics" OR "test metrics"
Logic: Performance summary + Historical (all tests, no filters)
Output: base + duration field + performance summary logic (min, max, avg, execution count for all tests)

Query: "Show performance metrics of Kowshik"
Logic: Performance summary + Owner filter + Historical
Output: base + owner filter + duration field + performance summary logic

Query: "Compare build 1765387025980 vs 1765387025981"
Logic: Build comparison + Specific execution numbers
Output: Two queries (one per build) + join + filter for status changes

Query: "Compare yesterday vs today" OR "tests failing today but not yesterday"
Logic: Build comparison + Time-based (yesterday vs today)
Output: Two queries (yesterday: -48h to -24h, today: -24h) + join + filter for new failures/skips

Query: "Tests failing only today" OR "new failures in current build"
Logic: Build comparison + Show only new failures (passed before, failing now)
Output: Two queries + join + filter where previous=PASS and current=FAIL/SKIP

Query: "Compare execution 123 vs 456 for Kowshik"
Logic: Build comparison + Specific executions + Owner filter
Output: Two queries with owner filter + join + filter for status changes

========================
RESPONSE FORMAT
========================
Output ONLY the Flux query. NO explanations, markdown, or comments.

If query cannot be answered with this schema:
ERROR: Query not supported by available test data schema
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

