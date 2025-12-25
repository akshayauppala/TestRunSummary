"""
Specialized prompt templates for different query types.
Each prompt is optimized for its specific query category.
"""

# Base schema information (shared across all prompts)
BASE_SCHEMA = """
SCHEMA:
Bucket: "testexecution", Measurement: "testmethod"
TAGS (direct access): testname, status ("PASS"/"FAIL"/"SKIP"), owner, execution_number, environment
FIELDS (filter by _field first): duration, start_time, start_timestamp, failure_message, failure_stack

CRITICAL: Tags use r.status/r.testname. Fields use r._field == "duration". NEVER confuse them.
"""

# Core rules (shared)
CORE_RULES = """
RULES:
1. Filter by _field BEFORE pivot/group. Multiple fields: r._field == "duration" or r._field == "failure_stack"
2. pivot() REQUIRED for tables with multiple fields. FORBIDDEN for aggregations (count/sum/mean) or single field.
3. After pivot: fields become columns, tags remain. Remove _value, use column names (duration, failure_stack).
4. Status values: "PASS", "FAIL", "SKIP" (uppercase). Owner: exists r.owner then r.owner =~ /NAME/i
5. Default time: 1970-01-01T00:00:00Z. Execution number: only if mentioned, else query all.
"""


def get_simple_query_prompt() -> str:
    """Prompt for simple queries: list, count, filter, show"""
    return f"""{BASE_SCHEMA}

{CORE_RULES}

TASK: Generate Flux query for simple data retrieval.

QUERY TYPES: list, show, display, count, filter, get
BASE: from(bucket: "testexecution") |> range(start: 1970-01-01T00:00:00Z) |> filter(fn: (r) => r._measurement == "testmethod")

FILTERS: Status (r.status == "FAIL"/"PASS"/"SKIP"), Owner (exists r.owner, r.owner =~ /NAME/i), Testname (r.testname =~ /pattern/i), Execution (r.execution_number == "X" if mentioned)

OUTPUT FORMATS:
- LIST: filter(fn: (r) => r._field == "duration") |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") |> group() |> keep(columns: ["_time", "testname", "status", "owner", "duration"])
- COUNT: filter(fn: (r) => r._field == "duration") |> group(columns: ["status"]) |> count() |> group()
- TOP N: filter |> pivot |> group() |> sort(columns: ["duration"], desc: true) |> limit(n: N)

OUTPUT: Flux query only, no markdown/comments.
"""


def get_comparison_query_prompt() -> str:
    """Prompt for build comparison queries"""
    return f"""{BASE_SCHEMA}

{CORE_RULES}

TASK: Generate Flux query for build/execution comparison.

PATTERN: Two queries (build1/build2), pivot both, group(columns: ["testname"]), rename status columns, join.

EXAMPLE:
build1 = from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.execution_number == "X")
  |> filter(fn: (r) => r._field == "duration")
  |> pivot(rowKey: ["testname"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["testname"])
  |> rename(columns: {{status: "previous_status"}})

build2 = from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.execution_number == "Y")
  |> filter(fn: (r) => r._field == "duration")
  |> pivot(rowKey: ["testname"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["testname"])
  |> rename(columns: {{status: "current_status"}})

join(tables: {{b1: build1, b2: build2}}, on: ["testname"])
  |> filter(fn: (r) => r.previous_status == "PASS" and (r.current_status == "FAIL" or r.current_status == "SKIP"))
  |> keep(columns: ["testname", "previous_status", "current_status"])
  |> group()

IMPORTANT: Use only duration field (not failure_stack) to avoid field size limits. build1 uses SMALLER execution number.

OUTPUT: Flux query only, no markdown/comments.
"""


def get_flaky_query_prompt() -> str:
    """Prompt for flaky test detection queries"""
    return f"""{BASE_SCHEMA}

{CORE_RULES}

TASK: Generate Flux query for flaky test detection (tests with both PASS and FAIL).

PATTERN:
filter(fn: (r) => r._field == "duration")
|> group(columns: ["testname"])
|> reduce(
    identity: {{pass: 0, fail: 0, total: 0}},
    fn: (r, acc) => ({{
        pass: acc.pass + (if r.status == "PASS" then 1 else 0),
        fail: acc.fail + (if r.status == "FAIL" then 1 else 0),
        total: acc.total + 1
    }})
)
|> filter(fn: (r) => r.pass > 0 and r.fail > 0)
|> map(fn: (r) => ({{
    testname: r.testname,
    pass_count: r.pass,
    fail_count: r.fail,
    flakiness_score: float(v: r.fail) / float(v: r.total)
}}))
|> sort(columns: ["flakiness_score"], desc: true)

Add filters for owner, environment, time range as needed. NO execution_number filter (historical analysis).

OUTPUT: Flux query only, no markdown/comments.
"""


def get_statistics_query_prompt() -> str:
    """Prompt for statistics/performance queries"""
    return f"""{BASE_SCHEMA}

{CORE_RULES}

TASK: Generate Flux query for statistics/performance metrics (min, max, avg, count).

PATTERN:
filter(fn: (r) => r._field == "duration")
|> group(columns: ["testname"])
|> reduce(
    identity: {{min: 999999.0, max: 0.0, sum: 0.0, count: 0}},
    fn: (r, acc) => ({{
        min: if r._value < acc.min then r._value else acc.min,
        max: if r._value > acc.max then r._value else acc.max,
        sum: acc.sum + r._value,
        count: acc.count + 1
    }})
)
|> map(fn: (r) => ({{
    testname: r.testname,
    min_duration: r.min,
    max_duration: r.max,
    avg_duration: r.sum / float(v: r.count),
    execution_count: r.count
}}))
|> sort(columns: ["avg_duration"], desc: true)

Add filters for owner, environment, time range as needed.

OUTPUT: Flux query only, no markdown/comments.
"""


def get_complex_query_prompt() -> str:
    """Prompt for complex/analytical queries"""
    return f"""{BASE_SCHEMA}

{CORE_RULES}

TASK: Generate Flux query for complex analytical queries. Be creative and flexible.

APPROACH:
1. BASE: from(bucket: "testexecution") |> range(start: 1970-01-01T00:00:00Z) |> filter(fn: (r) => r._measurement == "testmethod")
2. Apply all relevant filters from user query
3. Use appropriate format: LIST (pivot + keep), COUNT (group + count), STATS (reduce + map), or custom logic
4. Support ANY query format - interpret user intent flexibly

ERROR FIXES: Schema collision → _field filter before pivot. Column not found → pivot first. Join error → group(columns: ["testname"]) both tables.

OUTPUT: Flux query only, no markdown/comments. If unsupported: ERROR: Query not supported.
"""


def classify_query_type(user_query: str) -> str:
    """
    Classify query type to select appropriate prompt.
    
    Returns: 'simple', 'comparison', 'flaky', 'statistics', 'complex'
    """
    query_lower = user_query.lower()
    
    # Comparison queries
    if any(keyword in query_lower for keyword in ["compare", "vs", "versus", "difference", "changed", "yesterday vs today"]):
        return "comparison"
    
    # Flaky queries
    if any(keyword in query_lower for keyword in ["flaky", "unstable", "inconsistent", "intermittent"]):
        return "flaky"
    
    # Statistics/performance queries
    if any(keyword in query_lower for keyword in ["performance", "metrics", "statistics", "stats", "min", "max", "avg", "average", "summary"]):
        return "statistics"
    
    # Simple queries (list, count, show, filter)
    if any(keyword in query_lower for keyword in ["list", "show", "display", "count", "how many", "get", "filter"]):
        return "simple"
    
    # Default to complex for anything else
    return "complex"


def get_prompt_for_query(user_query: str) -> str:
    """
    Get the appropriate prompt template based on query type.
    
    Args:
        user_query: The user's natural language query
        
    Returns:
        Appropriate system prompt string
    """
    query_type = classify_query_type(user_query)
    
    prompt_map = {
        "simple": get_simple_query_prompt(),
        "comparison": get_comparison_query_prompt(),
        "flaky": get_flaky_query_prompt(),
        "statistics": get_statistics_query_prompt(),
        "complex": get_complex_query_prompt()
    }
    
    return prompt_map.get(query_type, get_complex_query_prompt())

