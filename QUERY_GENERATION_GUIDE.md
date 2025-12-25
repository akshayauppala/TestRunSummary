# Query Generation & Summary System

## Overview

The enhanced query generation system uses a **two-phase LLM approach** to generate accurate Flux queries and insightful summaries from InfluxDB test execution data.

## Architecture

### Phase 1: Query Analysis & Generation
The LLM first **analyzes** the user's natural language query to understand:
- Query type (status lookup, list, count, top N, comparison, etc.)
- Filters needed (testname, status, owner, execution_number, environment)
- Fields required (duration, failure_stack, start_time, etc.)
- Operations needed (sorting, limiting, aggregation, pivot, grouping)

Then it **generates** a syntactically correct Flux query with **retry logic** to fix any errors.

### Phase 2: Summary Generation
After executing the query, the LLM **analyzes the results** and generates:
- Key findings and patterns
- Numerical summaries (counts, percentages, averages)
- Actionable insights
- Markdown-formatted output with emojis for clarity

## New Methods

### 1. `generate_flux_query_only(user_query, execution_number, max_retries)`
Generates and validates a Flux query with retry logic.

**Returns:**
```python
{
    "query": "from(bucket:...)...",
    "success": True,
    "error": None,
    "attempts": 1
}
```

**Use Case:** When you only need the query without executing it.

---

### 2. `generate_summary(user_query, flux_query, data, row_count)`
Generates a natural language summary from query results.

**Returns:** Markdown-formatted summary string

**Use Case:** When you already have query results and need a summary.

---

### 3. `generate_query_with_summary(user_query, execution_number, max_retries)` ⭐ **RECOMMENDED**
Complete solution that generates query, executes it, and returns both table and summary.

**Returns:**
```python
{
    "query": "from(bucket:...)...",
    "success": True,
    "data": [...],           # Table data (list of dicts)
    "summary": "**Test...",  # Markdown summary
    "error": None,
    "attempts": 1,
    "row_count": 150
}
```

**Use Case:** Default method for all new implementations.

---

### 4. `generate_flux_with_validation(user_query, execution_number, max_retries)`
Legacy method - still works but doesn't generate summaries.

**Use Case:** Backward compatibility with existing code.

## Query Analysis Process

The LLM follows this structured analysis:

```
STEP 1: QUERY ANALYSIS
├── Identify query type
├── Extract filters (tags)
├── Determine fields needed
└── Determine output operations

STEP 2: APPLY SCHEMA CONSTRAINTS
├── Validate tags vs fields
└── Check available data

STEP 3: APPLY FLUX RULES
├── Tags: direct access (r.status)
├── Fields: filter by _field first
├── Pivot: only when needed
├── Group: before joins
└── Validate syntax

STEP 4: BUILD FLUX QUERY
└── Generate optimized query

STEP 5: VALIDATION
└── Internal validation before output
```

## Supported Query Types

### ✅ Status Queries
```
"give me status of script addCandidatePlus in build 3573"
```
**Output:** Test status with duration and metadata

---

### ✅ List/Filter Queries
```
"show me all failed tests"
"tests by owner Akshaya"
```
**Output:** Filtered list with relevant columns

---

### ✅ Count/Aggregate Queries
```
"count tests by status"
"total failures in build 3573"
```
**Output:** Aggregated counts with percentages

---

### ✅ Top N Queries
```
"top 5 slowest tests"
"most failed script"
```
**Output:** Sorted and limited results

---

### ✅ Comparison Queries
```
"compare build 3568 vs 3569"
"new failures in current build"
```
**Output:** Changed tests with status transitions

---

### ✅ Statistical Queries
```
"average duration of tests"
"performance metrics for last week"
```
**Output:** Statistical analysis with min/max/avg

## Example Usage

```python
from services import OpenAIQueryGenerationService

# Simple usage
result = OpenAIQueryGenerationService.generate_query_with_summary(
    user_query="show me failed tests in build 3573",
    execution_number="3573",
    max_retries=3
)

if result["success"]:
    print(f"Query: {result['query']}")
    print(f"Rows: {result['row_count']}")
    print(f"Summary:\n{result['summary']}")
    
    # Access table data
    for row in result['data']:
        print(f"Test: {row['testname']}, Status: {row['status']}")
else:
    print(f"Error: {result['error']}")
```

## Key Benefits

1. **Accurate Queries:** LLM analyzes intent before generating Flux
2. **Retry Logic:** Automatically fixes syntax errors
3. **Rich Summaries:** Provides insights beyond raw data
4. **Flexible:** Handles any type of query
5. **Type-Safe:** Validates schema compliance
6. **Efficient:** Optimized queries with proper filtering

## Schema Reference

### Tags (Direct Access)
- `testname` - Test method name
- `status` - PASS | FAIL | SKIP (uppercase)
- `owner` - Test owner name
- `execution_number` - Build/execution number
- `environment` - Environment name

### Fields (Filter by _field)
- `duration` - Duration in seconds (float)
- `start_time` - Human-readable timestamp (string)
- `start_timestamp` - Epoch milliseconds (int)
- `failure_message` - Error message (string)
- `failure_stack` - Stack trace (string, large)

## Common Patterns

### Pattern 1: Status Lookup
```flux
from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.testname == "X")
  |> filter(fn: (r) => r.execution_number == "N")
  |> filter(fn: (r) => r._field == "duration")
  |> keep(columns: ["testname", "status", "execution_number", "_time"])
```

### Pattern 2: Failed Tests
```flux
from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.status == "FAIL")
  |> filter(fn: (r) => r._field == "duration")
  |> keep(columns: ["testname", "status", "owner", "_value"])
```

### Pattern 3: Count by Status
```flux
from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r._field == "duration")
  |> group(columns: ["status"])
  |> count()
  |> rename(columns: {_value: "count"})
```

### Pattern 4: Top N Slowest
```flux
from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r._field == "duration")
  |> sort(columns: ["_value"], desc: true)
  |> limit(n: N)
  |> keep(columns: ["testname", "status", "_value"])
```

## Error Handling

The system automatically handles common errors:

1. **Schema Collision:** Filters by _field before pivot
2. **Missing Columns:** Uses keep() to select existing columns
3. **Join Errors:** Groups correctly before joining
4. **Status Values:** Ensures uppercase (PASS/FAIL/SKIP)
5. **Field Size Limits:** Excludes large fields (failure_stack) from comparisons

## Migration Guide

### Old Code
```python
result = OpenAIQueryGenerationService.generate_flux_with_validation(
    user_query="show failed tests",
    execution_number="3573"
)

if result["success"]:
    # Only have data, no summary
    print(f"Rows: {result['row_count']}")
```

### New Code (Recommended)
```python
result = OpenAIQueryGenerationService.generate_query_with_summary(
    user_query="show failed tests",
    execution_number="3573"
)

if result["success"]:
    # Have both data AND summary
    print(result['summary'])  # Rich insights
    print(f"Rows: {result['row_count']}")
```

## Performance Notes

- Query generation: ~2-5 seconds (with GPT-4/GPT-5)
- Summary generation: ~1-3 seconds
- Total: ~3-8 seconds per query
- Retry adds ~2-5 seconds per attempt

## Best Practices

1. ✅ Use `generate_query_with_summary()` for all new code
2. ✅ Set appropriate `max_retries` (2-3 recommended)
3. ✅ Handle errors gracefully
4. ✅ Display both summary and table to users
5. ✅ Log queries for debugging (automatic)
6. ❌ Don't parse Flux queries manually
7. ❌ Don't skip error handling

---

**Last Updated:** December 25, 2025
**Version:** 2.0

