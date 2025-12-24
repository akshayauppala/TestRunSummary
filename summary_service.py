"""
Summary service for generating intelligent summaries of test execution data.
Implements analysis patterns for build comparisons, script analysis, and flaky test detection.
"""
from typing import Dict, Any, List, Optional
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from services import FluxQueryService
from config import config


class FailureCategoryAnalyzer:
    """
    Analyzes failure stacks to categorize failure reasons.
    """
    
    FAILURE_PATTERNS = {
        "toast": {
            "keywords": [".toast", "toast", "Toast"],
            "description": "Toast notification issues"
        },
        "visibility": {
            "keywords": ["By.cssSelector", "waiting for visibility", "visibility", "element not visible", "ElementNotVisibleException"],
            "description": "Element visibility issues"
        },
        "timeout": {
            "keywords": ["timeout", "TimeoutException", "waiting", "timed out"],
            "description": "Timeout issues"
        },
        "element_not_found": {
            "keywords": ["NoSuchElementException", "element not found", "could not find"],
            "description": "Element not found issues"
        },
        "assertion": {
            "keywords": ["AssertionError", "assert", "expected", "actual"],
            "description": "Assertion failures"
        },
        "network": {
            "keywords": ["network", "connection", "HttpException", "500", "404"],
            "description": "Network/API issues"
        }
    }
    
    @staticmethod
    def categorize_failure(failure_stack: str) -> List[str]:
        """
        Categorize a failure based on its stack trace.
        
        Args:
            failure_stack: The failure stack trace string
            
        Returns:
            List of failure categories
        """
        if not failure_stack or not isinstance(failure_stack, str):
            return ["unknown"]
        
        failure_stack_lower = failure_stack.lower()
        categories = []
        
        for category, pattern_info in FailureCategoryAnalyzer.FAILURE_PATTERNS.items():
            for keyword in pattern_info["keywords"]:
                if keyword.lower() in failure_stack_lower:
                    categories.append(category)
                    break
        
        return categories if categories else ["unknown"]
    
    @staticmethod
    def get_category_description(category: str) -> str:
        """Get human-readable description for a failure category."""
        return FailureCategoryAnalyzer.FAILURE_PATTERNS.get(
            category, 
            {"description": "Unknown issue"}
        )["description"]


class SummaryService:
    """
    Service for generating intelligent summaries of test execution data.
    """
    
    @staticmethod
    def get_latest_execution_number() -> Optional[str]:
        """
        Get the highest/latest execution number from InfluxDB.
        
        Returns:
            Latest execution number as string, or None if not found
        """
        query = '''
from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => exists r.execution_number)
  |> group(columns: ["execution_number"])
  |> distinct(column: "execution_number")
  |> sort(columns: ["execution_number"], desc: true)
  |> limit(n: 1)
  |> keep(columns: ["execution_number"])
'''
        result = FluxQueryService.execute_flux_query(query)
        
        if result["success"] and result["data"]:
            for record in result["data"]:
                if "execution_number" in record:
                    return str(record["execution_number"])
        return None
    
    @staticmethod
    def generate_build_summary(execution_number: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate summary for a build showing failed/skipped scripts with categorized failures.
        
        Args:
            execution_number: Specific execution number, or None for latest
            
        Returns:
            Dictionary with summary data
        """
        if execution_number is None:
            execution_number = SummaryService.get_latest_execution_number()
            if not execution_number:
                return {
                    "success": False,
                    "error": "No execution data found",
                    "summary": None
                }
        
        # Query failed and skipped tests with failure stacks
        query = f'''
duration_data = from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.execution_number == "{execution_number}")
  |> filter(fn: (r) => r.status == "FAIL" or r.status == "SKIP")
  |> filter(fn: (r) => r._field == "duration")
  |> keep(columns: ["_time", "testname", "status", "execution_number"])

failure_data = from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.execution_number == "{execution_number}")
  |> filter(fn: (r) => r.status == "FAIL" or r.status == "SKIP")
  |> filter(fn: (r) => r._field == "failure_stack")
  |> keep(columns: ["_time", "testname", "status", "execution_number", "_value"])
  |> rename(columns: {{_value: "failure_stack"}})

join(tables: {{d: duration_data, f: failure_data}}, on: ["_time", "testname", "status", "execution_number"])
  |> group()
  |> keep(columns: ["testname", "status", "failure_stack"])
'''
        
        result = FluxQueryService.execute_flux_query(query, execution_number)
        
        if not result["success"]:
            return {
                "success": False,
                "error": result.get("error", "Query execution failed"),
                "summary": None
            }
        
        # Process results
        scripts = []
        failure_categories = defaultdict(list)
        
        for record in result["data"]:
            testname = record.get("testname", "Unknown")
            status = record.get("status", "UNKNOWN")
            failure_stack = record.get("failure_stack", "")
            
            scripts.append({
                "testname": testname,
                "status": status,
                "failure_stack": failure_stack
            })
            
            if status == "FAIL" and failure_stack:
                categories = FailureCategoryAnalyzer.categorize_failure(failure_stack)
                for category in categories:
                    failure_categories[category].append(testname)
        
        # Generate summary text
        summary_parts = [
            f"## Build Summary (Execution #{execution_number})\n",
            f"**Total Failed/Skipped Scripts:** {len(scripts)}\n"
        ]
        
        if failure_categories:
            summary_parts.append("\n### Failure Categories:\n")
            for category, testnames in failure_categories.items():
                unique_tests = list(set(testnames))
                category_desc = FailureCategoryAnalyzer.get_category_description(category)
                summary_parts.append(
                    f"- **{category_desc}** ({len(unique_tests)} scripts): {', '.join(unique_tests[:10])}"
                )
                if len(unique_tests) > 10:
                    summary_parts.append(f"  ... and {len(unique_tests) - 10} more")
        
        summary_parts.append("\n### Failed/Skipped Scripts:\n")
        for script in scripts[:20]:  # Show first 20
            summary_parts.append(f"- {script['testname']} ({script['status']})")
        
        if len(scripts) > 20:
            summary_parts.append(f"\n... and {len(scripts) - 20} more scripts")
        
        return {
            "success": True,
            "execution_number": execution_number,
            "total_scripts": len(scripts),
            "scripts": scripts,
            "failure_categories": dict(failure_categories),
            "summary": "\n".join(summary_parts)
        }
    
    @staticmethod
    def generate_script_summary(script_name: str) -> Dict[str, Any]:
        """
        Generate detailed summary for a specific script showing all failure reasons.
        
        Args:
            script_name: Name of the script/test to analyze
            
        Returns:
            Dictionary with script analysis
        """
        # Query all executions of this script
        query = f'''
duration_data = from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.testname =~ /.*{script_name}.*/)
  |> filter(fn: (r) => r._field == "duration")
  |> keep(columns: ["_time", "testname", "status", "execution_number"])

failure_data = from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.testname =~ /.*{script_name}.*/)
  |> filter(fn: (r) => r._field == "failure_stack")
  |> keep(columns: ["_time", "testname", "status", "execution_number", "_value"])
  |> rename(columns: {{_value: "failure_stack"}})

join(tables: {{d: duration_data, f: failure_data}}, on: ["_time", "testname", "status", "execution_number"])
  |> group()
  |> keep(columns: ["_time", "testname", "status", "execution_number", "failure_stack"])
  |> sort(columns: ["_time"], desc: true)
'''
        
        result = FluxQueryService.execute_flux_query(query)
        
        if not result["success"]:
            return {
                "success": False,
                "error": result.get("error", "Query execution failed"),
                "summary": None
            }
        
        executions = result["data"]
        total_executions = len(executions)
        failed_executions = [e for e in executions if e.get("status") == "FAIL"]
        passed_executions = [e for e in executions if e.get("status") == "PASS"]
        skipped_executions = [e for e in executions if e.get("status") == "SKIP"]
        
        # Analyze failure reasons
        failure_reasons = defaultdict(int)
        failure_details = []
        
        for execution in failed_executions:
            failure_stack = execution.get("failure_stack", "")
            if failure_stack:
                categories = FailureCategoryAnalyzer.categorize_failure(failure_stack)
                for category in categories:
                    failure_reasons[category] += 1
                
                failure_details.append({
                    "execution_number": execution.get("execution_number", "Unknown"),
                    "timestamp": execution.get("_time", "Unknown"),
                    "categories": categories,
                    "failure_stack": failure_stack[:500]  # Truncate for display
                })
        
        # Generate summary
        summary_parts = [
            f"## Script Analysis: {script_name}\n",
            f"**Total Executions:** {total_executions}\n",
            f"- Passed: {len(passed_executions)}\n",
            f"- Failed: {len(failed_executions)}\n",
            f"- Skipped: {len(skipped_executions)}\n"
        ]
        
        if failed_executions:
            success_rate = (len(passed_executions) / total_executions * 100) if total_executions > 0 else 0
            summary_parts.append(f"\n**Success Rate:** {success_rate:.1f}%\n")
            
            summary_parts.append("\n### Failure Reasons:\n")
            for category, count in sorted(failure_reasons.items(), key=lambda x: x[1], reverse=True):
                category_desc = FailureCategoryAnalyzer.get_category_description(category)
                percentage = (count / len(failed_executions) * 100) if failed_executions else 0
                summary_parts.append(f"- **{category_desc}**: {count} times ({percentage:.1f}% of failures)")
            
            summary_parts.append("\n### Recent Failures:\n")
            for detail in failure_details[:5]:  # Show last 5 failures
                categories_str = ", ".join([
                    FailureCategoryAnalyzer.get_category_description(c) 
                    for c in detail["categories"]
                ])
                summary_parts.append(
                    f"- Execution #{detail['execution_number']} ({detail['timestamp']}): {categories_str}"
                )
        else:
            summary_parts.append("\n✅ No failures recorded for this script.")
        
        return {
            "success": True,
            "script_name": script_name,
            "total_executions": total_executions,
            "passed": len(passed_executions),
            "failed": len(failed_executions),
            "skipped": len(skipped_executions),
            "failure_reasons": dict(failure_reasons),
            "failure_details": failure_details,
            "executions": executions,  # Include all execution data for table display
            "summary": "\n".join(summary_parts)
        }
    
    @staticmethod
    def generate_flaky_scripts_summary() -> Dict[str, Any]:
        """
        Generate summary of flaky scripts showing failure count in last 7 days and reasons.
        
        Returns:
            Dictionary with flaky scripts analysis
        """
        # Calculate 7 days ago timestamp
        seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat() + "Z"
        
        # Query flaky tests (tests that have both PASS and FAIL in last 7 days)
        # First get status counts
        status_query = f'''
from(bucket: "testexecution")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r._field == "duration")
  |> group(columns: ["testname"])
  |> reduce(
      identity: {{testname: "", pass_count: 0, fail_count: 0, total: 0}},
      fn: (r, accumulator) => ({{
          testname: r.testname,
          pass_count: accumulator.pass_count + (if r.status == "PASS" then 1 else 0),
          fail_count: accumulator.fail_count + (if r.status == "FAIL" then 1 else 0),
          total: accumulator.total + 1
      }})
  )
  |> filter(fn: (r) => r.pass_count > 0 and r.fail_count > 0)
  |> map(fn: (r) => ({{
      testname: r.testname,
      pass_count: r.pass_count,
      fail_count: r.fail_count,
      total: r.total,
      flakiness_score: float(v: r.fail_count) / float(v: r.total)
  }}))
  |> sort(columns: ["flakiness_score"], desc: true)
'''
        
        # Get failure stacks for flaky tests
        failure_query = f'''
from(bucket: "testexecution")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r._field == "failure_stack")
  |> filter(fn: (r) => r.status == "FAIL")
  |> group()
  |> keep(columns: ["testname", "_value"])
  |> rename(columns: {{_value: "failure_stack"}})
'''
        
        # Execute both queries
        status_result = FluxQueryService.execute_flux_query(status_query)
        failure_result = FluxQueryService.execute_flux_query(failure_query)
        
        if not status_result["success"]:
            return {
                "success": False,
                "error": status_result.get("error", "Query execution failed"),
                "summary": None
            }
        
        # Build failure stacks map
        failure_stacks_map = defaultdict(list)
        if failure_result["success"]:
            for record in failure_result["data"]:
                testname = record.get("testname", "")
                failure_stack = record.get("failure_stack", "")
                if testname and failure_stack:
                    failure_stacks_map[testname].append(failure_stack)
        
        # Process flaky scripts
        flaky_scripts = []
        for record in status_result["data"]:
            testname = record.get("testname", "Unknown")
            fail_count = record.get("fail_count", 0)
            pass_count = record.get("pass_count", 0)
            total = record.get("total", 0)
            flakiness_score = record.get("flakiness_score", 0.0)
            failure_stacks = failure_stacks_map.get(testname, [])
            
            # Analyze failure reasons
            failure_categories = Counter()
            for stack in failure_stacks:
                if stack:
                    categories = FailureCategoryAnalyzer.categorize_failure(str(stack))
                    failure_categories.update(categories)
            
            # Get most common failure reason
            most_common_reason = "unknown"
            if failure_categories:
                most_common_reason = failure_categories.most_common(1)[0][0]
            
            flaky_scripts.append({
                "testname": testname,
                "fail_count": fail_count,
                "pass_count": pass_count,
                "total": total,
                "flakiness_score": flakiness_score,
                "failure_reason": most_common_reason,
                "failure_categories": dict(failure_categories)
            })
        
        # Generate summary
        summary_parts = [
            "## Flaky Scripts Summary (Last 7 Days)\n",
            f"**Total Flaky Scripts:** {len(flaky_scripts)}\n"
        ]
        
        if flaky_scripts:
            summary_parts.append("\n### Top Flaky Scripts:\n")
            for script in flaky_scripts[:10]:  # Show top 10
                reason_desc = FailureCategoryAnalyzer.get_category_description(script["failure_reason"])
                flakiness_pct = script["flakiness_score"] * 100
                summary_parts.append(
                    f"- **{script['testname']}**: Failed {script['fail_count']} times in last 7 days "
                    f"({flakiness_pct:.1f}% failure rate) - Main reason: {reason_desc}"
                )
        else:
            summary_parts.append("\n✅ No flaky scripts found in the last 7 days.")
        
        return {
            "success": True,
            "total_flaky": len(flaky_scripts),
            "flaky_scripts": flaky_scripts,
            "summary": "\n".join(summary_parts)
        }
        
        result = FluxQueryService.execute_flux_query(query)
        
        if not result["success"]:
            return {
                "success": False,
                "error": result.get("error", "Query execution failed"),
                "summary": None
            }
        
        flaky_scripts = []
        
        for record in result["data"]:
            testname = record.get("testname", "Unknown")
            fail_count = record.get("fail_count", 0)
            pass_count = record.get("pass_count", 0)
            total = record.get("total", 0)
            flakiness_score = record.get("flakiness_score", 0.0)
            failure_stacks = record.get("failure_stacks", [])
            
            # Analyze failure reasons
            failure_categories = Counter()
            for stack in failure_stacks:
                if stack:
                    categories = FailureCategoryAnalyzer.categorize_failure(str(stack))
                    failure_categories.update(categories)
            
            # Get most common failure reason
            most_common_reason = "unknown"
            if failure_categories:
                most_common_reason = failure_categories.most_common(1)[0][0]
            
            flaky_scripts.append({
                "testname": testname,
                "fail_count": fail_count,
                "pass_count": pass_count,
                "total": total,
                "flakiness_score": flakiness_score,
                "failure_reason": most_common_reason,
                "failure_categories": dict(failure_categories)
            })
        
        # Generate summary
        summary_parts = [
            "## Flaky Scripts Summary (Last 7 Days)\n",
            f"**Total Flaky Scripts:** {len(flaky_scripts)}\n"
        ]
        
        if flaky_scripts:
            summary_parts.append("\n### Top Flaky Scripts:\n")
            for script in flaky_scripts[:10]:  # Show top 10
                reason_desc = FailureCategoryAnalyzer.get_category_description(script["failure_reason"])
                flakiness_pct = script["flakiness_score"] * 100
                summary_parts.append(
                    f"- **{script['testname']}**: Failed {script['fail_count']} times in last 7 days "
                    f"({flakiness_pct:.1f}% failure rate) - Main reason: {reason_desc}"
                )
        else:
            summary_parts.append("\n✅ No flaky scripts found in the last 7 days.")
        
        return {
            "success": True,
            "total_flaky": len(flaky_scripts),
            "flaky_scripts": flaky_scripts,
            "summary": "\n".join(summary_parts)
        }


    @staticmethod
    def generate_top_flaky_scripts(limit: int = 10) -> Dict[str, Any]:
        """
        Generate top N flaky scripts sorted by flakiness_score in descending order.
        Considers last 7 days of data.
        
        Args:
            limit: Number of top scripts to return (default: 10)
            
        Returns:
            Dictionary with top flaky scripts data
        """
        # Query flaky tests (tests that have both PASS and FAIL in last 7 days)
        query = f'''
from(bucket: "testexecution")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r._field == "duration")
  |> group(columns: ["testname"])
  |> reduce(
      identity: {{testname: "", pass_count: 0, fail_count: 0, total: 0}},
      fn: (r, accumulator) => ({{
          testname: r.testname,
          pass_count: accumulator.pass_count + (if r.status == "PASS" then 1 else 0),
          fail_count: accumulator.fail_count + (if r.status == "FAIL" then 1 else 0),
          total: accumulator.total + 1
      }})
  )
  |> filter(fn: (r) => r.pass_count > 0 and r.fail_count > 0)
  |> map(fn: (r) => ({{
      testname: r.testname,
      pass_count: r.pass_count,
      fail_count: r.fail_count,
      total: r.total,
      flakiness_score: float(v: r.fail_count) / float(v: r.total)
  }}))
  |> sort(columns: ["flakiness_score"], desc: true)
  |> limit(n: {limit})
'''
        
        result = FluxQueryService.execute_flux_query(query)
        
        if not result["success"]:
            return {
                "success": False,
                "error": result.get("error", "Query execution failed"),
                "data": None
            }
        
        scripts = result["data"]
        
        return {
            "success": True,
            "scripts": scripts,
            "total": len(scripts),
            "limit": limit
        }
    
    @staticmethod
    def generate_top_failing_scripts(limit: int = 10) -> Dict[str, Any]:
        """
        Generate top N failing scripts sorted by fail count in descending order.
        Considers last 7 days of data.
        
        Args:
            limit: Number of top scripts to return (default: 10)
            
        Returns:
            Dictionary with top failing scripts data
        """
        # Query failing tests in last 7 days, sorted by fail count
        query = f'''
from(bucket: "testexecution")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r._field == "duration")
  |> filter(fn: (r) => r.status == "FAIL")
  |> group(columns: ["testname"])
  |> count()
  |> sort(columns: ["_value"], desc: true)
  |> limit(n: {limit})
  |> rename(columns: {{_value: "fail_count"}})
'''
        
        result = FluxQueryService.execute_flux_query(query)
        
        if not result["success"]:
            return {
                "success": False,
                "error": result.get("error", "Query execution failed"),
                "data": None
            }
        
        scripts = result["data"]
        
        return {
            "success": True,
            "scripts": scripts,
            "total": len(scripts),
            "limit": limit
        }

    @staticmethod
    def generate_build_comparison_summary(execution1: Optional[str] = None, execution2: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate summary for build comparison showing changed tests with categorized failures.
        
        This function is automatically called when build comparison queries are detected, such as:
        - "compare build"
        - "compare build X vs Y"
        - "compare execution X vs Y"
        - "yesterday vs today"
        - "previous vs current"
        
        The word "summary" is NOT required in the query - any build comparison query will trigger this.
        
        Args:
            execution1: First execution number (or None to auto-select smallest). 
                       If both are provided, the function will automatically ensure execution1 < execution2.
            execution2: Second execution number (or None to auto-select largest/latest).
                       If both are provided, the function will automatically ensure execution1 < execution2.
            
        Returns:
            Dictionary with build comparison summary containing:
            - execution1: Smaller execution number used
            - execution2: Larger execution number used
            - total_changed: Number of tests that changed from PASS to FAIL/SKIP
            - changed_tests: List of changed tests
            - status_changes: Dictionary of status change types
            - failure_categories: Dictionary of failure categories
            - summary: Formatted markdown summary text
        """
        # If both execution numbers are provided, ensure correct order first
        if execution1 is not None and execution2 is not None:
            # Ensure execution1 < execution2 (build1 should be smaller)
            try:
                exec1_int = int(execution1)
                exec2_int = int(execution2)
                if exec1_int > exec2_int:
                    execution1, execution2 = execution2, execution1
            except (ValueError, TypeError):
                # If not numeric, try string comparison
                if str(execution1) > str(execution2):
                    execution1, execution2 = execution2, execution1
        
        # Get list of execution numbers if either is None
        if execution1 is None or execution2 is None:
            # Get list of execution numbers and find the one before execution2
            query = '''
from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => exists r.execution_number)
  |> keep(columns: ["execution_number"])
  |> group(columns: ["execution_number"])
  |> distinct(column: "execution_number")
  |> sort(columns: ["execution_number"], desc: true)
  |> limit(n: 100)
'''
        result = FluxQueryService.execute_flux_query(query)
        
        if not result["success"]:
            error_msg = result.get("error", "Unknown error")
            return {
                "success": False,
                "error": f"Failed to query execution numbers: {error_msg}",
                "summary": None
            }
        
        if not result["data"]:
            return {
                "success": False,
                "error": "No execution data found in database",
                "summary": None
            }
        
        # Extract execution numbers - try multiple possible keys
        executions = []
        for record in result["data"]:
            # Try different possible keys for execution_number
            exec_num = record.get("execution_number") or record.get("_value") or record.get("executionNumber")
            if exec_num:
                exec_str = str(exec_num).strip()
                if exec_str and exec_str not in executions:
                    executions.append(exec_str)
        
        if not executions:
            # Provide debug info
            sample_data = result["data"][:2] if result["data"] else []
            return {
                "success": False,
                "error": f"No execution numbers found. Sample data: {sample_data}",
                "summary": None
            }
        
        # If execution2 is None, use largest (latest) - first in desc sorted list
        if execution2 is None:
            execution2 = executions[0]  # First one is largest (desc sorted)
        
        # If execution1 is None, use smallest - last in desc sorted list
        if execution1 is None:
            execution1 = executions[-1]  # Last one is smallest
        
        # Ensure execution1 < execution2 (build1 should be smaller)
        # Convert to int for comparison, but keep as strings for the query
        try:
            exec1_int = int(execution1)
            exec2_int = int(execution2)
            if exec1_int > exec2_int:
                execution1, execution2 = execution2, execution1
        except (ValueError, TypeError):
            # If not numeric, try string comparison
            if str(execution1) > str(execution2):
                execution1, execution2 = execution2, execution1
        
        # Query build comparison data
        query = f'''
build1 = from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.execution_number == "{execution1}")
  |> filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")
  |> pivot(rowKey: ["testname"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {{status: "previous_status"}})

build2 = from(bucket: "testexecution")
  |> range(start: 1970-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "testmethod")
  |> filter(fn: (r) => r.execution_number == "{execution2}")
  |> filter(fn: (r) => r._field == "duration" or r._field == "failure_stack")
  |> pivot(rowKey: ["testname"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {{status: "current_status"}})
  |> rename(columns: {{failure_stack: "current_failure_stack"}})

join(tables: {{b1: build1, b2: build2}}, on: ["testname"])
  |> filter(fn: (r) => r.previous_status == "PASS" and (r.current_status == "FAIL" or r.current_status == "SKIP"))
  |> keep(columns: ["testname", "previous_status", "current_status", "current_failure_stack"])
  |> group()
'''
        
        result = FluxQueryService.execute_flux_query(query)
        
        if not result["success"]:
            return {
                "success": False,
                "error": result.get("error", "Query execution failed"),
                "summary": None
            }
        
        changed_tests = result["data"]
        total_changed = len(changed_tests)
        
        # Categorize failures
        failure_categories = defaultdict(list)
        status_changes = {
            "PASS→FAIL": [],
            "PASS→SKIP": [],
            "FAIL→PASS": [],
            "SKIP→PASS": [],
            "FAIL→SKIP": [],
            "SKIP→FAIL": []
        }
        
        for test in changed_tests:
            testname = test.get("testname", "Unknown")
            previous_status = test.get("previous_status", "")
            current_status = test.get("current_status", "")
            failure_stack = test.get("current_failure_stack", "")
            
            change_key = f"{previous_status}→{current_status}"
            if change_key in status_changes:
                status_changes[change_key].append(testname)
            
            if current_status == "FAIL" and failure_stack:
                categories = FailureCategoryAnalyzer.categorize_failure(failure_stack)
                for category in categories:
                    failure_categories[category].append(testname)
        
        # Generate summary
        summary_parts = [
            f"## Build Comparison Summary\n",
            f"**Build 1 (Previous - Smaller Execution):** Execution #{execution1}\n",
            f"**Build 2 (Current - Larger Execution):** Execution #{execution2}\n",
            f"**Total Tests Changed (PASS → FAIL/SKIP):** {total_changed}\n"
        ]
        
        if status_changes:
            summary_parts.append("\n### Status Changes:\n")
            for change_type, tests in status_changes.items():
                if tests:
                    summary_parts.append(f"- **{change_type}**: {len(tests)} tests")
                    summary_parts.append(f"  - {', '.join(tests[:5])}")
                    if len(tests) > 5:
                        summary_parts.append(f"  - ... and {len(tests) - 5} more")
        
        if failure_categories:
            summary_parts.append("\n### Failure Categories in Build 2:\n")
            for category, testnames in failure_categories.items():
                unique_tests = list(set(testnames))
                category_desc = FailureCategoryAnalyzer.get_category_description(category)
                summary_parts.append(
                    f"- **{category_desc}** ({len(unique_tests)} tests): {', '.join(unique_tests[:5])}"
                )
                if len(unique_tests) > 5:
                    summary_parts.append(f"  ... and {len(unique_tests) - 5} more")
        
        if not changed_tests:
            summary_parts.append("\n✅ No status changes between builds.")
        
        return {
            "success": True,
            "execution1": execution1,
            "execution2": execution2,
            "total_changed": total_changed,
            "changed_tests": changed_tests,
            "status_changes": dict(status_changes),
            "failure_categories": dict(failure_categories),
            "summary": "\n".join(summary_parts)
        }
