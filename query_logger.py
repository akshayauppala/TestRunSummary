"""
Query logging service using Singleton pattern.
Logs user queries and generated Flux queries to a JSON file.
"""
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path


class QueryLogger:
    """
    Singleton logger class for tracking user queries and Flux queries.
    Implements Observer pattern for query tracking.
    """
    _instance: Optional['QueryLogger'] = None
    _initialized: bool = False
    _log_file: str = "query_logs.json"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QueryLogger, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not QueryLogger._initialized:
            self.log_file_path = Path(self._log_file)
            self._ensure_log_file_exists()
            QueryLogger._initialized = True

    def _ensure_log_file_exists(self):
        """Create log file if it doesn't exist with empty array structure."""
        if not self.log_file_path.exists():
            with open(self.log_file_path, 'w') as f:
                json.dump([], f, indent=2)

    def _read_logs(self) -> list:
        """Read existing logs from file."""
        try:
            if self.log_file_path.exists() and self.log_file_path.stat().st_size > 0:
                with open(self.log_file_path, 'r') as f:
                    return json.load(f)
            return []
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading log file: {e}")
            return []

    def _write_logs(self, logs: list):
        """Write logs to file."""
        try:
            with open(self.log_file_path, 'w') as f:
                json.dump(logs, f, indent=2)
        except IOError as e:
            print(f"Error writing to log file: {e}")

    def log_query(
        self,
        user_query: str,
        flux_query: str,
        execution_number: str,
        success: bool,
        row_count: int = 0,
        error: Optional[str] = None,
        attempts: int = 1
    ):
        """
        Log a user query and its corresponding Flux query.
        
        Args:
            user_query: The natural language query from the user
            flux_query: The generated Flux query
            execution_number: Execution number used
            success: Whether the query execution was successful
            row_count: Number of rows returned
            error: Error message if query failed
            attempts: Number of attempts made
        """
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "user_query": user_query,
            "flux_query": flux_query,
            "execution_number": execution_number,
            "success": success,
            "row_count": row_count,
            "attempts": attempts,
            "error": error
        }

        logs = self._read_logs()
        logs.append(log_entry)
        
        # Keep only last 1000 entries to prevent file from growing too large
        if len(logs) > 1000:
            logs = logs[-1000:]
        
        self._write_logs(logs)

    def get_recent_logs(self, limit: int = 50) -> list:
        """
        Get recent log entries.
        
        Args:
            limit: Maximum number of entries to return
            
        Returns:
            List of recent log entries
        """
        logs = self._read_logs()
        return logs[-limit:] if len(logs) > limit else logs

    def get_logs_by_query(self, search_term: str) -> list:
        """
        Search logs by user query or Flux query content.
        
        Args:
            search_term: Term to search for in queries
            
        Returns:
            List of matching log entries
        """
        logs = self._read_logs()
        search_term_lower = search_term.lower()
        return [
            log for log in logs
            if search_term_lower in log.get("user_query", "").lower()
            or search_term_lower in log.get("flux_query", "").lower()
        ]

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about logged queries.
        
        Returns:
            Dictionary with query statistics
        """
        logs = self._read_logs()
        if not logs:
            return {
                "total_queries": 0,
                "successful_queries": 0,
                "failed_queries": 0,
                "success_rate": 0.0,
                "average_attempts": 0.0,
                "total_rows_returned": 0
            }

        successful = sum(1 for log in logs if log.get("success", False))
        failed = len(logs) - successful
        total_attempts = sum(log.get("attempts", 1) for log in logs)
        total_rows = sum(log.get("row_count", 0) for log in logs)

        return {
            "total_queries": len(logs),
            "successful_queries": successful,
            "failed_queries": failed,
            "success_rate": round(successful / len(logs) * 100, 2) if logs else 0.0,
            "average_attempts": round(total_attempts / len(logs), 2) if logs else 0.0,
            "total_rows_returned": total_rows
        }


# Global logger instance
query_logger = QueryLogger()

