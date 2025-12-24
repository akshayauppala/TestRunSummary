"""
Configuration management module using Singleton pattern.
Centralizes all configuration loading from environment variables.
"""
import os
from typing import Optional
from dotenv import load_dotenv


class Config:
    """
    Singleton configuration class for managing application settings.
    Uses environment variables with fallback to default values.
    """
    _instance: Optional['Config'] = None
    _initialized: bool = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            # Load environment variables from .env file
            load_dotenv()
            
            # OpenAI Configuration
            self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
            
            # InfluxDB Configuration
            self.INFLUX_URL = os.getenv("INFLUX_URL")
            self.INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
            self.INFLUX_ORG = os.getenv("INFLUX_ORG", "myorg")
            self.INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "testexecution")
            
            # Application Configuration
            self.DEFAULT_EXECUTION_NUMBER = os.getenv("DEFAULT_EXECUTION_NUMBER", "1")
            self.MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
            self.OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
            
            Config._initialized = True

    def validate(self) -> tuple[bool, Optional[str]]:
        """
        Validate that all required configuration values are present.
        Returns (is_valid, error_message).
        """
        if not self.OPENAI_API_KEY or self.OPENAI_API_KEY.startswith("your_"):
            return False, "OPENAI_API_KEY is not set or is using placeholder value"
        
        if not self.INFLUX_URL or self.INFLUX_URL.startswith("your_"):
            return False, "INFLUX_URL is not set or is using placeholder value"
        
        if not self.INFLUX_TOKEN or self.INFLUX_TOKEN.startswith("your_"):
            return False, "INFLUX_TOKEN is not set or is using placeholder value"
        
        if not self.INFLUX_ORG or self.INFLUX_ORG.startswith("your_"):
            return False, "INFLUX_ORG is not set or is using placeholder value"
        
        return True, None


# Global configuration instance
config = Config()

