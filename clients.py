"""
Client factory module using Factory pattern.
Creates and manages client instances for external services.
"""
import streamlit as st
from openai import OpenAI
from influxdb_client import InfluxDBClient
from config import config


class ClientFactory:
    """
    Factory class for creating and managing client instances.
    Implements Singleton pattern for cached clients.
    """
    
    @staticmethod
    @st.cache_resource
    def get_openai_client() -> OpenAI:
        """
        Create and cache OpenAI client instance.
        Uses Streamlit's cache_resource for efficient resource management.
        """
        return OpenAI(api_key=config.OPENAI_API_KEY)
    
    @staticmethod
    @st.cache_resource
    def get_influx_client() -> InfluxDBClient:
        """
        Create and cache InfluxDB client instance.
        Uses Streamlit's cache_resource for efficient resource management.
        
        Returns:
            InfluxDBClient instance or None if connection fails
        """
        try:
            client = InfluxDBClient(
                url=config.INFLUX_URL,
                token=config.INFLUX_TOKEN,
                org=config.INFLUX_ORG
            )
            return client
        except Exception as e:
            st.error(f"Failed to connect to InfluxDB: {str(e)}")
            return None

