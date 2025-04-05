from typing import Any, List, Dict
import os
from dotenv import load_dotenv
import snowflake.connector
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("snowflake")

# Load environment variables
load_dotenv()


def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="LAUNDRYGO_LIVE",
        schema="PUBLIC",
    )


@mcp.tool()
async def list_tables() -> str:
    """Get list of tables in LAUNDRYGO_LIVE.PUBLIC schema."""
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES IN LAUNDRYGO_LIVE.PUBLIC")
            tables = cursor.fetchall()

            if not tables:
                return "No tables found in LAUNDRYGO_LIVE.PUBLIC"

            table_list = []
            for table in tables:
                table_list.append(f"- {table[1]}")

            return "\n".join(table_list)
    except Exception as e:
        return f"Error fetching tables: {str(e)}"


@mcp.tool()
async def get_table_schema(table_name: str) -> str:
    """Get schema information for a specific table.

    Args:
        table_name: Name of the table to get schema for
    """
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DESCRIBE TABLE LAUNDRYGO_LIVE.PUBLIC.{table_name}")
            columns = cursor.fetchall()

            if not columns:
                return f"No schema found for table {table_name}"

            schema_info = [f"Schema for table {table_name}:"]
            for col in columns:
                schema_info.append(f"- {col[0]} ({col[1]})")

            return "\n".join(schema_info)
    except Exception as e:
        return f"Error fetching schema: {str(e)}"


@mcp.tool()
async def query_table(table_name: str, limit: int = 10) -> str:
    """Query data from a specific table.

    Args:
        table_name: Name of the table to query
        limit: Maximum number of rows to return (default: 10)
    """
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM LAUNDRYGO_LIVE.PUBLIC.{table_name} LIMIT {limit}")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            if not rows:
                return f"No data found in table {table_name}"

            # Format the results
            result = [f"Data from {table_name} (first {limit} rows):"]
            result.append("\nColumns:")
            result.append(", ".join(columns))
            result.append("\nRows:")

            for row in rows:
                result.append(str(row))

            return "\n".join(result)
    except Exception as e:
        return f"Error querying table: {str(e)}"


if __name__ == "__main__":
    print("Starting Snowflake MCP server...")
    mcp.run(transport="stdio")
