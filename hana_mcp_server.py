import json
import datetime
import decimal
import os
from pathlib import Path
from mcp.server import Server
from mcp.server.models import Tool
from hdbcli import dbapi

# Hardcoded config path (single source of truth)
CONFIG_PATH = Path(__file__).parent / "hana_config.json"

# JSON serializer for non-JSON-native types
def json_default(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return str(obj)

def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH) as f:
        return json.load(f)

def connect_hana():
    cfg = load_config()
    conn = dbapi.connect(
        address=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        encrypt="true",
        sslValidateCertificate="false"
    )
    return conn

# Tool functions
def list_schemas():
    with connect_hana() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT SCHEMA_NAME FROM SYS.SCHEMAS ORDER BY SCHEMA_NAME")
        rows = cursor.fetchall()
        return json.dumps({"schemas": [r[0] for r in rows]}, indent=2, default=json_default)

def list_tables(schema):
    with connect_hana() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT TABLE_NAME FROM TABLES WHERE SCHEMA_NAME = '{schema}' ORDER BY TABLE_NAME")
        rows = cursor.fetchall()
        return json.dumps({"tables": [r[0] for r in rows]}, indent=2, default=json_default)

def describe_table(schema, table):
    with connect_hana() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE, IS_NULLABLE
            FROM TABLE_COLUMNS
            WHERE SCHEMA_NAME = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY POSITION
        """)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return json.dumps({"columns": cols, "rows": rows}, indent=2, default=json_default)

def run_sql(query):
    with connect_hana() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        cols = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall() if cursor.description else []
        return json.dumps({"columns": cols, "rows": rows}, indent=2, default=json_default)

# MCP server setup
server = Server("hana-mcp")

@server.tool()
def tool_list_schemas() -> str:
    """List all schemas in the HANA database."""
    return list_schemas()

@server.tool()
def tool_list_tables(schema: str) -> str:
    """List all tables for a given schema."""
    return list_tables(schema)

@server.tool()
def tool_describe_table(schema: str, table: str) -> str:
    """Describe columns of a given table."""
    return describe_table(schema, table)

@server.tool()
def tool_run_sql(query: str) -> str:
    """Run arbitrary SQL and return the result as JSON."""
    return run_sql(query)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", help="Run in standalone test mode with given SQL")
    args = parser.parse_args()

    if args.test:
        print(run_sql(args.test))
    else:
        server.run()
