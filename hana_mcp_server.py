import json
import datetime
import decimal
import asyncio
from pathlib import Path
from hdbcli import dbapi
from mcp.server import Server
from mcp.server.models import Tool

CONFIG_FILE = Path(__file__).parent / "hana_config.json"

# JSON-safe serializer
def json_default(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return str(obj)

# Load config
def load_config():
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_FILE}")
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

# HANA connection
def connect_hana():
    cfg = load_config()
    return dbapi.connect(
        address=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        encrypt="true",
        sslValidateCertificate="false"
    )

# --- Tool functions ---
def list_schemas():
    conn = connect_hana()
    cursor = conn.cursor()
    cursor.execute("SELECT SCHEMA_NAME FROM SYS.SCHEMAS ORDER BY SCHEMA_NAME")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return json.dumps([r[0] for r in rows], indent=2, default=json_default)

def list_tables(schema):
    conn = connect_hana()
    cursor = conn.cursor()
    cursor.execute(f"SELECT TABLE_NAME FROM TABLES WHERE SCHEMA_NAME = '{schema.upper()}' ORDER BY TABLE_NAME")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return json.dumps([r[0] for r in rows], indent=2, default=json_default)

def describe_table(schema, table):
    conn = connect_hana()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE, IS_NULLABLE
        FROM TABLE_COLUMNS
        WHERE SCHEMA_NAME = '{schema.upper()}' AND TABLE_NAME = '{table.upper()}'
        ORDER BY POSITION
    """)
    cols = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return json.dumps({"columns": cols, "rows": rows}, indent=2, default=json_default)

def run_sql(query):
    conn = connect_hana()
    cursor = conn.cursor()
    cursor.execute(query)
    if cursor.description:
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    else:
        cols, rows = [], []
        conn.commit()
    cursor.close()
    conn.close()
    return json.dumps({"columns": cols, "rows": rows}, indent=2, default=json_default)

# --- MCP server setup ---
server = Server("hana-mcp")

# Explicit tool registration
server.add_tool(Tool(name="list_schemas", description="List all schemas", handler=lambda: list_schemas()))
server.add_tool(Tool(name="list_tables", description="List tables in schema", handler=lambda schema: list_tables(schema)))
server.add_tool(Tool(name="describe_table", description="Describe a table", handler=lambda schema, table: describe_table(schema, table)))
server.add_tool(Tool(name="run_sql", description="Run arbitrary SQL", handler=lambda query: run_sql(query)))

# --- CLI vs MCP mode ---
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", help="Run a test SQL query and exit")
    args = parser.parse_args()

    if args.test:
        print(run_sql(args.test))
    else:
        print("[INFO] Starting MCP server... waiting for client connections")
        asyncio.run(server.run_stdio_async())
