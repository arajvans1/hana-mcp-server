import json
import datetime
import decimal
from pathlib import Path
from hdbcli import dbapi
from mcp.server.fastmcp import FastMCP

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
        sslValidateCertificate="false",
        schema=cfg.get("schema"),
    )

# --- MCP server ---
mcp = FastMCP("hana-mcp")

@mcp.tool()
def list_schemas() -> str:
    """List all schemas in the SAP HANA database."""
    conn = connect_hana()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT SCHEMA_NAME FROM SYS.SCHEMAS ORDER BY SCHEMA_NAME")
        rows = [r[0] for r in cursor.fetchall()]
        return json.dumps(rows, indent=2, default=json_default)
    except Exception as e:
        return f"Error listing schemas: {e}"
    finally:
        try:
            cursor.close()
        finally:
            conn.close()

@mcp.tool()
def list_tables(schema: str) -> str:
    """List all tables in the given schema."""
    conn = connect_hana()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = ? ORDER BY TABLE_NAME",
            (schema.upper(),),
        )
        rows = [r[0] for r in cursor.fetchall()]
        return json.dumps(rows, indent=2, default=json_default)
    except Exception as e:
        return f"Error listing tables for schema {schema}: {e}"
    finally:
        try:
            cursor.close()
        finally:
            conn.close()

@mcp.tool()
def describe_table(schema: str, table: str) -> str:
    """Describe the columns of a given table."""
    conn = connect_hana()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE, IS_NULLABLE
            FROM SYS.TABLE_COLUMNS
            WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
            ORDER BY POSITION
            """,
            (schema.upper(), table.upper()),
        )
        cols = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        return json.dumps({"columns": cols, "rows": rows}, indent=2, default=json_default)
    except Exception as e:
        return f"Error describing table {schema}.{table}: {e}"
    finally:
        try:
            cursor.close()
        finally:
            conn.close()

@mcp.tool()
def run_sql(query: str) -> str:
    """Run a SQL query against the configured SAP HANA DB."""
    conn = connect_hana()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        if cursor.description:  # SELECT or similar
            cols = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return json.dumps({"columns": cols, "rows": rows}, indent=2, default=json_default)
        else:  # INSERT/UPDATE/DELETE
            conn.commit()
            return json.dumps({"message": f"{cursor.rowcount} rows affected."}, indent=2)
    except Exception as e:
        return f"SQL Error: {e}"
    finally:
        try:
            cursor.close()
        finally:
            conn.close()

# --- CLI vs MCP mode ---
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SAP HANA MCP Server")
    parser.add_argument("--test", help="Run a test SQL query and exit")
    args = parser.parse_args()

    if args.test:
        print("[TEST MODE] Running:", args.test)
        print(run_sql(args.test))
    else:
        mcp.run()
