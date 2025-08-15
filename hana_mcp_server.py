import json
import sys
import argparse
from pathlib import Path
from hdbcli import dbapi
from mcp.server.fastmcp import FastMCP

CONFIG_PATH = Path(__file__).parent / "hana_config.json"

def load_config():
    if not CONFIG_PATH.exists():
        print(f"[ERROR] Config file not found: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def connect_hana(cfg):
    try:
        conn = dbapi.connect(
            address=cfg["host"],
            port=int(cfg["port"]),
            user=cfg["user"],
            password=cfg["password"],
            schema=cfg.get("schema"),
            encrypt="true",
            sslValidateCertificate="false"
        )
        print("[INFO] Connected to SAP HANA successfully.")
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to HANA: {e}")
        sys.exit(1)

# --- Load config and connect ---
cfg = load_config()
connection = connect_hana(cfg)

# --- MCP server ---
mcp = FastMCP("hana-mcp")

@mcp.tool()
def list_schemas() -> str:
    """List all schemas in the SAP HANA database."""
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT SCHEMA_NAME FROM SYS.SCHEMAS ORDER BY SCHEMA_NAME")
        rows = [r[0] for r in cursor.fetchall()]
        return json.dumps(rows, indent=2)
    except Exception as e:
        return f"Error listing schemas: {e}"
    finally:
        cursor.close()

@mcp.tool()
def list_tables(schema: str) -> str:
    """List all tables in the given schema."""
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = ? ORDER BY TABLE_NAME",
            (schema.upper(),)
        )
        rows = [r[0] for r in cursor.fetchall()]
        return json.dumps(rows, indent=2)
    except Exception as e:
        return f"Error listing tables for schema {schema}: {e}"
    finally:
        cursor.close()

@mcp.tool()
def describe_table(schema: str, table: str) -> str:
    """Describe the columns of a given table."""
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE, IS_NULLABLE
            FROM SYS.TABLE_COLUMNS
            WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
            ORDER BY POSITION
        """, (schema.upper(), table.upper()))
        cols = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        return json.dumps({"columns": cols, "rows": rows}, indent=2)
    except Exception as e:
        return f"Error describing table {schema}.{table}: {e}"
    finally:
        cursor.close()

@mcp.tool()
def run_sql(query: str) -> str:
    """Run a SQL query against the configured SAP HANA DB."""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        if cursor.description:  # SELECT or similar
            cols = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return json.dumps({"columns": cols, "rows": rows}, indent=2)
        else:  # INSERT/UPDATE/DELETE
            connection.commit()
            return f"{cursor.rowcount} rows affected."
    except Exception as e:
        return f"SQL Error: {e}"
    finally:
        cursor.close()

# --- CLI test mode ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAP HANA MCP Server")
    parser.add_argument("--test", help="Run a test SQL query and exit")
    args = parser.parse_args()

    if args.test:
        print("[TEST MODE] Running:", args.test)
        print(run_sql(args.test))
        sys.exit(0)
    else:
        mcp.run()
