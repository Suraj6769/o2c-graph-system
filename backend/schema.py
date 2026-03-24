"""
Provides the database schema as a string for LLM context injection.
Also defines the O2C domain knowledge used in guardrails.
"""
from database import get_connection

# Domain keywords used to detect off-topic queries
DOMAIN_KEYWORDS = [
    "order", "delivery", "invoice", "billing", "payment", "customer",
    "product", "material", "sales", "shipment", "accounting", "journal",
    "plant", "warehouse", "document", "dispatch", "receivable", "revenue",
    "ledger", "transaction", "business partner", "quantity", "amount",
    "currency", "status", "flow", "trace", "company", "partner",
    "address", "region", "country", "schedule", "confirm", "cancel",
    "outbound", "billing document", "clearing", "gl account",
]

SYSTEM_CONTEXT = """You are a data analyst assistant for an SAP Order-to-Cash (O2C) system.
You help users explore and analyze business data including:
- Sales orders and their line items
- Delivery documents (outbound shipments)
- Billing documents (invoices)
- Payments and accounts receivable
- Customer and product master data
- Accounting journal entries

The complete O2C flow is:
Customer → Sales Order → Delivery → Billing → Payment → Journal Entry

You ONLY answer questions about this dataset. If a user asks something unrelated to this
business domain, respond exactly with:
"This system is designed to answer questions related to the SAP Order-to-Cash dataset only."

When generating SQL:
- Use only tables that exist in the schema
- Always use proper JOINs
- Return at most 200 rows unless asked for more
- Format monetary values with 2 decimal places
- For status fields, treat empty string as unknown
"""


def get_schema_string() -> str:
    """Returns CREATE TABLE statements for all tables — fed to LLM."""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]

    schema_parts = []
    for table in tables:
        cursor = conn.execute(f'PRAGMA table_info("{table}")')
        cols = cursor.fetchall()
        if not cols:
            continue
        col_defs = ", ".join(f'"{c["name"]}" TEXT' for c in cols)
        schema_parts.append(f'CREATE TABLE "{table}" ({col_defs});')

    conn.close()
    return "\n".join(schema_parts)


def get_table_list() -> list[str]:
    conn = get_connection()
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables


def get_table_columns(table: str) -> list[str]:
    conn = get_connection()
    cursor = conn.execute(f'PRAGMA table_info("{table}")')
    cols = [row["name"] for row in cursor.fetchall()]
    conn.close()
    return cols