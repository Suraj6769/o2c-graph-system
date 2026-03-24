"""
Ingest JSONL files into SQLite.

Supports TWO data layouts:
  Layout A (folder per entity):
    data/
      business_partners/
        part1.jsonl
        part2.jsonl
      sales_order_headers/
        data.jsonl
      ...

  Layout B (flat files):
    data/
      business_partners.jsonl
      sales_order_headers.jsonl
      ...

All 19 O2C entities are supported. Run: python ingest.py
"""

import json
import os
import glob
import sqlite3
from pathlib import Path
from database import get_connection, DB_PATH
from dotenv import load_dotenv
load_dotenv()

# This ensures Render finds the 'data' folder at the root of the project
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = str(BASE_DIR / "data")

# ── All 19 canonical table names ─────────────────────────────────────────────
# Key = folder name OR file stem → Value = SQLite table name
TABLE_MAP = {
    # 1. Customer Master (4)
    "business_partners":                    "business_partners",
    "business_partner_addresses":           "business_partner_addresses",
    "customer_company_assignments":         "customer_company_assignments",
    "customer_sales_area_assignments":      "customer_sales_area_assignments",

    # 2. Sales Order (3)
    "sales_order_headers":                  "sales_order_headers",
    "sales_order_items":                    "sales_order_items",
    "sales_order_schedule_lines":           "sales_order_schedule_lines",

    # 3. Delivery (2)
    "outbound_delivery_headers":            "outbound_delivery_headers",
    "outbound_delivery_items":              "outbound_delivery_items",

    # 4. Billing (3)
    "billing_document_headers":             "billing_document_headers",
    "billing_document_items":               "billing_document_items",
    "billing_document_cancellations":       "billing_document_cancellations",

    # 5. Finance / Accounting (2)
    "payments_accounts_receivable":                  "payments_accounts_receivable",
    "journal_entry_items_accounts_receivable":       "journal_entry_items_accounts_receivable",

    # 6. Product & Supply Chain (6)
    "products":                             "products",
    "product_descriptions":                 "product_descriptions",
    "product_plants":                       "product_plants",
    "product_storage_locations":            "product_storage_locations",
    "plants":                               "plants",
}

# ─────────────────────────────────────────────────────────────────────────────

def load_jsonl_file(path: str) -> list[dict]:
    """Read one .jsonl file → list of dicts. Skips bad lines."""
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                # Some JSONL dumps wrap records in {"value": {...}}
                if isinstance(obj, dict) and list(obj.keys()) == ["value"]:
                    obj = obj["value"]
                if isinstance(obj, dict):
                    records.append(obj)
                elif isinstance(obj, list):
                    records.extend(obj)
            except json.JSONDecodeError as e:
                print(f"    [warn] Line {lineno} in {path}: {e}")
    return records


def infer_columns(records: list[dict]) -> list[str]:
    """Union of all keys across first 200 rows (handles sparse records)."""
    cols: set[str] = set()
    for r in records[:200]:
        cols.update(r.keys())
    return sorted(cols)


def create_or_alter_table(conn: sqlite3.Connection, table: str, columns: list[str]):
    """Create table if missing; ALTER TABLE to add new columns if schema grew."""
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()

    if not exists:
        cols_def = ", ".join(f'"{c}" TEXT' for c in columns)
        conn.execute(f'CREATE TABLE "{table}" ({cols_def})')
        return

    # Table exists — add any missing columns
    existing_cols = {
        row[1] for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    }
    for col in columns:
        if col not in existing_cols:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')
            print(f"    [schema] Added column '{col}' to '{table}'")


def insert_records(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    records: list[dict],
):
    if not records:
        return
    placeholders = ", ".join("?" for _ in columns)
    col_names = ", ".join(f'"{c}"' for c in columns)
    sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'
    rows = [tuple(str(r.get(c, "") or "") for c in columns) for r in records]
    conn.executemany(sql, rows)


# ── Discovery ─────────────────────────────────────────────────────────────────

def discover_entities(data_dir: str) -> dict[str, list[str]]:
    """
    Returns { table_name: [list_of_jsonl_file_paths] }

    Handles both:
      - data/<entity_name>/<anything>.jsonl   (folder layout)
      - data/<entity_name>.jsonl              (flat layout)
    """
    entity_files: dict[str, list[str]] = {}

    # Layout A: scan sub-folders
    for folder in sorted(Path(data_dir).iterdir()):
        if not folder.is_dir():
            continue
        folder_name = folder.name
        table = TABLE_MAP.get(folder_name)
        if table is None:
            # Try case-insensitive match
            table = next(
                (v for k, v in TABLE_MAP.items() if k.lower() == folder_name.lower()),
                folder_name,  # fallback: use folder name as table
            )
        jsonl_files = sorted(folder.glob("**/*.jsonl"))
        if jsonl_files:
            entity_files[table] = [str(p) for p in jsonl_files]
            print(f"[discover] {folder_name}/ → table '{table}' ({len(jsonl_files)} file(s))")

    # Layout B: flat .jsonl files directly in data/
    for fpath in sorted(Path(data_dir).glob("*.jsonl")):
        stem = fpath.stem
        table = TABLE_MAP.get(stem, stem)
        if table not in entity_files:  # don't override folder if both exist
            entity_files[table] = [str(fpath)]
            print(f"[discover] {fpath.name} → table '{table}'")

    return entity_files


# ── Main ingest ───────────────────────────────────────────────────────────────

def ingest_all():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = get_connection()

    entity_files = discover_entities(DATA_DIR)

    if not entity_files:
        print(f"[ingest] No JSONL files found in '{DATA_DIR}/'")
        print(f"         Expected 19 sub-folders or flat .jsonl files.")
        return

    total_rows = 0

    for table, file_paths in entity_files.items():
        print(f"\n[ingest] ── {table} ──")

        # Load all JSONL files for this entity and merge
        all_records: list[dict] = []
        for fpath in file_paths:
            recs = load_jsonl_file(fpath)
            print(f"  {Path(fpath).name}: {len(recs)} records")
            all_records.extend(recs)

        if not all_records:
            print(f"  Skipped (no valid records)")
            continue

        columns = infer_columns(all_records)
        create_or_alter_table(conn, table, columns)

        # Wipe existing rows so ingest is idempotent
        conn.execute(f'DELETE FROM "{table}"')

        insert_records(conn, table, columns, all_records)
        conn.commit()

        total_rows += len(all_records)
        print(f"  {len(all_records)} rows, {len(columns)} columns -> '{table}'")

    _create_indexes(conn)
    conn.close()

    print(f"\n[ingest] Done.")
    print(f"         Tables loaded: {len(entity_files)}/19")
    print(f"         Total rows: {total_rows:,}")
    print(f"         DB: {DB_PATH}")

    # Warn about any missing tables
    expected = set(TABLE_MAP.values())
    loaded = set(entity_files.keys())
    missing = expected - loaded
    if missing:
        print(f"\n[ingest] Missing tables (folders not found):")
        for t in sorted(missing):
            print(f"         - {t}")


def _create_indexes(conn: sqlite3.Connection):
    """Create indexes on all major join keys for fast queries."""
    indexes = [
        # Sales Order
        ("sales_order_headers",        "salesOrder"),
        ("sales_order_items",          "salesOrder"),
        ("sales_order_items",          "material"),
        ("sales_order_schedule_lines", "salesOrder"),

        # Delivery
        ("outbound_delivery_headers",  "deliveryDocument"),
        ("outbound_delivery_items",    "deliveryDocument"),
        ("outbound_delivery_items",    "plant"),

        # Billing
        ("billing_document_headers",       "billingDocument"),
        ("billing_document_headers",       "soldToParty"),
        ("billing_document_items",         "billingDocument"),
        ("billing_document_items",         "material"),
        ("billing_document_cancellations", "cancelledBillingDocument"),

        # Finance
        ("payments_accounts_receivable",            "customer"),
        ("payments_accounts_receivable",            "invoiceReference"),
        ("payments_accounts_receivable",            "accountingDocument"),
        ("journal_entry_items_accounts_receivable", "customer"),

        # Customer Master
        ("business_partners",               "businessPartner"),
        ("business_partners",               "customer"),
        ("business_partner_addresses",      "businessPartner"),
        ("customer_company_assignments",    "customer"),
        ("customer_sales_area_assignments", "customer"),

        # Product
        ("products",                  "product"),
        ("product_descriptions",      "product"),
        ("product_plants",            "product"),
        ("product_plants",            "plant"),
        ("product_storage_locations", "product"),
        ("plants",                    "plant"),
    ]

    created = 0
    for table, col in indexes:
        # Only create if table AND column both exist
        table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if not table_exists:
            continue
        col_exists = any(
            row[1] == col
            for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        )
        if not col_exists:
            continue
        try:
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS idx_{table}_{col} ON "{table}" ("{col}")'
            )
            created += 1
        except Exception as e:
            print(f"  [warn] Index {table}.{col}: {e}")

    conn.commit()
    print(f"[ingest] {created} indexes created")


if __name__ == "__main__":
    ingest_all()