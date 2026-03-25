"""
LLM Service with professional SAP formatting and table/column citations.
"""

import os
import re
import sqlite3
from typing import Any
from dotenv import load_dotenv
load_dotenv()

from database import get_connection
from schema import get_schema_string, DOMAIN_KEYWORDS

LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "gemini")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

OFF_TOPIC_RESPONSE = (
    "This system is designed to answer questions related to the "
    "SAP Order-to-Cash dataset only."
)

MAX_ROWS = 200
MAX_SQL_RETRIES = 2

# ── Guardrail ─────────────────────────────────────────────────────────────────

def is_on_topic(query: str) -> bool:
    q = query.lower()
    off_topic_patterns = [
        r"\bwrite (a |an )?(poem|story|essay|song|joke|recipe)\b",
        r"\bwho (is|was) (the )?(president|prime minister|king|queen)\b",
        r"\bweather\b", r"\bsocial media\b", r"\bsports\b",
        r"\bmovie\b", r"\bmusic\b", r"\bcelebrit", r"\bcookbook\b", r"\bhoroscope\b",
    ]
    for pat in off_topic_patterns:
        if re.search(pat, q):
            return False
    return any(kw in q for kw in DOMAIN_KEYWORDS)

# ── LLM Calls ─────────────────────────────────────────────────────────────────

def _call_gemini(prompt: str) -> str:
    import google.generativeai as genai
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.0-flash")
    resp = model.generate_content(prompt)
    return resp.text.strip()

def _call_groq(prompt: str) -> str:
    from groq import Groq
    client = Groq(api_key=GROQ_API_KEY)
    resp = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return resp.choices[0].message.content.strip()

def _call_llm(prompt: str) -> str:
    if LLM_PROVIDER == "groq":
        return _call_groq(prompt)
    return _call_gemini(prompt)

# ── SQL Generation ─────────────────────────────────────────────────────────────

SQL_GEN_PROMPT = """You are a SQLite expert for an SAP Order-to-Cash system.

DATABASE SCHEMA:
{schema}

USER QUESTION: {question}

Generate ONE valid SQLite SQL query that answers this question.

STRICT RULES:
- Output ONLY the raw SQL query. No explanation, no markdown, no backticks, no preamble.
- Use only tables and columns that exist in the schema above.
- Double-quote all identifiers: "tableName"."columnName"
- LIMIT to {max_rows} rows maximum.
- Never use DROP, DELETE, INSERT, UPDATE, CREATE, or ALTER.

CRITICAL SAP JOIN RULES:
- NEVER join document headers directly to other document headers. Always use the item tables and reference fields.
- Product to Billing: Join "products"."product" to "billing_document_items"."material", then join to "billing_document_headers"."billingDocument"
- Sales Order to Delivery: Join "sales_order_items"."salesOrder" to "outbound_delivery_items"."ReferenceDocument"
- Delivery to Billing: Join "outbound_delivery_items"."DeliveryDocument" to "billing_document_items"."ReferenceDocument"
- "Broken flows": Use LEFT JOIN and check for IS NULL on the right-side key.

SQL:"""

def generate_sql(question: str) -> str:
    schema = get_schema_string()
    prompt = SQL_GEN_PROMPT.format(schema=schema, question=question, max_rows=MAX_ROWS)
    raw = _call_llm(prompt)
    raw = re.sub(r"```sql\s*", "", raw, flags=re.I)
    raw = re.sub(r"```\s*", "", raw)
    return raw.strip()

# ── SQL Execution ─────────────────────────────────────────────────────────────

def execute_sql(sql: str) -> tuple[list[str], list[tuple]]:
    forbidden = re.compile(
        r"\b(DROP|DELETE|INSERT|UPDATE|CREATE|ALTER|ATTACH|DETACH|PRAGMA)\b", re.I)
    if forbidden.search(sql):
        raise ValueError("Mutating SQL is not allowed.")
    conn = get_connection()
    try:
        cursor = conn.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchmany(MAX_ROWS)
        return columns, rows
    finally:
        conn.close()

# ── NL Answer Generation (Formatted with Citations) ───────────────────────────

NL_ANSWER_PROMPT = """You are a professional SAP Business Analyst. 
Answer the user's question based ONLY on the SQL query results below.

USER QUESTION: "{question}"

QUERY RESULTS:
{results_preview}

FORMATTING RULES (STRICT COMPLIANCE):
1. **Citations**: For every Document ID, Monetary Amount, or Status you mention, you MUST append its source in square brackets.
   Format: [TableName.ColumnName]
   Example: "Sales Order **740555** [sales_order_headers.salesOrder] for **€1,200** [sales_order_items.netValue] is blocked."

2. **Visual Style**:
   - Use **bold** for IDs, currencies, and dates.
   - Use bullet points (•) for lists of 3 or more items.
   - Start with a one-sentence summary of the finding.

3. **Insight**: If applicable, end with a "💡 Business Insight:" line.

4. **Tone**: Executive and direct. Do NOT mention technical terms like "SQL", "Table", or "Query".

Answer:"""

def generate_nl_answer(question: str, sql: str, columns: list[str], rows: list) -> str:
    if not rows:
        return "No matching records were found in the SAP system. Please verify if the IDs or criteria provided are correct."

    preview_rows = rows[:25]
    header = " | ".join(columns)
    body = "\n".join(" | ".join(str(v) for v in row) for row in preview_rows)
    
    # We include a snippet of the SQL in the preview so the LLM 
    # knows exactly which tables were involved to create citations.
    results_str = f"SOURCE SQL CONTEXT: {sql}\n\nDATA:\n{header}\n{body}"
    
    prompt = NL_ANSWER_PROMPT.format(
        question=question,
        results_preview=results_str
    )
    return _call_llm(prompt)

# ── Node ID Extraction ────────────────────────────────────────────────────────

def extract_node_ids(columns: list[str], rows: list) -> list[str]:
    PREFIX_MAP = {
        "salesorder": "SO", "salesOrder": "SO",
        "deliverydocument": "DEL", "deliveryDocument": "DEL",
        "billingdocument": "BILL", "billingDocument": "BILL",
        "accountingdocument": "PAY", "accountingDocument": "PAY",
        "businesspartner": "BP", "businessPartner": "BP",
        "customer": "BP", "product": "PROD", "material": "PROD", "plant": "PLANT",
    }
    node_ids = []
    col_lower = [c.lower() for c in columns]
    for i, col in enumerate(col_lower):
        prefix = PREFIX_MAP.get(col) or PREFIX_MAP.get(columns[i])
        if prefix:
            for row in rows[:50]:
                val = row[i]
                if val is not None and val != "":
                    val_clean = str(val).lstrip('0')
                    if val_clean:
                        node_ids.append(f"{prefix}:{val_clean}")
    return list(set(node_ids))

# ── Main Query Handler ─────────────────────────────────────────────────────────

def handle_query(question: str, conversation_history: list[dict] | None = None) -> dict[str, Any]:
    if not is_on_topic(question):
        return {"answer": OFF_TOPIC_RESPONSE, "sql": None,
                "columns": [], "rows": [], "node_ids": [], "error": None}

    sql = None
    columns = []
    rows = []
    error = None

    for attempt in range(MAX_SQL_RETRIES + 1):
        try:
            sql = generate_sql(question)
            columns, rows = execute_sql(sql)
            break
        except Exception as e:
            error = str(e)
            if attempt < MAX_SQL_RETRIES:
                question = f"{question}\n\n[Previous SQL execution failed with error: {error}. Review the DATABASE SCHEMA provided and fix the column/table names.]"
            else:
                return {"answer": f"I couldn't generate a valid query for that question. ({error})",
                        "sql": sql, "columns": [], "rows": [], "node_ids": [], "error": error}

    rows_plain = [list(row) for row in rows]
    answer = generate_nl_answer(question, sql, columns, rows_plain)
    node_ids = extract_node_ids(columns, rows_plain)

    return {"answer": answer, "sql": sql, "columns": columns,
            "rows": rows_plain[:100], "node_ids": node_ids, "error": None}