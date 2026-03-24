"""
Graph Builder with:
- Betweenness Centrality for intelligent node sizing
- Robust SAP date parsing (fixes epoch=0 / Jan 1970 bug)
- Temporal date extraction for time-travel slider
- Anomaly detection (broken O2C flows)
- Semantic search support
"""

import networkx as nx
from database import get_connection
from schema import get_table_columns

_GRAPH = None

NODE_TYPE_ORDER = {
    "customer": 0, "product": 1, "plant": 1,
    "sales_order": 2, "sales_order_item": 3,
    "delivery": 4, "billing_document": 5, "payment": 6, "journal_entry": 7,
}

COLOR_MAP = {
    "customer": "#10b981", "sales_order": "#6366f1", "sales_order_item": "#818cf8",
    "delivery": "#38bdf8", "billing_document": "#f97316", "payment": "#eab308",
    "product": "#ec4899", "plant": "#6b7280",
}

def _table_has_col(table, col):
    try:
        return col in get_table_columns(table)
    except Exception:
        return False

def _safe_fetch(conn, sql, params=()):
    try:
        return conn.execute(sql, params).fetchall()
    except Exception as e:
        print(f"  [graph warn] {e}")
        return []

def _col(table, *candidates):
    for c in candidates:
        if _table_has_col(table, c):
            return c
    return None

def _parse_date_to_epoch(date_str):
    """
    Robustly convert SAP date strings to Unix timestamp.
    Handles: ISO8601, /Date(ms)/, YYYYMMDD, DD.MM.YYYY, empty/null.
    Returns None if unparseable (never returns 0).
    """
    if not date_str:
        return None
    import re
    from datetime import datetime, timezone

    s = str(date_str).strip()
    if not s or s in ('null', 'None', '0', '00000000'):
        return None

    # SAP OData format: /Date(1234567890000)/  or /Date(1234567890000+0000)/
    m = re.match(r'/Date\((\d+)([+-]\d{4})?\)/', s)
    if m:
        ms = int(m.group(1))
        if ms > 0:
            return ms // 1000  # convert ms → seconds
        return None

    # ISO 8601 variants: 2025-04-02T00:00:00.000Z  /  2025-04-02
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s[:len(fmt)+4], fmt)
            ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
            return ts if ts > 0 else None
        except ValueError:
            continue

    # YYYYMMDD (SAP compact)
    if re.match(r'^\d{8}$', s):
        try:
            dt = datetime.strptime(s, "%Y%m%d")
            ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
            return ts if ts > 0 else None
        except ValueError:
            pass

    # DD.MM.YYYY (European)
    if re.match(r'^\d{2}\.\d{2}\.\d{4}$', s):
        try:
            dt = datetime.strptime(s, "%d.%m.%Y")
            ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
            return ts if ts > 0 else None
        except ValueError:
            pass

    # Regex fallback: extract YYYY-MM-DD from anywhere in string
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                          tzinfo=timezone.utc)
            ts = int(dt.timestamp())
            return ts if ts > 0 else None
        except (ValueError, OverflowError):
            pass

    return None


def build_graph(limit=600):
    global _GRAPH
    conn = get_connection()
    G = nx.DiGraph()

    def node(nid, ntype, **attrs):
        G.add_node(nid, node_type=ntype, label=nid, **attrs)

    def edge(src, tgt, rel, **attrs):
        if G.has_node(src) and G.has_node(tgt) and src != tgt:
            G.add_edge(src, tgt, relationship=rel, **attrs)

    # 1. Plants
    plant_id = _col("plants", "plant", "Plant")
    plant_nm = _col("plants", "plantName", "PlantName", "name")
    if plant_id:
        for r in _safe_fetch(conn, f'SELECT * FROM plants LIMIT {limit}'):
            pid = r[plant_id]
            if pid:
                node(f"PLANT:{pid}", "plant", name=r[plant_nm] if plant_nm else pid, plant=pid)

    # 2. Products
    prod_id = _col("products", "product", "Product", "material")
    if prod_id:
        rows = _safe_fetch(conn, f'''
            SELECT p.*, pd.productDescription FROM products p
            LEFT JOIN product_descriptions pd ON p."{prod_id}" = pd.product AND pd.language = "EN"
            LIMIT {limit}
        ''')
        if not rows:
            rows = _safe_fetch(conn, f'SELECT * FROM products LIMIT {limit}')
        for r in rows:
            pid = r[prod_id]
            if pid:
                node(f"PROD:{pid}", "product",
                     product=pid,
                     productType=r["productType"] if "productType" in r.keys() else "",
                     description=r["productDescription"] if "productDescription" in r.keys() else "")

    pp_prod = _col("product_plants", "product", "material")
    pp_plant = _col("product_plants", "plant", "Plant")
    if pp_prod and pp_plant:
        for r in _safe_fetch(conn, f'SELECT "{pp_prod}", "{pp_plant}" FROM product_plants LIMIT {limit}'):
            edge(f"PROD:{r[pp_prod]}", f"PLANT:{r[pp_plant]}", "STORED_AT")

    # 3. Customers
    bp_id = _col("business_partners", "businessPartner", "BusinessPartner")
    cust_c = _col("business_partners", "customer", "Customer")
    nm_c = _col("business_partners", "businessPartnerFullName", "fullName", "name")
    if bp_id:
        for r in _safe_fetch(conn, f'SELECT * FROM business_partners LIMIT {limit}'):
            bp = r[bp_id]
            if bp:
                node(f"BP:{bp}", "customer",
                     name=r[nm_c] if nm_c else bp,
                     customer=r[cust_c] if cust_c else "",
                     industry=r["industry"] if "industry" in r.keys() else "")

    # 4. Sales Orders
    so_col = _col("sales_order_headers", "salesOrder", "SalesOrder")
    party_c = _col("sales_order_headers", "soldToParty", "SoldToParty", "customer")
    amt_c = _col("sales_order_headers", "totalNetAmount", "netAmount")
    stat_c = _col("sales_order_headers", "overallDeliveryStatus", "deliveryStatus")
    date_c = _col("sales_order_headers", "creationDate", "orderDate")
    if so_col:
        for r in _safe_fetch(conn, f'SELECT * FROM sales_order_headers LIMIT {limit}'):
            so = r[so_col]
            if not so:
                continue
            date_val = r[date_c] if date_c else ""
            epoch = _parse_date_to_epoch(date_val)
            node(f"SO:{so}", "sales_order",
                 salesOrder=so,
                 amount=r[amt_c] if amt_c else "",
                 status=r[stat_c] if stat_c else "",
                 date=date_val,
                 epoch=epoch)
            if party_c and r[party_c]:
                bp_nid = _find_customer_node(G, r[party_c])
                if bp_nid:
                    edge(bp_nid, f"SO:{so}", "PLACED")

    # 5. Sales Order Items
    soi_so = _col("sales_order_items", "salesOrder", "SalesOrder")
    soi_item = _col("sales_order_items", "salesOrderItem", "item", "lineItem")
    soi_mat = _col("sales_order_items", "material", "Material", "product")
    soi_qty = _col("sales_order_items", "requestedQuantity", "quantity")
    if soi_so and soi_item:
        for r in _safe_fetch(conn, f'SELECT * FROM sales_order_items LIMIT {limit}'):
            so = r[soi_so]; item = r[soi_item]
            if not so or not item:
                continue
            nid = f"SOI:{so}-{item}"
            node(nid, "sales_order_item",
                 salesOrder=so, item=item,
                 material=r[soi_mat] if soi_mat else "",
                 quantity=r[soi_qty] if soi_qty else "")
            edge(f"SO:{so}", nid, "CONTAINS")
            if soi_mat and r[soi_mat]:
                edge(nid, f"PROD:{r[soi_mat]}", "REFERENCES")

    # 6. Deliveries
    del_col = _col("outbound_delivery_headers", "deliveryDocument", "DeliveryDocument")
    del_stat = _col("outbound_delivery_headers", "overallGoodsMovementStatus", "status")
    del_date = _col("outbound_delivery_headers", "actualGoodsMovementDate", "goodsMovementDate")
    if del_col:
        for r in _safe_fetch(conn, f'SELECT * FROM outbound_delivery_headers LIMIT {limit}'):
            did = r[del_col]
            if did:
                date_val = r[del_date] if del_date else ""
                epoch = _parse_date_to_epoch(date_val)
                node(f"DEL:{did}", "delivery",
                     deliveryDocument=did,
                     status=r[del_stat] if del_stat else "",
                     date=date_val,
                     epoch=epoch)

    di_del = _col("outbound_delivery_items", "deliveryDocument", "DeliveryDocument")
    di_so = _col("outbound_delivery_items", "salesOrder", "SalesOrder", "referenceDocument")
    if di_del and di_so:
        for r in _safe_fetch(conn, f'SELECT DISTINCT "{di_del}", "{di_so}" FROM outbound_delivery_items WHERE "{di_so}" != "" LIMIT {limit}'):
            edge(f"SO:{r[di_so]}", f"DEL:{r[di_del]}", "FULFILLED_BY")
    else:
        di_plant = _col("outbound_delivery_items", "plant", "Plant")
        di_sloc = _col("outbound_delivery_items", "storageLocation", "StorageLocation")
        si_plant = _col("sales_order_items", "plant", "Plant")
        si_sloc = _col("sales_order_items", "storageLocation", "StorageLocation")
        if all([di_plant, di_sloc, di_del, si_plant, si_sloc, soi_so]):
            rows = _safe_fetch(conn, f'''
                SELECT DISTINCT di."{di_del}", si."{soi_so}"
                FROM outbound_delivery_items di
                JOIN sales_order_items si ON di."{di_plant}"=si."{si_plant}" AND di."{di_sloc}"=si."{si_sloc}"
                LIMIT {limit}
            ''')
            for r in rows:
                edge(f"SO:{r[soi_so]}", f"DEL:{r[di_del]}", "FULFILLED_BY")

    # 7. Billing Documents
    bill_col = _col("billing_document_headers", "billingDocument", "BillingDocument")
    bill_so = _col("billing_document_headers", "salesOrder", "SalesOrder", "referenceDocument")
    bill_amt = _col("billing_document_headers", "totalNetAmount", "netAmount")
    bill_date = _col("billing_document_headers", "billingDocumentDate", "date")
    if bill_col:
        for r in _safe_fetch(conn, f'SELECT * FROM billing_document_headers LIMIT {limit}'):
            bid = r[bill_col]
            if not bid:
                continue
            date_val = r[bill_date] if bill_date else ""
            epoch = _parse_date_to_epoch(date_val)
            node(f"BILL:{bid}", "billing_document",
                 billingDocument=bid,
                 amount=r[bill_amt] if bill_amt else "",
                 date=date_val,
                 epoch=epoch)
            if bill_so and r[bill_so]:
                edge(f"SO:{r[bill_so]}", f"BILL:{bid}", "BILLED_AS")

    bi_bill = _col("billing_document_items", "billingDocument", "BillingDocument")
    bi_so = _col("billing_document_items", "salesOrder", "SalesOrder", "referenceDocument")
    if bi_bill and bi_so:
        for r in _safe_fetch(conn, f'SELECT DISTINCT "{bi_bill}", "{bi_so}" FROM billing_document_items WHERE "{bi_so}" != "" LIMIT {limit}'):
            edge(f"SO:{r[bi_so]}", f"BILL:{r[bi_bill]}", "BILLED_AS")

    # 8. Payments
    pay_doc = _col("payments_accounts_receivable", "accountingDocument", "AccountingDocument")
    pay_inv = _col("payments_accounts_receivable", "invoiceReference", "billingDocument", "invoice")
    pay_cust = _col("payments_accounts_receivable", "customer", "Customer")
    pay_amt = _col("payments_accounts_receivable", "amountInTransactionCurrency", "amount")
    pay_date = _col("payments_accounts_receivable", "clearingDate", "postingDate")
    if pay_doc:
        for r in _safe_fetch(conn, f'SELECT * FROM payments_accounts_receivable LIMIT {limit}'):
            doc = r[pay_doc]
            if not doc:
                continue
            date_val = r[pay_date] if pay_date else ""
            epoch = _parse_date_to_epoch(date_val)
            node(f"PAY:{doc}", "payment",
                 accountingDocument=doc,
                 amount=r[pay_amt] if pay_amt else "",
                 date=date_val,
                 epoch=epoch,
                 customer=r[pay_cust] if pay_cust else "")
            if pay_inv and r[pay_inv]:
                edge(f"BILL:{r[pay_inv]}", f"PAY:{doc}", "PAID_VIA")

    conn.close()

    # ── Betweenness Centrality ────────────────────────────────────────────────
    print("[graph] Computing betweenness centrality...")
    try:
        UG = G.to_undirected()
        centrality = nx.betweenness_centrality(UG, normalized=True, endpoints=False)
        max_c = max(centrality.values()) if centrality else 1
        for nid, c in centrality.items():
            if G.has_node(nid):
                G.nodes[nid]['centrality'] = round((c / max_c) * 100, 1)
    except Exception as e:
        print(f"  [warn] Centrality failed: {e}")
        for nid in G.nodes:
            G.nodes[nid]['centrality'] = 0

    # ── Assign flow_order; fill missing epochs with flow-based fallback ───────
    # Collect real epoch range per node_type to use for fallback interpolation
    type_epochs = {}
    for nid, data in G.nodes(data=True):
        ntype = data.get("node_type", "")
        ep = data.get("epoch")
        if ep and ep > 86400:  # valid non-zero epoch
            type_epochs.setdefault(ntype, []).append(ep)

    type_median = {}
    for ntype, eps in type_epochs.items():
        eps.sort()
        type_median[ntype] = eps[len(eps) // 2]

    for nid, data in G.nodes(data=True):
        ntype = data.get("node_type", "")
        data['flow_order'] = NODE_TYPE_ORDER.get(ntype, 5)
        ep = data.get("epoch")
        # Fix epoch=0 or None: use median of same node_type, or flow_order-based fallback
        if not ep or ep <= 0:
            if ntype in type_median:
                data['epoch'] = type_median[ntype]
            else:
                # Last resort: relative spacing based on flow order (not Jan 1970)
                fo = NODE_TYPE_ORDER.get(ntype, 5)
                # Use 2024-01-01 as base + fo * 30 days
                data['epoch'] = 1704067200 + fo * 2592000

    _GRAPH = G
    print(f"[graph] Built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G


def _find_customer_node(G, party):
    if not party:
        return None
    direct = f"BP:{party}"
    if G.has_node(direct):
        return direct
    for nid, data in G.nodes(data=True):
        if data.get("node_type") == "customer" and data.get("customer") == party:
            return nid
    return None


def get_graph():
    global _GRAPH
    if _GRAPH is None:
        _GRAPH = build_graph()
    return _GRAPH


def detect_anomalies():
    G = get_graph()
    anomalies = []
    so_nodes = [n for n, d in G.nodes(data=True) if d.get("node_type") == "sales_order"]
    del_nodes = set(n for n, d in G.nodes(data=True) if d.get("node_type") == "delivery")
    bill_nodes = set(n for n, d in G.nodes(data=True) if d.get("node_type") == "billing_document")
    pay_nodes = set(n for n, d in G.nodes(data=True) if d.get("node_type") == "payment")

    unbilled_orders, orphan_deliveries, orphan_billings, unpaid_billings = [], [], [], []

    for so in so_nodes:
        successors = set(G.successors(so))
        has_delivery = bool(successors & del_nodes)
        has_billing = bool(successors & bill_nodes)
        if has_delivery and not has_billing:
            unbilled_orders.append(so)

    for del_node in del_nodes:
        if not list(G.predecessors(del_node)):
            orphan_deliveries.append(del_node)

    for bill_node in bill_nodes:
        if not list(G.predecessors(bill_node)):
            orphan_billings.append(bill_node)
        if not set(G.successors(bill_node)) & pay_nodes:
            unpaid_billings.append(bill_node)

    if unbilled_orders:
        anomalies.append({"type": "unbilled_delivery", "label": "Delivered but not billed",
            "node_ids": unbilled_orders[:50], "color": "#f97316",
            "description": f"{len(unbilled_orders)} sales orders have deliveries but no billing document",
            "chat_query": "Show me all sales orders that have been delivered but not yet billed, with their order amounts and delivery dates"})
    if orphan_deliveries:
        anomalies.append({"type": "orphan_delivery", "label": "Delivery without Sales Order",
            "node_ids": orphan_deliveries[:50], "color": "#ef4444",
            "description": f"{len(orphan_deliveries)} deliveries have no linked sales order",
            "chat_query": "List all delivery documents that have no associated sales order"})
    if orphan_billings:
        anomalies.append({"type": "orphan_billing", "label": "Billing without Sales Order",
            "node_ids": orphan_billings[:50], "color": "#ef4444",
            "description": f"{len(orphan_billings)} billing documents have no linked sales order",
            "chat_query": "List all billing documents that have no associated sales order"})
    if unpaid_billings:
        anomalies.append({"type": "unpaid_billing", "label": "Billed but not paid",
            "node_ids": unpaid_billings[:50], "color": "#eab308",
            "description": f"{len(unpaid_billings)} billing documents have no payment recorded",
            "chat_query": "Show all billing documents that have not received any payment, with their amounts and dates"})
    return anomalies


def semantic_search(query: str, max_results: int = 50) -> list[str]:
    G = get_graph()
    q = query.lower().strip()
    matched = []
    SEMANTIC_MAP = {
        "late": lambda d: d.get("status") in ("", "A"),
        "delayed": lambda d: d.get("status") in ("", "A"),
        "complete": lambda d: d.get("status") == "C",
        "delivered": lambda d: d.get("node_type") == "delivery",
        "unpaid": lambda d: d.get("node_type") == "billing_document",
        "high value": lambda d: float(d.get("amount", 0) or 0) > 10000,
        "high-value": lambda d: float(d.get("amount", 0) or 0) > 10000,
        "large order": lambda d: float(d.get("amount", 0) or 0) > 5000,
        "customer": lambda d: d.get("node_type") == "customer",
        "product": lambda d: d.get("node_type") == "product",
        "payment": lambda d: d.get("node_type") == "payment",
        "bridge": lambda d: float(d.get("centrality", 0)) > 50,
        "hub": lambda d: float(d.get("centrality", 0)) > 30,
    }
    for kw, fn in SEMANTIC_MAP.items():
        if kw in q:
            for nid, data in G.nodes(data=True):
                try:
                    if fn(data):
                        matched.append(nid)
                except:
                    pass
            if matched:
                return list(set(matched))[:max_results]
    for nid, data in G.nodes(data=True):
        for v in data.values():
            if v and q in str(v).lower():
                matched.append(nid); break
    return list(set(matched))[:max_results]


def graph_to_cytoscape(G, max_nodes=400):
    nodes = list(G.nodes(data=True))[:max_nodes]
    node_ids = {n[0] for n in nodes}
    elements = []
    for nid, data in nodes:
        elements.append({"data": {
            "id": nid, "label": nid,
            "node_type": data.get("node_type", "unknown"),
            "color": COLOR_MAP.get(data.get("node_type", ""), "#888780"),
            "centrality": data.get("centrality", 0),
            "epoch": data.get("epoch") or 0,
            "flow_order": data.get("flow_order", 5),
            **{k: str(v) for k, v in data.items()
               if k not in ("label","node_type","centrality","epoch","flow_order") and v},
        }})
    for src, tgt, data in G.edges(data=True):
        if src in node_ids and tgt in node_ids:
            elements.append({"data": {
                "id": f"{src}__{tgt}", "source": src, "target": tgt,
                "relationship": data.get("relationship", ""),
            }})
    return {"elements": elements}


def get_node_neighbors(node_id, depth=1):
    G = get_graph()
    if node_id not in G:
        return {"elements": []}
    sub_nodes = {node_id}
    frontier = {node_id}
    for _ in range(depth):
        nxt = set()
        for n in frontier:
            nxt.update(G.predecessors(n))
            nxt.update(G.successors(n))
        sub_nodes.update(nxt)
        frontier = nxt
    return graph_to_cytoscape(G.subgraph(sub_nodes), max_nodes=150)