"""Siigo API integration — products & weekly sales."""

import os
import requests
from datetime import datetime, timedelta
from functools import lru_cache

SIIGO_AUTH_URL = "https://api.siigo.com/auth"
SIIGO_BASE = "https://api.siigo.com/v1"
PARTNER_ID = "dailySistemaGestion"

_token_cache = {"token": None, "expires": 0}


def _get_credentials():
    user = os.environ.get("SIIGO_USERNAME", "")
    key = os.environ.get("SIIGO_ACCESS_KEY", "")
    if not user or not key:
        raise RuntimeError("SIIGO_USERNAME y SIIGO_ACCESS_KEY no configurados")
    return user, key


def get_token() -> str:
    """Authenticate and cache token (valid 24h, we refresh every 23h)."""
    import time
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires"]:
        return _token_cache["token"]

    user, key = _get_credentials()
    resp = requests.post(SIIGO_AUTH_URL, json={
        "username": user,
        "access_key": key
    }, timeout=15)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    _token_cache["token"] = token
    _token_cache["expires"] = now + 23 * 3600  # 23h
    return token


def _headers():
    return {
        "Authorization": f"Bearer {get_token()}",
        "Partner-Id": PARTNER_ID,
        "Content-Type": "application/json"
    }


def _paginate(endpoint: str, params: dict = None, max_pages: int = 50) -> list:
    """Fetch all pages from a Siigo endpoint."""
    if params is None:
        params = {}
    params.setdefault("page_size", 100)
    params["page"] = 1
    all_results = []

    for _ in range(max_pages):
        resp = requests.get(f"{SIIGO_BASE}{endpoint}", headers=_headers(),
                            params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        all_results.extend(results)
        pagination = data.get("pagination", {})
        total = pagination.get("total_results", 0)
        if len(all_results) >= total:
            break
        params["page"] += 1

    return all_results


# ─── PUBLIC API ──────────────────────────────────────────────────

# Generic/system products to always skip
_SKIP_CODES = {"productogenericonube", "1", "2", "RegistroManual"}
# Groups that are raw materials, not finished products
_MP_GROUPS = {"Materia Prima", "Ingredientes"}
_EXCLUDED_GROUPS = {"Servicios", "Productos"}


def fetch_products(tipo: str = "terminado") -> list:
    """
    Get products from Siigo.
    tipo='terminado' -> only finished products (Proteinas, Sopas, Salsas, etc.)
    tipo='mp' -> only MP + Ingredientes
    tipo='todos' -> everything
    """
    raw = _paginate("/products")
    result = []
    for p in raw:
        code = p.get("code", "")
        if code in _SKIP_CODES:
            continue
        grp = p.get("account_group", {}).get("name", "")
        if grp in _EXCLUDED_GROUPS:
            continue
        is_mp = grp in _MP_GROUPS
        if tipo == "terminado" and is_mp:
            continue
        if tipo == "mp" and not is_mp:
            continue
        # Extract price from price list
        precio_venta = 0
        prices = p.get("prices", [])
        if prices:
            pl = prices[0].get("price_list", [])
            if pl:
                precio_venta = pl[0].get("value", 0)

        result.append({
            "id": p["id"],
            "code": code,
            "name": p.get("name", ""),
            "group": grp,
            "type": p.get("type", ""),
            "unit": p.get("unit_label") or p.get("unit", {}).get("name", ""),
            "active": p.get("active", True),
            "stock_control": p.get("stock_control", False),
            "available_quantity": p.get("available_quantity", 0),
            "tax_classification": p.get("tax_classification", ""),
            "precio_venta": precio_venta,
        })
    return result


def fetch_invoices(date_start: str, date_end: str) -> list:
    """Get all invoices in a date range. Dates: YYYY-MM-DD."""
    raw = _paginate("/invoices", {
        "date_start": date_start,
        "date_end": date_end
    })
    return raw


def fetch_purchases(date_start: str = None, date_end: str = None) -> list:
    """Get all purchases, optionally filtered by date (filtered in Python since API ignores date params)."""
    raw = _paginate("/purchases")
    if date_start or date_end:
        filtered = []
        for p in raw:
            fecha = p.get('date', '')[:10]
            if date_start and fecha < date_start:
                continue
            if date_end and fecha > date_end:
                continue
            filtered.append(p)
        return filtered
    return raw


def sales_by_product_weekly(weeks: int = 8) -> dict:
    """
    Returns sales aggregated by product, broken down by week.
    Output: {
      "weeks": ["2026-W13", "2026-W14", ...],
      "products": [
        {"code": "PR001", "name": "Carne desmechada", "group": "...",
         "weekly": [{"week": "2026-W13", "qty": 10, "revenue": 150000}, ...],
         "total_qty": 80, "total_revenue": 1200000}
      ]
    }
    """
    today = datetime.now().date()
    start = today - timedelta(weeks=weeks)
    invoices = fetch_invoices(start.isoformat(), today.isoformat())

    # Build week labels
    week_set = set()
    product_data = {}  # code -> {name, group, weeks: {week_label: {qty, revenue}}}

    for inv in invoices:
        if inv.get("annulled"):
            continue
        inv_date = inv.get("date", "")
        if not inv_date:
            continue
        try:
            dt = datetime.strptime(inv_date[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        iso = dt.isocalendar()
        week_label = f"{iso[0]}-W{iso[1]:02d}"
        week_set.add(week_label)

        for item in inv.get("items", []):
            code = item.get("code", "NOCODE")
            if code in _SKIP_CODES:
                continue
            # Only count finished product sales (not MP/ING/SV)
            if code.startswith("MP") or code.startswith("ING") or code.startswith("SV"):
                continue
            name = item.get("description", code)
            qty = float(item.get("quantity", 0))
            revenue = float(item.get("total", 0))

            if code not in product_data:
                product_data[code] = {
                    "code": code, "name": name, "group": "",
                    "weeks": {}, "total_qty": 0, "total_revenue": 0
                }
            pd = product_data[code]
            if week_label not in pd["weeks"]:
                pd["weeks"][week_label] = {"qty": 0, "revenue": 0}
            pd["weeks"][week_label]["qty"] += qty
            pd["weeks"][week_label]["revenue"] += revenue
            pd["total_qty"] += qty
            pd["total_revenue"] += revenue

    weeks_sorted = sorted(week_set)

    # Build product list with weekly arrays
    products = []
    for code, pd in sorted(product_data.items(), key=lambda x: -x[1]["total_revenue"]):
        weekly = []
        for w in weeks_sorted:
            wd = pd["weeks"].get(w, {"qty": 0, "revenue": 0})
            weekly.append({"week": w, "qty": wd["qty"], "revenue": round(wd["revenue"], 2)})
        products.append({
            "code": pd["code"],
            "name": pd["name"],
            "group": pd["group"],
            "weekly": weekly,
            "total_qty": pd["total_qty"],
            "total_revenue": round(pd["total_revenue"], 2)
        })

    return {"weeks": weeks_sorted, "products": products}
