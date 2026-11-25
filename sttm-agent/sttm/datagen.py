"""Synthetic raw extracts for an online retailer, landed as files in a UC volume.

Three sources, each with the mess real extracts have:
  customers/  CRM export, JSON lines. Prefixed string ids, mixed-case emails and country codes, one-letter
              loyalty codes, and the same customer re-exported when it changes (needs dedup / upsert).
  orders/     Web-shop order events, JSON lines. Everything is a string, epoch-millis timestamps, several
              currencies, and one event per status change for the same order_id (needs dedup).
  products/   Product catalogue, CSV. Prices as strings, re-sent rows when a price changes.

Batches are deterministic (seeded), so a re-run lands identical files. Batch 2 adds new customers and orders,
updates some customers, moves some orders to SHIPPED/RETURNED, and re-prices some products.
"""

from __future__ import annotations

import csv
import io
import json
import random
from datetime import datetime, timedelta, timezone

FIRST = ["anna", "ben", "carla", "dmitri", "eva", "farid", "grace", "hugo", "ines", "jonas", "kai", "lena",
         "marco", "nora", "oscar", "paula", "quinn", "rosa", "sven", "tara", "umar", "vera", "wim", "yara"]
LAST = ["smith", "garcia", "müller", "rossi", "dubois", "novak", "jensen", "silva", "kowalski", "okafor",
        "haddad", "tanaka", "berg", "lopez", "fischer", "moreau"]
COUNTRIES = ["de", "FR", "es", "IT", "nl", "GB", "us", "PL"]
CURRENCY = {"de": "EUR", "FR": "EUR", "es": "EUR", "IT": "EUR", "nl": "EUR", "GB": "GBP", "us": "USD", "PL": "EUR"}
TIERS = ["G", "S", "B", None, None]
CATEGORIES = {"electronics": (40, 900), "home": (8, 250), "outdoor": (15, 400), "books": (5, 45),
              "toys": (6, 120)}
EPOCH = datetime(2026, 9, 1, tzinfo=timezone.utc)


def _customer(rng: random.Random, cid: int, ts: datetime) -> dict:
    first, last = rng.choice(FIRST), rng.choice(LAST)
    country = rng.choice(COUNTRIES)
    return {
        "cust_id": f"C{cid:05d}",
        "first_nm": f" {first.title() if rng.random() < .7 else first} ",
        "last_nm": last if rng.random() < .5 else last.upper(),
        "email_addr": f"{first}.{last}{cid}@Example.COM" if rng.random() < .5 else f"{first}.{last}{cid}@example.com",
        "country_cd": country,
        "signup_dt": (EPOCH - timedelta(days=rng.randint(30, 900))).strftime("%Y-%m-%d"),
        "loyalty_tier": rng.choice(TIERS),
        "updated_at": ts.isoformat(timespec="seconds"),
    }


def _products(rng: random.Random, n: int, ts: datetime) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        cat = list(CATEGORIES)[i % len(CATEGORIES)]
        lo, hi = CATEGORIES[cat]
        price = round(rng.uniform(lo, hi), 2)
        rows.append({"sku": f"sk-{i:04d}" if i % 3 else f"SK-{i:04d}", "product_name": f"{cat} item {i}",
                     "category": cat.upper() if i % 4 == 0 else cat, "list_price": f"{price:.2f}",
                     "cost_price": f"{price * rng.uniform(.45, .8):.2f}", "updated_at": ts.isoformat(timespec="seconds")})
    return rows


def _order_events(rng: random.Random, first_id: int, n: int, customers: list[dict], products: list[dict],
                  start: datetime, days: int) -> list[dict]:
    events = []
    for oid in range(first_id, first_id + n):
        c = rng.choice(customers)
        p = rng.choice(products)
        placed = start + timedelta(minutes=rng.randint(0, days * 24 * 60))
        price = float(p["list_price"]) * {"EUR": 1, "GBP": .86, "USD": 1.08}[CURRENCY[c["country_cd"]]]
        base = {"order_id": str(oid), "cust_id": c["cust_id"], "sku": p["sku"].lower() if rng.random() < .3 else p["sku"],
                "qty": str(rng.choice([1, 1, 1, 2, 2, 3, 5])), "unit_price": f"{price:.2f}",
                "discount_amt": f"{price * .1:.2f}" if rng.random() < .25 else None,
                "currency": CURRENCY[c["country_cd"]].lower() if rng.random() < .2 else CURRENCY[c["country_cd"]],
                "order_ts": str(int(placed.timestamp() * 1000))}
        events.append({**base, "status": "PLACED", "event_ts": base["order_ts"]})
        if rng.random() < .6:   # later status change for the same order -> duplicate order_id in raw
            later = placed + timedelta(hours=rng.randint(6, 72))
            events.append({**base, "status": rng.choice(["SHIPPED", "SHIPPED", "SHIPPED", "CANCELLED"]),
                           "event_ts": str(int(later.timestamp() * 1000))})
    return events


def batch(n: int) -> dict[str, tuple[str, str]]:
    """Files for batch n: {relative path: (content, description)}."""
    rng = random.Random(1000 + n)
    t1 = EPOCH + timedelta(days=60)
    base_customers = [_customer(random.Random(cid), cid, t1) for cid in range(1, 201)]
    base_products = _products(random.Random(7), 40, t1)
    files: dict[str, tuple[str, str]] = {}

    if n == 1:
        customers = base_customers
        products = base_products
        orders = _order_events(rng, 100001, 1200, customers, products, EPOCH, 60)
    elif n == 2:
        t2 = t1 + timedelta(days=14)
        new = [_customer(random.Random(cid), cid, t2) for cid in range(201, 231)]
        changed = []
        for c in rng.sample(base_customers, 25):        # re-exported with a new email / tier
            c2 = dict(c, email_addr=c["email_addr"].replace("@", ".new@"), loyalty_tier=rng.choice(["G", "S"]),
                      updated_at=t2.isoformat(timespec="seconds"))
            changed.append(c2)
        customers = new + changed
        products = [dict(p, list_price=f"{float(p['list_price']) * 1.05:.2f}", updated_at=t2.isoformat(timespec="seconds"))
                    for p in rng.sample(base_products, 6)]
        orders = _order_events(rng, 101201, 300, base_customers + new, base_products, t1, 14)
        # returns for some batch-1 orders: same order_id, newer event
        for e in _order_events(random.Random(1001), 100001, 1200, base_customers, base_products, EPOCH, 60)[::40]:
            if e["status"] == "PLACED":
                orders.append({**e, "status": "RETURNED", "event_ts": str(int(t2.timestamp() * 1000))})
    else:
        raise ValueError("batches 1 and 2 exist")

    files[f"customers/crm_export_{n:03d}.json"] = ("\n".join(json.dumps(c) for c in customers), f"{len(customers)} customers")
    files[f"orders/web_orders_{n:03d}.json"] = ("\n".join(json.dumps(o) for o in orders), f"{len(orders)} order events")
    buf = io.StringIO()
    wr = csv.DictWriter(buf, fieldnames=list(products[0]))
    wr.writeheader()
    wr.writerows(products)
    files[f"products/catalogue_{n:03d}.csv"] = (buf.getvalue(), f"{len(products)} products")
    return files
