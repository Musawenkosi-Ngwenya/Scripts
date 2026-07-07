#!/usr/bin/env python3
"""
🧮 Generic Cross-System Data Comparator — RT / RI / FC

Compares data between any two of the three AWS-backed systems:
  RT  → "Retail Trade"  (TradePricelists-style tables, uses SupplierID)
  RI  → "RI SaaS"       (same struct family as RT, uses SupplierID)
  FC  → "Fast Catalogue" (jackhammer*-style tables, uses SellerID)

Supported data types (struct pairs), matching the Go model file provided:
  1. Pricelist            → PricelistInfo (RT/RI)         vs FCPricelistStruct (FC)
  2. Stock                → StockInfo (RT/RI)              vs FCStockStruct (FC)
  3. DiscountValues2/DealItems → DiscountValue2Info (RT/RI) vs DealItemsStruct (FC)
  4. Account               → AccountInfo (RT/RI)            vs FCAccountStruct (FC)

The script is fully interactive: it prompts for AWS profile, table name,
SupplierID/SellerID, GSI name, output CSV name, and data type. Everything
else (which fields form the match-key, which field holds the comparable
value, numeric vs string compare) is driven by the FIELD_MAP config below,
which mirrors the Go structs. Tweak FIELD_MAP if your schema differs.

Usage:
    python compare_data.py
"""

import os
import csv
import sys
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError, ProfileNotFound, NoCredentialsError

# ── Pretty logging ─────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("compare")


def say(msg: str) -> None:
    log.info(msg)


# ── System metadata ────────────────────────────────────────────────────────
# Guessed sane defaults — override at the prompt if yours differ.

SYSTEMS = {
    "RT": {"label": "🏬 RT  (Retail Trade)", "default_profile": "musa",       "id_field": "SupplierID", "schema": "trade"},
    "RI": {"label": "🧾 RI  (RI SaaS)",       "default_profile": "musarisaas", "id_field": "SupplierID", "schema": "trade"},
    "FC": {"label": "🛒 FC  (Fast Catalogue)", "default_profile": "musafc",     "id_field": "SellerID",   "schema": "fc"},
}

DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")

# ── Data-type field map ────────────────────────────────────────────────────
# key_fields   -> attributes that together form the match key (case/space
#                 normalised) between the two sides.
# value_field  -> attribute holding the value that gets compared.
# numeric      -> True to compare as Decimal with tolerance, False for exact
#                 (case-insensitive) string equality.

FIELD_MAP = {
    "Pricelist": {
        "trade": {"key_fields": ["ProductID", "PriceList"], "value_field": "Gross", "numeric": True},
        "fc":    {"key_fields": ["PartNumber", "Pricelist"], "value_field": "Price", "numeric": True},
    },
    "Stock": {
        "trade": {"key_fields": ["ProductID", "Warehouse"], "value_field": "Stock", "numeric": True},
        "fc":    {"key_fields": ["PartNumber", "Warehouse"], "value_field": "Stock", "numeric": True},
    },
    "DiscountValues2/DealItems": {
        # ⚠️ FC's DealItemsStruct only carries a SortKey (no bare ProductID) —
        # adjust key_fields below if your SortKey doesn't encode ProductID#DiscountID.
        "trade": {"key_fields": ["ProductID", "DiscountID"], "value_field": "Price", "numeric": True},
        "fc":    {"key_fields": ["SortKey"],                 "value_field": "Price", "numeric": True},
    },
    "Account": {
        "trade": {"key_fields": ["AccountID"], "value_field": "Pricelist", "numeric": False},
        "fc":    {"key_fields": ["AccountID"], "value_field": "Pricelist", "numeric": False},
    },
}

# Best-guess table / GSI suggestions shown as defaults (blank = user must type).
TABLE_DEFAULTS = {
    ("RT", "Pricelist"): "TradePricelists",
    ("FC", "Pricelist"): "jackhammerPricelist",
    ("RT", "Stock"): "TradeStock",
    ("FC", "Stock"): "jackhammerStock",
    ("RT", "DiscountValues2/DealItems"): "TradeDiscountValues2",
    ("FC", "DiscountValues2/DealItems"): "jackhammerDealItems",
    ("RT", "Account"): "TradeAccounts",
    ("FC", "Account"): "jackhammerAccounts",
}
GSI_DEFAULTS = {
    ("FC", "Pricelist"): "SellerIDPartNumberIndex",
}

TOLERANCE = Decimal("0.0001")


# ── Small interactive helpers ──────────────────────────────────────────────

def ask(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    val = input(f"➡️  {prompt}{suffix}: ").strip()
    return val or default


def ask_choice(prompt: str, options: list) -> str:
    print(f"\n❓ {prompt}")
    for i, opt in enumerate(options, 1):
        print(f"   {i}. {opt}")
    while True:
        raw = input(f"   Choose [1-{len(options)}] (default 1): ").strip()
        if not raw:
            return options[0]
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1]
        print("   ⚠️  Invalid choice, try again.")


# ── Side configuration ──────────────────────────────────────────────────────

class SideConfig:
    def __init__(self, label, system, profile, table, id_field, id_value, gsi, schema):
        self.label = label
        self.system = system
        self.profile = profile
        self.table = table
        self.id_field = id_field
        self.id_value = id_value
        self.gsi = gsi or None
        self.schema = schema

    def __repr__(self):
        return f"{self.system}:{self.table}"


def collect_side_config(label: str, data_type: str, taken_system: str = None) -> SideConfig:
    options = [s for s in SYSTEMS if s != taken_system] if taken_system else list(SYSTEMS)
    system = ask_choice(f"{label} — which system?", [SYSTEMS[s]["label"] for s in options])
    system_key = [s for s in options if SYSTEMS[s]["label"] == system][0]
    meta = SYSTEMS[system_key]

    profile = ask(
        f"{label} ({system_key}) AWS profile",
        os.environ.get(f"{system_key}_AWS_PROFILE", meta["default_profile"]),
    )
    table = ask(f"{label} ({system_key}) table name", TABLE_DEFAULTS.get((system_key, data_type), ""))
    id_value = ask(f"{label} ({system_key}) {meta['id_field']} value")
    gsi = ask(
        f"{label} ({system_key}) GSI name (blank = query base table on partition key)",
        GSI_DEFAULTS.get((system_key, data_type), ""),
    )

    return SideConfig(label, system_key, profile, table, meta["id_field"], id_value, gsi, meta["schema"])


# ── DynamoDB fetch ──────────────────────────────────────────────────────────

def get_dynamodb_resource(profile: str, region: str):
    session = boto3.Session(profile_name=profile, region_name=region)
    return session.resource("dynamodb")


def fetch_items(cfg: SideConfig, region: str) -> list:
    via = f"GSI '{cfg.gsi}'" if cfg.gsi else "partition key"
    say(f"🔎 [{cfg.system}] querying '{cfg.table}' ({via}) where {cfg.id_field} = {cfg.id_value} …")
    try:
        ddb = get_dynamodb_resource(cfg.profile, region)
        table = ddb.Table(cfg.table)
        kwargs = {"KeyConditionExpression": Key(cfg.id_field).eq(cfg.id_value)}
        if cfg.gsi:
            kwargs["IndexName"] = cfg.gsi

        items = []
        while True:
            resp = table.query(**kwargs)
            items.extend(resp.get("Items", []))
            last = resp.get("LastEvaluatedKey")
            if not last:
                break
            kwargs["ExclusiveStartKey"] = last

        say(f"✅ [{cfg.system}] fetched {len(items)} items from '{cfg.table}'")
        return items

    except ProfileNotFound:
        say(f"❌ [{cfg.system}] AWS profile '{cfg.profile}' not found — check ~/.aws/config")
        raise
    except NoCredentialsError:
        say(f"❌ [{cfg.system}] no AWS credentials found for profile '{cfg.profile}'")
        raise
    except ClientError as e:
        say(f"❌ [{cfg.system}] AWS error: {e.response['Error']['Message']}")
        raise


# ── Dataset building & comparison ──────────────────────────────────────────

def normalize_key(item: dict, key_fields: list):
    parts = []
    for kf in key_fields:
        v = item.get(kf)
        if v is None or str(v).strip() == "":
            return None
        parts.append(str(v).strip().upper())
    return tuple(parts)


def build_dataset(items: list, key_fields: list, value_field: str, numeric: bool) -> dict:
    dataset = {}
    skipped = 0
    for item in items:
        key = normalize_key(item, key_fields)
        if key is None:
            skipped += 1
            continue
        raw_val = item.get(value_field)
        if raw_val is None:
            skipped += 1
            continue
        try:
            val = Decimal(str(raw_val)) if numeric else str(raw_val).strip()
        except InvalidOperation:
            skipped += 1
            continue
        dataset[key] = (val, item)
    if skipped:
        say(f"   ⚠️  skipped {skipped} item(s) missing key/value fields")
    return dataset


def compare_datasets(a: dict, b: dict, a_cfg: SideConfig, b_cfg: SideConfig, numeric: bool, key_fields: list):
    common = set(a) & set(b)
    only_a = set(a) - set(b)
    only_b = set(b) - set(a)

    say(f"📊 Matched: {len(common)} | Only in {a_cfg.system}: {len(only_a)} | Only in {b_cfg.system}: {len(only_b)}")

    mismatches = []
    for key in common:
        a_val, _ = a[key]
        b_val, _ = b[key]
        is_mismatch = (abs(a_val - b_val) > TOLERANCE) if numeric else (a_val.upper() != b_val.upper())
        if is_mismatch:
            row = {f"Key_{f}": key[i] for i, f in enumerate(key_fields)}
            row[f"{a_cfg.system}_Value"] = str(a_val)
            row[f"{b_cfg.system}_Value"] = str(b_val)
            if numeric:
                row["Difference"] = float(a_val - b_val)
            mismatches.append(row)

    say(f"🚨 Mismatches found: {len(mismatches)}")
    return mismatches, only_a, only_b


def write_csv(mismatches: list, key_fields: list, a_system: str, b_system: str, path: str):
    if not mismatches:
        say("📭 No mismatches — CSV not written.")
        return
    fieldnames = [f"Key_{f}" for f in key_fields] + [f"{a_system}_Value", f"{b_system}_Value", "Difference"]
    fieldnames = [f for f in fieldnames if f in mismatches[0] or f.startswith("Key_") or "Value" in f]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[fn for fn in fieldnames if fn in mismatches[0]] or list(mismatches[0].keys()))
        writer.writeheader()
        writer.writerows(mismatches)
    say(f"💾 Saved {len(mismatches)} mismatch(es) → {path}")


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("🧮  Cross-System Data Comparator — RT / RI / FC")
    print("=" * 70)

    start = datetime.now()

    # 6. Type of data
    data_type = ask_choice("Which data type are you comparing?", list(FIELD_MAP.keys()))

    # 1-4 for side A
    side_a = collect_side_config("Side A", data_type)
    # 1-4 for side B (must be a different system)
    side_b = collect_side_config("Side B", data_type, taken_system=side_a.system)

    # 5. Output CSV name
    default_csv = f"{data_type.replace('/', '_')}_mismatches_{side_a.system}_vs_{side_b.system}.csv"
    output_csv = ask("Output CSV filename", default_csv)

    region = ask("AWS region", DEFAULT_REGION)

    field_cfg_a = FIELD_MAP[data_type][side_a.schema]
    field_cfg_b = FIELD_MAP[data_type][side_b.schema]

    print()
    say(f"🚀 Starting comparison: {side_a} ⚔️ {side_b}  |  data type: {data_type}")

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_a = pool.submit(fetch_items, side_a, region)
            fut_b = pool.submit(fetch_items, side_b, region)
            items_a = fut_a.result()
            items_b = fut_b.result()
    except Exception:
        say("💥 Aborting due to fetch error above.")
        sys.exit(1)

    say(f"🧱 Building datasets ({data_type}) …")
    dataset_a = build_dataset(items_a, field_cfg_a["key_fields"], field_cfg_a["value_field"], field_cfg_a["numeric"])
    dataset_b = build_dataset(items_b, field_cfg_b["key_fields"], field_cfg_b["value_field"], field_cfg_b["numeric"])

    # key_fields for the CSV are taken from side A's naming (they're positionally aligned)
    mismatches, only_a, only_b = compare_datasets(
        dataset_a, dataset_b, side_a, side_b,
        numeric=field_cfg_a["numeric"], key_fields=field_cfg_a["key_fields"],
    )

    write_csv(mismatches, field_cfg_a["key_fields"], side_a.system, side_b.system, output_csv)

    elapsed = (datetime.now() - start).total_seconds()
    print()
    say(f"🏁 Done in {elapsed:.1f}s — {len(mismatches)} mismatch(es), "
        f"{len(only_a)} only in {side_a.system}, {len(only_b)} only in {side_b.system}")
    print("=" * 70)


if __name__ == "__main__":
    main()