#!/usr/bin/env python3
"""
sync_all_to_fc.py

Syncs Pricelists, Accounts, DiscountValues, and Stock from DynamoDB
(TradePricelists, TargetsAccounts2, TradeDiscountValues2, TradeStock)
to the RapidTrade QA REST API.

Order: Pricelists -> Accounts -> DiscountValues -> Stock

PERFORMANCE NOTES (why this version is faster for ~120k+ records):
  - Fetch and upload are now PIPELINED. Instead of "scan the whole table,
    THEN chunk, THEN upload", batches are uploaded as soon as they're built,
    while the next DynamoDB page is still being fetched. This overlaps
    query latency with network upload latency instead of paying both costs
    back-to-back.
  - A single `requests.Session()` with a pooled HTTPAdapter is reused across
    all uploads (per entity), instead of opening a fresh connection per
    `requests.post()` call. This avoids repeated TCP/TLS handshake overhead.
  - UPLOAD_WORKERS is bumped and the connection pool is sized to match it
    (pool_maxsize=UPLOAD_WORKERS), since uploads are I/O-bound, not CPU-bound.
  - With --auto, all 4 entities run concurrently (they're independent tables
    and independent endpoints, so there's no reason to serialize them). The
    interactive review flow forces entities to run one-at-a-time instead,
    since asking for confirmation on 4 interleaved entities would be a mess.
  - Optional gzip request body (--gzip) to cut upload time on the wire, if
    the QA endpoint honors Content-Encoding: gzip. If it doesn't, turn it
    back off -- some API gateways reject unrecognized encodings.

ENTITY SELECTION:
  - By default (no --entity / --entities / --all flag) the script now asks
    interactively whether to sync ALL four entities or let you PICK which
    ones, so you don't have to remember flag syntax for a one-off partial run.
  - --entity <key>        run exactly one entity, no prompt
  - --entities a,b,c      run a specific comma-separated list, no prompt
  - --all                 run all four, no prompt
  - Any of the above skips the interactive picker entirely (useful for cron
    / --auto runs where nothing should block on input()).

Usage:
    python sync_all_to_fc.py                     # interactive: asks all vs pick, then runs
    python sync_all_to_fc.py --entity pricelist   # just one entity, no prompt
    python sync_all_to_fc.py --entities pricelist,stock   # a couple entities, no prompt
    python sync_all_to_fc.py --all                # all four, no prompt
    python sync_all_to_fc.py --dry-run            # scan + chunk only, no network calls
    python sync_all_to_fc.py --auto               # skip confirmation prompts, run concurrently
    python sync_all_to_fc.py --auto --all --workers 20  # tune concurrency, fully non-interactive
    python sync_all_to_fc.py --auto --all --gzip  # compress upload bodies

Requires: boto3, requests
    pip install boto3 requests --break-system-packages
"""

import argparse
import gzip
import io
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import boto3
import requests
import threading
from boto3.dynamodb.conditions import Key
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth

# ---------------------------------------------------------------------------
# CONFIG — adjust here
# ---------------------------------------------------------------------------

DEFAULT_REGION = "us-east-1"

AWS_PROFILE = None
SUPPLIER_ID = None
API_BASE = None
API_USERNAME = None
API_PASSWORD = None

# If the real auth is different (e.g. token in JSON body instead of Basic Auth),
# change AUTH_MODE to "payload" and fill in how the endpoint expects it.
AUTH_MODE = "basic"  # "basic" | "payload" | "none"

SCAN_SEGMENTS = 8          # parallel DynamoDB scan workers, per table (only used if SUPPLIER_ID is None)
UPLOAD_BATCH_SIZE = 500    # records per POST body
UPLOAD_WORKERS = 12        # concurrent upload threads per entity (bumped from 6; tune with --workers)
REQUEST_TIMEOUT = 60       # seconds
MAX_RETRIES = 3
RETRY_BACKOFF = 2          # seconds, doubles each retry

# Entities in the order you asked for. Add "region" per-entry if a table
# lives outside DEFAULT_REGION (you mentioned af-south-1 is also in play).
ENTITIES = [
    {
        "key": "pricelist",
        "name": "Pricelists",
        "table": "TradePricelists",
        "endpoint": f"{API_BASE}/pricelist",
    },
    {
        "key": "accounts",
        "name": "Accounts",
        "table": "TargetsAccounts2",
        "endpoint": f"{API_BASE}/account",
    },
    {
        "key": "discountvalues2",
        "name": "DiscountValues",
        "table": "TradeDiscountValues2",
        "endpoint": f"{API_BASE}/discountvalues2",
    },
    {
        "key": "stock",
        "name": "Stock",
        "table": "TradeStock",
        "endpoint": f"{API_BASE}/stock",
    },
]

# ---------------------------------------------------------------------------
# Logging — console + file, so you have a record of what actually went out
# ---------------------------------------------------------------------------

LOG_FILE = "sync_all_to_fc.log"

logger = logging.getLogger("sync_all_to_fc")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(_fmt)
logger.addHandler(_console)

_file = logging.FileHandler(LOG_FILE)
_file.setFormatter(_fmt)
logger.addHandler(_file)

_log_lock = threading.Lock()


def log_info(msg):
    with _log_lock:
        logger.info(msg)


def log_warning(msg):
    with _log_lock:
        logger.warning(msg)


def log_error(msg):
    with _log_lock:
        logger.error(msg)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean_value(value):
    """Recursively convert DynamoDB Decimal types into plain int/float for JSON."""
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    if isinstance(value, dict):
        return {k: clean_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean_value(v) for v in value]
    if isinstance(value, set):
        return [clean_value(v) for v in value]
    return value


def clean_item(item):
    return {k: clean_value(v) for k, v in item.items()}


def chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def build_session(pool_size):
    """One Session per entity, reused across every upload for that entity.
    Reusing connections avoids a fresh TCP/TLS handshake per POST, which
    matters a lot once you're firing hundreds of batches."""
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class Heartbeat:
    """Pings the log every `interval` seconds while a blocking call is in
    flight, so a slow/throttled single page doesn't look like a hang."""

    def __init__(self, label, interval=5):
        self.label = label
        self.interval = interval
        self._stop = threading.Event()
        self._thread = None
        self._t0 = None

    def _run(self):
        while not self._stop.wait(self.interval):
            elapsed = time.time() - self._t0
            log_info(f"    ...{self.label} still in progress ({elapsed:.0f}s elapsed)")

    def __enter__(self):
        self._t0 = time.time()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=1)


def get_auth():
    if AUTH_MODE == "basic":
        return HTTPBasicAuth(API_USERNAME, API_PASSWORD)
    return None


def inject_payload_auth(batch):
    """Only used if AUTH_MODE == 'payload' — adjust shape to match the real API."""
    return {
        "Username": API_USERNAME,
        "Password": API_PASSWORD,
        "Data": batch,
    }


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def upload_batch(session, endpoint, batch, batch_num, total_batches, use_gzip=False):
    body = inject_payload_auth(batch) if AUTH_MODE == "payload" else batch
    auth = get_auth()

    headers = {"Content-Type": "application/json"}
    if use_gzip:
        raw = json.dumps(body).encode("utf-8")
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(raw)
        data = buf.getvalue()
        headers["Content-Encoding"] = "gzip"
    else:
        data = json.dumps(body).encode("utf-8")

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(
                endpoint,
                data=data,
                auth=auth,
                timeout=REQUEST_TIMEOUT,
                headers=headers,
            )
            if resp.status_code in (200, 201, 202):
                log_info(
                    f"  batch {batch_num}/{total_batches} -> {resp.status_code} "
                    f"({len(batch)} records)"
                )
                return True, resp
            else:
                log_warning(
                    f"  batch {batch_num}/{total_batches} attempt {attempt} -> "
                    f"HTTP {resp.status_code}: {resp.text[:300]}"
                )
                last_error = f"HTTP {resp.status_code}: {resp.text[:300]}"
        except requests.RequestException as e:
            log_warning(f"  batch {batch_num}/{total_batches} attempt {attempt} -> {e}")
            last_error = str(e)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF * attempt)

    log_error(f"  batch {batch_num}/{total_batches} FAILED after {MAX_RETRIES} attempts: {last_error}")
    return False, None


# ---------------------------------------------------------------------------
# DynamoDB — supplier-scoped Query (fast path) or full parallel Scan (fallback)
# ---------------------------------------------------------------------------

def scan_segment(table, segment, total_segments):
    items = []
    scan_kwargs = {"Segment": segment, "TotalSegments": total_segments}
    page = 0
    while True:
        with Heartbeat(f"segment {segment} page {page + 1} scan"):
            resp = table.scan(**scan_kwargs)
        page += 1
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key
    return items


def iter_query_pages_by_supplier(table, supplier_id):
    """Generator version of the old query_by_supplier — yields each page's
    items as soon as they arrive, instead of building one big list and
    returning only at the very end. This is what lets uploads start before
    the fetch is fully done."""
    page = 0
    t0 = time.time()
    query_kwargs = {"KeyConditionExpression": Key("SupplierID").eq(supplier_id)}
    total = 0
    while True:
        with Heartbeat(f"page {page + 1} query"):
            resp = table.query(**query_kwargs)
        page += 1
        page_items = resp.get("Items", [])
        total += len(page_items)
        elapsed = time.time() - t0
        log_info(
            f"    page {page}: +{len(page_items)} records "
            f"(running total {total}, {elapsed:.1f}s elapsed)"
        )
        yield page_items
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        query_kwargs["ExclusiveStartKey"] = last_key


def iter_full_scan(table, segments=SCAN_SEGMENTS):
    """Full-table scan fallback (SUPPLIER_ID = None). Unlike the query path,
    all segments are gathered before yielding, since they arrive out of
    order across threads. Still much faster than a single-threaded scan."""
    with ThreadPoolExecutor(max_workers=segments) as executor:
        futures = [executor.submit(scan_segment, table, seg, segments) for seg in range(segments)]
        for future in as_completed(futures):
            yield future.result()


# ---------------------------------------------------------------------------
# Shared confirmation state — lets the "review first batch" pause happen
# only ONCE across the whole run (not once per entity). Once the user types
# "y", every subsequent entity (and the rest of the current one) proceeds
# with no further pauses.
# ---------------------------------------------------------------------------

class ConfirmState:
    def __init__(self, already_confirmed=False):
        self.confirmed = already_confirmed
        self.aborted = False
        self.lock = threading.Lock()


# ---------------------------------------------------------------------------
# Entity selection — decide which of the 4 entities to run.
# Priority: --entity  >  --entities  >  --all  >  interactive prompt.
# ---------------------------------------------------------------------------

def parse_entities_arg(raw, all_keys):
    """Parse a comma-separated --entities value into a validated key list,
    preserving ENTITIES order (not the order the user typed them in)."""
    requested = {k.strip().lower() for k in raw.split(",") if k.strip()}
    unknown = requested - set(all_keys)
    if unknown:
        valid = ", ".join(all_keys)
        raise SystemExit(f"Unknown entity key(s): {', '.join(sorted(unknown))}. Valid keys: {valid}")
    return [e for e in ENTITIES if e["key"] in requested]


def prompt_entity_selection():
    """Interactive picker used when no --entity / --entities / --all flag
    was given. Asks ALL vs PICK, and if PICK, which ones (by number)."""
    log_info("No --entity / --entities / --all flag given — asking interactively.")
    print("\nWhich entities should be synced?")
    print("  a) ALL  (pricelist, accounts, discountvalues2, stock)")
    print("  p) PICK specific ones")
    while True:
        choice = input("Choose [a/p]: ").strip().lower()
        if choice in ("a", "all", ""):
            return ENTITIES
        if choice in ("p", "pick"):
            break
        print("Please type 'a' for all or 'p' to pick.")

    print("\nAvailable entities:")
    for i, e in enumerate(ENTITIES, start=1):
        print(f"  {i}) {e['name']}  (key: {e['key']})")

    while True:
        raw = input(
            "\nEnter the numbers or keys you want, comma-separated "
            "(e.g. '1,3' or 'pricelist,stock'): "
        ).strip()
        if not raw:
            print("Please enter at least one entity.")
            continue

        tokens = [t.strip() for t in raw.split(",") if t.strip()]
        selected = []
        bad_tokens = []
        for tok in tokens:
            if tok.isdigit() and 1 <= int(tok) <= len(ENTITIES):
                entity = ENTITIES[int(tok) - 1]
                if entity not in selected:
                    selected.append(entity)
            else:
                match = next((e for e in ENTITIES if e["key"] == tok.lower()), None)
                if match and match not in selected:
                    selected.append(match)
                elif not match:
                    bad_tokens.append(tok)

        if bad_tokens:
            print(f"Didn't recognize: {', '.join(bad_tokens)}. Try again.")
            continue
        if not selected:
            print("No valid entities selected. Try again.")
            continue

        # Keep canonical ENTITIES order regardless of how the user typed them.
        ordered = [e for e in ENTITIES if e in selected]
        names = ", ".join(e["name"] for e in ordered)
        confirm = input(f"Sync these {len(ordered)} entities: {names}? [y/N]: ").strip().lower()
        if confirm == "y":
            return ordered
        print("OK, let's try the selection again.")


# ---------------------------------------------------------------------------
# Per-entity pipeline (streaming: fetch and upload overlap)
# ---------------------------------------------------------------------------

def process_entity(session_boto, entity, dry_run=False, auto=False, region=DEFAULT_REGION,
                    upload_workers=UPLOAD_WORKERS, use_gzip=False, confirm_state=None):
    name = entity["name"]
    table_name = entity["table"]
    endpoint = entity["endpoint"]

    log_info("=" * 70)
    log_info(f"{name}  (table: {table_name}  ->  {endpoint})")
    log_info("=" * 70)

    if confirm_state is None:
        confirm_state = ConfirmState(already_confirmed=auto)

    if confirm_state.aborted:
        log_info(f"Skipping {name} — run was aborted at an earlier confirmation prompt.")
        return

    dynamodb = session_boto.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    if SUPPLIER_ID:
        log_info(f"Querying {table_name} for SupplierID={SUPPLIER_ID}...")
        page_source = iter_query_pages_by_supplier(table, SUPPLIER_ID)
    else:
        log_info(f"Scanning {table_name} in full ({SCAN_SEGMENTS} parallel segments, all suppliers)...")
        page_source = iter_full_scan(table)

    http_session = build_session(upload_workers)

    buffer = []
    batch_num = 0
    total_records = 0
    first_batch_handled = False
    submitted_futures = []
    t0 = time.time()

    def flush_buffer(executor, force=False):
        """Pop full-size batches off the buffer and submit them for upload."""
        nonlocal batch_num
        results = []
        while len(buffer) >= UPLOAD_BATCH_SIZE or (force and buffer):
            take = UPLOAD_BATCH_SIZE if len(buffer) >= UPLOAD_BATCH_SIZE else len(buffer)
            batch = [clean_item(i) for i in buffer[:take]]
            del buffer[:take]
            batch_num += 1
            results.append((batch_num, batch))
        for bnum, batch in results:
            fut = executor.submit(upload_batch, http_session, endpoint, batch, bnum, "?", use_gzip)
            submitted_futures.append(fut)

    if dry_run:
        # Just pull enough to show a sample, no need to read the whole table.
        for page_items in page_source:
            buffer.extend(page_items)
            total_records += len(page_items)
            if buffer:
                sample = clean_item(buffer[0])
                log_info(f"[DRY RUN] Sample record:\n{json.dumps(sample, indent=2, default=str)}")
                break
        log_info(f"[DRY RUN] Stopping early — not reading the full table.")
        return

    with ThreadPoolExecutor(max_workers=upload_workers) as executor:
        for page_items in page_source:
            buffer.extend(page_items)
            total_records += len(page_items)

            if not first_batch_handled and len(buffer) >= UPLOAD_BATCH_SIZE:
                first_batch = [clean_item(i) for i in buffer[:UPLOAD_BATCH_SIZE]]
                del buffer[:UPLOAD_BATCH_SIZE]
                batch_num += 1

                if confirm_state.confirmed:
                    # Already confirmed earlier in the run (this entity or a
                    # previous one) — just send it like any other batch, no pause.
                    ok, resp = upload_batch(http_session, endpoint, first_batch, batch_num, "?", use_gzip)
                    if not ok:
                        log_error(f"First batch for {name} failed. Stopping before sending the rest.")
                        return
                    first_batch_handled = True
                else:
                    # This is the very first batch of the WHOLE run — pause here once.
                    log_info(f"Sending first batch ({len(first_batch)} records) for review...")
                    ok, resp = upload_batch(http_session, endpoint, first_batch, batch_num, "?", use_gzip)
                    log_info(f"Sample record sent:\n{json.dumps(first_batch[0], indent=2, default=str)}")
                    if resp is not None:
                        log_info(f"API response body (truncated): {resp.text[:500]}")
                    if not ok:
                        log_error(f"First batch for {name} failed. Stopping before sending the rest.")
                        return
                    answer = input(
                        f"\nFirst batch of {name} looks OK? Type 'y' to continue sending "
                        f"EVERYTHING else — all remaining batches and all remaining "
                        f"entities — with no further pauses: "
                    ).strip().lower()
                    if answer == "y":
                        with confirm_state.lock:
                            confirm_state.confirmed = True
                        first_batch_handled = True
                    else:
                        log_info("Aborting run per user choice at the first-batch confirmation.")
                        with confirm_state.lock:
                            confirm_state.aborted = True
                        return

            if first_batch_handled:
                flush_buffer(executor)

        # If we never even hit one full batch (small table), handle it here.
        if not first_batch_handled and buffer:
            first_batch = [clean_item(i) for i in buffer]
            buffer.clear()
            batch_num += 1
            log_info(f"Sending only batch ({len(first_batch)} records) for review...")
            ok, resp = upload_batch(http_session, endpoint, first_batch, batch_num, 1, use_gzip)
            log_info(f"Sample record sent:\n{json.dumps(first_batch[0], indent=2, default=str)}")
            if resp is not None:
                log_info(f"API response body (truncated): {resp.text[:500]}")
            first_batch_handled = True
        elif first_batch_handled:
            flush_buffer(executor, force=True)

        success, fail = 0, 0
        for fut in as_completed(submitted_futures):
            ok, _ = fut.result()
            if ok:
                success += 1
            else:
                fail += 1

    elapsed = time.time() - t0
    log_info(
        f"{name} done: {total_records} record(s), {batch_num} batch(es) total "
        f"({success} OK / {fail} failed), in {elapsed:.1f}s"
    )

#---------------------------------------------------------------------------
# User Inputs
#---------------------------------------------------------------------------
def prompt_configuration():
    """Prompt the user for runtime configuration."""

    global AWS_PROFILE
    global SUPPLIER_ID
    global API_USERNAME
    global API_PASSWORD
    global API_BASE

    print("\n===== Runtime Configuration =====\n")

    AWS_PROFILE = input("AWS Profile: ").strip()
    SUPPLIER_ID = input("Supplier ID: ").strip()
    API_USERNAME = input("API Username: ").strip()
    API_PASSWORD = input("API Password: ").strip()

    while True:
        env = input("Environment (qa/prod): ").strip().lower()

        if env == "qa":
            API_BASE = "https://qa.rapidtradews.com/post"
            break
        elif env == "prod":
            API_BASE = "https://rapi.rapidtradews.com/post"
            break
        else:
            print("Please enter either 'qa' or 'prod'.")

    print("\nConfiguration Loaded")
    print(f" AWS Profile : {AWS_PROFILE}")
    print(f" Supplier ID : {SUPPLIER_ID}")
    print(f" Environment : {env.upper()}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global ENTITIES
    prompt_configuration()

    ENTITIES = [
        {
            "key": "pricelist",
            "name": "Pricelists",
            "table": "TradePricelists",
            "endpoint": f"{API_BASE}/pricelist",
        },
        {
            "key": "accounts",
            "name": "Accounts",
            "table": "TargetsAccounts2",
            "endpoint": f"{API_BASE}/account",
        },
        {
            "key": "discountvalues2",
            "name": "DiscountValues",
            "table": "TradeDiscountValues2",
            "endpoint": f"{API_BASE}/discountvalues2",
        },
        {
            "key": "stock",
            "name": "Stock",
            "table": "TradeStock",
            "endpoint": f"{API_BASE}/stock",
        },
    ]

    all_keys = [e["key"] for e in ENTITIES]


    parser = argparse.ArgumentParser(description="Sync Pricelists/Accounts/DiscountValues/Stock to FC QA API")
    parser.add_argument(
        "--entity",
        choices=all_keys,
        help="Run only this single entity, no prompt",
    )
    parser.add_argument(
        "--entities",
        help=f"Comma-separated list of entities to run, no prompt (choices: {', '.join(all_keys)})",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Run all four entities, no prompt",
    )
    parser.add_argument("--dry-run", action="store_true", help="Peek + chunk only, no uploads")
    parser.add_argument("--auto", action="store_true",
                         help="Skip the confirmation prompt; also runs all entities CONCURRENTLY")
    parser.add_argument("--region", default=DEFAULT_REGION, help="AWS region for DynamoDB tables")
    parser.add_argument("--workers", type=int, default=UPLOAD_WORKERS,
                         help=f"Concurrent upload threads per entity (default {UPLOAD_WORKERS})")
    parser.add_argument("--gzip", action="store_true", help="Gzip-compress upload request bodies")
    args = parser.parse_args()

    session = boto3.Session(profile_name=AWS_PROFILE, region_name=args.region)

    # Decide which entities to run. Explicit flags always win and skip the
    # prompt (important for --auto / cron use so nothing blocks on input()).
    if args.entity:
        targets = [e for e in ENTITIES if e["key"] == args.entity]
    elif args.entities:
        targets = parse_entities_arg(args.entities, all_keys)
    elif args.all:
        targets = ENTITIES
    else:
        targets = prompt_entity_selection()

    if not targets:
        log_error("No entities selected to run. Exiting.")
        return

    log_info(f"Selected entities: {', '.join(e['name'] for e in targets)}")

    overall_start = time.time()

    if args.auto and len(targets) > 1:
        # Entities are independent tables/endpoints -> safe to run concurrently.
        log_info(f"Running {len(targets)} entities concurrently (--auto)...")
        with ThreadPoolExecutor(max_workers=len(targets)) as executor:
            futures = [
                executor.submit(
                    process_entity, session, entity,
                    dry_run=args.dry_run, auto=args.auto, region=args.region,
                    upload_workers=args.workers, use_gzip=args.gzip,
                )
                for entity in targets
            ]
            for fut in as_completed(futures):
                fut.result()
    else:
        # Interactive mode (or single entity) stays sequential so the ONE
        # confirmation prompt happens on the true first batch of the whole
        # run. Once confirmed, confirm_state.confirmed flips to True and
        # every batch after that — in this entity and all later ones —
        # goes straight through with no further pauses.
        confirm_state = ConfirmState(already_confirmed=args.auto)
        for entity in targets:
            if confirm_state.aborted:
                log_info(f"Skipping {entity['name']} — run was aborted at an earlier confirmation prompt.")
                continue
            process_entity(
                session, entity,
                dry_run=args.dry_run, auto=args.auto, region=args.region,
                upload_workers=args.workers, use_gzip=args.gzip,
                confirm_state=confirm_state,
            )

    log_info("=" * 70)
    log_info(f"All done in {time.time() - overall_start:.1f}s. Full log: {LOG_FILE}")


if __name__ == "__main__":
    main()