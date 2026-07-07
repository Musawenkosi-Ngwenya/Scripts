import boto3
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from typing import Optional
from boto3.dynamodb.conditions import Key

# ── Static config (not prompted) ─────────────────────────────────────────────
BATCH_SIZE   = 100
MAX_WORKERS  = 10
REGION       = "us-east-1"


def prompt_config() -> dict:
    """Ask the user for the values that vary per run."""
    table_name = input("Table name: ").strip()
    while not table_name:
        table_name = input("Table name (required): ").strip()

    seller_id = input("Seller ID: ").strip()
    while not seller_id:
        seller_id = input("Seller ID (required): ").strip()

    index_name = input("Index name: ").strip()
    while not index_name:
        index_name = input("Index name (required): ").strip()

    profile = input(
        "AWS profile [musa, musafc, musarisaas] (leave blank for 'default'): "
    ).strip()
    if not profile:
        profile = "default"

    return {
        "table_name": table_name,
        "seller_id": seller_id,
        "index_name": index_name,
        "profile": profile,
    }


def get_userfield_value(user_fields: list, key: str) -> Optional[str]:
    if not user_fields:
        return None
    for field in user_fields:
        if field.get("Key") == key:
            return field.get("Value")
    return None


def resolve_pack_size(item: dict) -> int:
    if "PackSize" in item and item["PackSize"] is not None:
        try:
            return int(item["PackSize"])
        except (ValueError, TypeError):
            pass

    user_fields = item.get("UserFields", [])
    raw = get_userfield_value(user_fields, "Userfield04")
    if raw is not None:
        try:
            return int(float(str(raw).strip()))  # "1.0" → 1
        except (ValueError, TypeError):
            pass

    return 1


def fetch_all_items(table, seller_id: str, index_name: str) -> list:
    items    = []
    last_key = None
    page     = 0

    print(f"Fetching all records from '{table.name}' (index: {index_name}) …")

    while True:
        kwargs = dict(
            IndexName=index_name,
            KeyConditionExpression=Key("SellerID").eq(seller_id),
        )
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        response = table.query(**kwargs)
        batch    = response.get("Items", [])
        items.extend(batch)
        page    += 1
        print(f"  Page {page:>3}: fetched {len(batch):>5} items  (total so far: {len(items)})")

        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break

    print(f"Fetch complete. Total items: {len(items)}\n")
    return items


def decimal_safe(obj):
    """Convert Decimal to int or float for JSON / DynamoDB put safety."""
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    raise TypeError


def process_and_write_batch(table, batch: list) -> int:
    """
    Resolve PackSize for each item that is missing it,
    then write ALL changed items back to DynamoDB using batch_writer.
    Returns the count of items actually updated.
    """
    to_write = []

    for item in batch:
        # Deserialise Decimals so we can work with plain Python types
        item = json.loads(json.dumps(item, default=decimal_safe))

        # Only update items that are missing PackSize
        if "PackSize" not in item or item["PackSize"] is None:
            item["PackSize"] = resolve_pack_size(item)
            to_write.append(item)

    if not to_write:
        return 0

    # batch_writer handles chunking into DynamoDB's 25-item put limit automatically
    with table.batch_writer() as writer:
        for item in to_write:
            # Re-encode floats/ints as Decimal for DynamoDB
            dynamo_item = json.loads(
                json.dumps(item),
                parse_float=Decimal,
                parse_int=Decimal
            )
            writer.put_item(Item=dynamo_item)

    return len(to_write)


def chunk(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def main():
    config = prompt_config()

    print(
        f"\nUsing profile='{config['profile']}', table='{config['table_name']}', "
        f"index='{config['index_name']}', seller_id='{config['seller_id']}'\n"
    )

    session  = boto3.Session(profile_name=config["profile"])
    dynamodb = session.resource("dynamodb", region_name=REGION)
    table    = dynamodb.Table(config["table_name"])

    all_items = fetch_all_items(table, config["seller_id"], config["index_name"])

    if not all_items:
        print("No records found.")
        return

    # Filter to only items missing PackSize — avoids unnecessary writes
    missing = [i for i in all_items if "PackSize" not in i or i.get("PackSize") is None]
    print(f"{len(missing)} of {len(all_items)} items are missing PackSize — updating those.\n")

    if not missing:
        print("Nothing to update.")
        return

    batches       = list(chunk(missing, BATCH_SIZE))
    total_updated = 0

    print(f"Writing updates in {len(batches)} batches ({MAX_WORKERS} workers) …")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_and_write_batch, table, b): i
            for i, b in enumerate(batches)
        }
        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                count = future.result()
                total_updated += count
                print(f"  Batch {batch_idx + 1:>3} done — {count} items written")
            except Exception as exc:
                print(f"  Batch {batch_idx + 1:>3} ERROR: {exc}")

    print(f"\nAll done. {total_updated} items updated in DynamoDB.")


if __name__ == "__main__":
    main()