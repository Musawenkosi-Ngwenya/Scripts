#!/usr/bin/env python3
"""
DynamoDB Query Counter (terminal version, fast)

- Uses Select='COUNT' so DynamoDB doesn't return full item payloads,
  only the count per page — much less data over the wire.
- Queries multiple partition key values concurrently (I/O-bound work,
  so a thread pool gives real speedup even under the GIL).
- Live, in-place progress per partition instead of scrolling spam.

Usage:
    python dynamodb_query_counter.py
"""

import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from boto3.dynamodb.conditions import Key

MAX_WORKERS = 10  # concurrent partitions in flight at once
PAGE_LIMIT = 1000  # items evaluated per DynamoDB page (max allowed)

print_lock = threading.Lock()


def prompt_required(prompt_text):
    while True:
        value = input(prompt_text).strip()
        if value:
            return value
        print("   This field is required.")


def collect_partition_values():
    print("\nEnter partition key values, one per line.")
    print("(Paste multiple lines if your terminal supports it, or type one at a time.)")
    print("Press Enter on an empty line when you're done.\n")

    values = []
    while True:
        line = input("Partition key value (blank to finish): ").strip()
        if not line:
            if values:
                break
            print("   You must enter at least one value.")
            continue
        values.append(line)

    seen = set()
    deduped = [v for v in values if not (v in seen or seen.add(v))]
    return deduped


def query_partition(table, partition_key_name, sort_key_name, sort_key_prefix,
                     partition_value, idx, total):
    """Runs in a worker thread. Paginates with COUNT-only queries."""
    record_count = 0
    page_count = 0
    last_evaluated_key = None

    key_cond = Key(partition_key_name).eq(partition_value)
    if sort_key_name and sort_key_prefix:
        key_cond = key_cond & Key(sort_key_name).begins_with(sort_key_prefix)

    while True:
        query_params = {
            "KeyConditionExpression": key_cond,
            "Select": "COUNT",   # <-- key speedup: no item payloads returned
            "Limit": PAGE_LIMIT,
        }
        if last_evaluated_key:
            query_params["ExclusiveStartKey"] = last_evaluated_key

        response = table.query(**query_params)

        record_count += response["Count"]
        page_count += 1

        with print_lock:
            sys.stdout.write(
                f"\r\033[K[{idx}/{total}] '{partition_value}': "
                f"page {page_count}, running total {record_count}"
            )
            sys.stdout.flush()

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    with print_lock:
        sys.stdout.write(
            f"\r\033[K[{idx}/{total}] '{partition_value}': "
            f"done — {record_count} records in {page_count} page(s)\n"
        )
        sys.stdout.flush()

    return partition_value, record_count, page_count


def main():
    print("🔍 DynamoDB Query Counter (fast, parallel)")
    print("Query any DynamoDB table and count matching records.\n")

    profile_name = prompt_required("AWS Profile (e.g., musarisaas): ")
    table_name = prompt_required("Table Name (e.g., CoreUser): ")
    partition_values = collect_partition_values()
    sort_key_prefix = input("Sort Key Prefix (optional, press Enter to skip): ").strip()

    print("\n🔄 Connecting to DynamoDB...")

    try:
        session = boto3.Session(profile_name=profile_name)
        dynamodb = session.resource("dynamodb")
        table = dynamodb.Table(table_name)

        print("🔍 Detecting table schema...")
        table.load()

        key_schema = {item["AttributeName"]: item["KeyType"] for item in table.key_schema}
        partition_key_name = next(name for name, kt in key_schema.items() if kt == "HASH")
        sort_key_name = next((name for name, kt in key_schema.items() if kt == "RANGE"), None)

        print(f"\n📊 Table: {table_name}")
        print(f"🔑 Partition Key: '{partition_key_name}'")
        print(f"📝 Querying {len(partition_values)} partition value(s) "
              f"with up to {MAX_WORKERS} in parallel:")
        for pv in partition_values:
            print(f"   - {pv}")
        if sort_key_name:
            if sort_key_prefix:
                print(f"🔑 Sort Key: '{sort_key_name}' begins_with '{sort_key_prefix}'")
            else:
                print(f"🔑 Sort Key: '{sort_key_name}' (no filter applied)")
        print("\n" + "=" * 60)

        total_record_count = 0
        total_page_count = 0
        partition_results = {}

        workers = min(MAX_WORKERS, len(partition_values))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    query_partition, table, partition_key_name, sort_key_name,
                    sort_key_prefix, pv, idx, len(partition_values)
                ): pv
                for idx, pv in enumerate(partition_values, 1)
            }

            for future in as_completed(futures):
                partition_value, record_count, page_count = future.result()
                partition_results[partition_value] = {
                    "records": record_count,
                    "pages": page_count,
                }
                total_record_count += record_count
                total_page_count += page_count

        # Final summary
        print("\n" + "=" * 60)
        print(f"📊 Table: {table_name}")
        print(f"🔑 Partition Key: '{partition_key_name}'")
        if sort_key_name and sort_key_prefix:
            print(f"🔑 Sort Key Filter: '{sort_key_name}' begins_with '{sort_key_prefix}'")
        print("\n✅ Query Complete!")
        print("\n📈 OVERALL SUMMARY:")
        print(f"   Total Records: {total_record_count}")
        print(f"   Total Pages: {total_page_count}")
        print(f"   Partitions Queried: {len(partition_values)}")

        if len(partition_values) > 1:
            print("\n📊 BREAKDOWN BY PARTITION:")
            # preserve original input order in the breakdown
            for pv in partition_values:
                results = partition_results[pv]
                print(f"   '{pv}':")
                print(f"      Records: {results['records']}")
                print(f"      Pages: {results['pages']}")

    except AttributeError:
        print(f"❌ Error: AWS profile '{profile_name}' not found.")
        print("💡 Check your AWS credentials configuration.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"🔍 Error Type: {type(e).__name__}")
        sys.exit(1)


if __name__ == "__main__":
    main()