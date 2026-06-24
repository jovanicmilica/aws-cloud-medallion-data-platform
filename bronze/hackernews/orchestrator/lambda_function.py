import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

sqs = boto3.client("sqs")
s3 = boto3.client("s3")

BUCKET_NAME = "medallion-data-platform"
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

HN_MAX_URL = "https://hacker-news.firebaseio.com/v0/maxitem.json"
HN_ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"
ALGOLIA_SEARCH_URL = "https://hn.algolia.com/api/v1/search_by_date"

CHUNK_SIZE = 1000


def fetch_item(item_id):
    try:
        with urllib.request.urlopen(HN_ITEM_URL.format(item_id), timeout=5) as response:
            return json.loads(response.read())
    except:
        return None


def get_item_date(item_id):
    item = fetch_item(item_id)
    if not item or "time" not in item:
        return None
    return datetime.fromtimestamp(item["time"], tz=timezone.utc).date()


def find_start_id_algolia(yesterday):
    """
    Query Algolia for the last item posted before yesterday midnight.
    Returns its HN item ID, which sits just below yesterday's first item.
    Single API call — O(1) vs O(log n) HN Firebase calls for binary search.
    """
    yesterday_start_ts = int(
        datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=timezone.utc).timestamp()
    )
    params = urllib.parse.urlencode({
        "numericFilters": f"created_at_i<{yesterday_start_ts}",
        "hitsPerPage": 1,
    })
    url = f"{ALGOLIA_SEARCH_URL}?{params}"

    with urllib.request.urlopen(url, timeout=10) as response:
        data = json.loads(response.read())

    hits = data.get("hits", [])
    if not hits:
        return None

    return int(hits[0]["objectID"])


def find_start_id_binary_search(max_id, yesterday):
    """Binary search fallback to find the first item ID belonging to yesterday."""
    lo = max_id - 30000
    hi = max_id

    while lo < hi:
        mid = (lo + hi) // 2
        mid_date = get_item_date(mid)

        if mid_date is None:
            lo = mid + 1
            continue

        if mid_date < yesterday:
            lo = mid + 1
        else:
            hi = mid

    return lo


def find_start_id(max_id):
    """Find the first item ID from yesterday, using Algolia with binary search fallback."""
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    try:
        start_id = find_start_id_algolia(yesterday)
        if start_id is not None:
            print(f"Start ID found via Algolia: {start_id} for date {yesterday}")
            return start_id
        print("Algolia returned no hits, falling back to binary search")
    except Exception as e:
        print(f"Algolia lookup failed ({e}), falling back to binary search")

    start_id = find_start_id_binary_search(max_id, yesterday)
    print(f"Start ID found via binary search: {start_id} for date {yesterday}")
    return start_id


def lambda_handler(event, context):

    # 1. Get the latest item ID
    try:
        with urllib.request.urlopen(HN_MAX_URL, timeout=5) as response:
            max_id = json.loads(response.read())
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to fetch max ID: {e}"}

    # 2. Find the start ID for yesterday
    start_id = find_start_id(max_id)

    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    item_ids = list(range(start_id, max_id + 1))
    chunks = [item_ids[i:i + CHUNK_SIZE] for i in range(0, len(item_ids), CHUNK_SIZE)]

    print(f"Total items: {len(item_ids)}, chunks: {len(chunks)}")

    # 3. Save job metadata to S3 so the merger knows how many chunks to expect
    meta_key = f"bronze/hackernews/year={yesterday_str[:4]}/month={yesterday_str[5:7]}/day={yesterday_str[8:10]}/meta.json"
    meta = {
        "date": yesterday_str,
        "total_chunks": len(chunks),
        "start_id": start_id,
        "max_id": max_id
    }
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=meta_key,
        Body=json.dumps(meta).encode("utf-8"),
        ContentType="application/json"
    )

    # 4. Send each chunk as a separate SQS message to be processed in parallel
    for i, chunk in enumerate(chunks):
        message = {
            "chunk_index": i,
            "total_chunks": len(chunks),
            "date": yesterday_str,
            "item_ids": chunk
        }
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message)
        )

    return {
        "statusCode": 200,
        "body": f"Sent {len(chunks)} chunks to SQS for date {yesterday_str}"
    }
