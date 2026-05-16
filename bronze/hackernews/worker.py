import json
import boto3
import urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = boto3.client("s3")

BUCKET_NAME = "medallion-data-platform"
HN_ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"

ALLOWED_TYPES = {"story", "ask", "comment", "job", "poll"}
MAX_WORKERS = 50


def fetch_item(item_id):
    try:
        with urllib.request.urlopen(HN_ITEM_URL.format(item_id), timeout=5) as response:
            return json.loads(response.read())
    except:
        return None


def is_yesterday(ts):
    item_time = datetime.fromtimestamp(ts, tz=timezone.utc)
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    return item_time.date() == yesterday.date()


def lambda_handler(event, context):

    # SQS triggers Lambda with a list of records (batch size = 1, always one message)
    for record in event["Records"]:
        message = json.loads(record["body"])

        chunk_index = message["chunk_index"]
        total_chunks = message["total_chunks"]
        date = message["date"]
        item_ids = message["item_ids"]

        # Fetch all items in the chunk in parallel
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_item, iid): iid for iid in item_ids}
            for future in as_completed(futures):
                item = future.result()
                if not item or "time" not in item:
                    continue
                if is_yesterday(item["time"]) and item.get("type") in ALLOWED_TYPES:
                    results.append(item)

        print(f"Chunk {chunk_index + 1}/{total_chunks}: {len(results)} items found for {date}")

        # Save partial results to S3
        year, month, day = date[:4], date[5:7], date[8:10]
        key = f"bronze/hackernews/year={year}/month={month}/day={day}/chunk_{chunk_index:04d}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(results).encode("utf-8"),
            ContentType="application/json"
        )

    return {"statusCode": 200}
