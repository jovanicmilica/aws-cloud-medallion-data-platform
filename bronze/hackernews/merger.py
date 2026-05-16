import json
import boto3
from datetime import datetime, timezone, timedelta

s3 = boto3.client("s3")

BUCKET_NAME = "medallion-data-platform"


def lambda_handler(event, context):

    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    year, month, day = yesterday_str[:4], yesterday_str[5:7], yesterday_str[8:10]
    prefix = f"bronze/hackernews/year={year}/month={month}/day={day}/"

    # 1. Load metadata to know how many chunks to expect
    meta_key = f"{prefix}meta.json"
    try:
        meta_obj = s3.get_object(Bucket=BUCKET_NAME, Key=meta_key)
        meta = json.loads(meta_obj["Body"].read())
        total_chunks = meta["total_chunks"]
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to load meta.json: {e}"}

    # 2. List all chunk files
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    chunk_keys = [
        obj["Key"] for obj in response.get("Contents", [])
        if "chunk_" in obj["Key"]
    ]

    # 3. Check if all chunks have arrived
    if len(chunk_keys) < total_chunks:
        return {
            "statusCode": 202,
            "body": f"Not ready yet: {len(chunk_keys)}/{total_chunks} chunks"
        }

    # 4. Merge all chunks into a single file
    all_items = []
    for key in sorted(chunk_keys):
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        items = json.loads(obj["Body"].read())
        all_items.extend(items)

    print(f"Total items merged: {len(all_items)}")

    # 5. Save final raw.json
    final_key = f"{prefix}raw.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=final_key,
        Body=json.dumps(all_items).encode("utf-8"),
        ContentType="application/json"
    )

    # 6. Delete partial chunk files and metadata
    for key in chunk_keys:
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)
    s3.delete_object(Bucket=BUCKET_NAME, Key=meta_key)

    return {
        "statusCode": 200,
        "body": f"Saved {len(all_items)} items to s3://{BUCKET_NAME}/{final_key}"
    }
