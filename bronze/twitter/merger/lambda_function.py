import json
import boto3
from datetime import datetime, timezone, timedelta

s3 = boto3.client("s3")

BUCKET_NAME = "medallion-data-platform"

def lambda_handler(event, context):

    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    year, month, day = yesterday_str[:4], yesterday_str[5:7], yesterday_str[8:10]
    prefix = f"bronze/twitter/year={year}/month={month}/day={day}/"


    meta_key = f"{prefix}meta.json"
    try:
        meta_obj = s3.get_object(Bucket=BUCKET_NAME, Key=meta_key)
        meta = json.loads(meta_obj["Body"].read())
        total_chunks = meta["total_chunks"]
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to load meta.json: {e}"}


    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    chunk_keys = [
        obj["Key"] for obj in response.get("Contents", [])
        if "chunk_" in obj["Key"]
    ]


    if len(chunk_keys) < total_chunks:
        return {
            "statusCode": 202,
            "body": f"Not ready yet: {len(chunk_keys)}/{total_chunks} chunks"
        }

    all_tweets = []

    for key in sorted(chunk_keys):
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        tweets = json.loads(obj["Body"].read())
        all_tweets.extend(tweets)

    final_key = f"{prefix}raw.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=final_key,
        Body=json.dumps(all_tweets, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )

    for key in chunk_keys:
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)

    s3.delete_object(Bucket=BUCKET_NAME, Key=meta_key)

    return {
        "statusCode": 200,
        "body": f"Saved merged dataset to s3://{BUCKET_NAME}/{final_key}"
    }