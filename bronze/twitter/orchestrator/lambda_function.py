import json
import boto3
import csv
import io
from datetime import datetime, timezone, timedelta

s3 = boto3.client("s3")

BUCKET_NAME = "medallion-data-platform"
SOURCE_KEY = "bronze/twitter/covid19_tweets.csv"
CHUNK_SIZE = 1000


def lambda_handler(event, context):
    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    year, month, day = yesterday_str[:4], yesterday_str[5:7], yesterday_str[8:10]
    prefix = f"bronze/twitter/year={year}/month={month}/day={day}/"

    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=SOURCE_KEY)
    except Exception as e:
        return {"statusCode": 500, "body": f"File not found: {e}"}

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=SOURCE_KEY)
    content = obj["Body"].read().decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(content))
    chunk = []
    chunk_index = 0
    total_rows = 0

    for row in reader:
        chunk.append(dict(row))

        if len(chunk) >= CHUNK_SIZE:
            chunk_key = f"{prefix}chunk_{chunk_index:04d}.json"
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=chunk_key,
                Body=json.dumps(chunk, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json"
            )
            print(f"Saved chunk {chunk_index}: {len(chunk)} rows")
            total_rows += len(chunk)
            chunk = []
            chunk_index += 1

    if chunk:
        chunk_key = f"{prefix}chunk_{chunk_index:04d}.json"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=chunk_key,
            Body=json.dumps(chunk, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json"
        )
        total_rows += len(chunk)
        chunk_index += 1

    meta_key = f"{prefix}meta.json"
    meta = {
        "date": yesterday_str,
        "total_chunks": chunk_index,
        "source_key": SOURCE_KEY
    }
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=meta_key,
        Body=json.dumps(meta).encode("utf-8"),
        ContentType="application/json"
    )

    # Trigger the merger lambda
    lambda_client = boto3.client("lambda")
    lambda_client.invoke(
        FunctionName="twitter-merger",
        InvocationType="Event"
    )
    print("Triggered twitter-merger")

    return {
        "statusCode": 200,
        "body": f"Sent {chunk_index} chunks for date {yesterday_str}"
    }