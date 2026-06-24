import pandas as pd
import awswrangler as wr
from datetime import datetime, timezone, timedelta

BUCKET_NAME = "medallion-data-platform"
PLATFORMS = ["twitter", "hackernews"]


def lambda_handler(_event, context):
    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    year, month, day = yesterday_str[:4], yesterday_str[5:7], yesterday_str[8:10]

    final_path = f"s3://{BUCKET_NAME}/silver/posts/year={year}/month={month}/day={day}/data.parquet"

    dfs = []
    staging_paths_read = []

    # Read existing merged output so we don't lose data from a platform that already ran
    try:
        dfs.append(wr.s3.read_parquet(path=final_path))
    except Exception:
        pass

    for platform in PLATFORMS:
        path = f"s3://{BUCKET_NAME}/silver/staging/posts/{platform}/year={year}/month={month}/day={day}/data.parquet"
        try:
            dfs.append(wr.s3.read_parquet(path=path))
            staging_paths_read.append(path)
        except Exception:
            pass

    if not dfs:
        return {"statusCode": 200, "body": f"No data found for {year}-{month}-{day}"}

    combined_df = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=["post_id"])

    try:
        wr.s3.to_parquet(df=combined_df, path=final_path)
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to write merged posts: {e}"}

    for path in staging_paths_read:
        try:
            wr.s3.delete_objects(path)
        except Exception:
            pass

    return {
        "statusCode": 200,
        "body": f"Merged {len(combined_df)} posts for {year}-{month}-{day}",
    }
