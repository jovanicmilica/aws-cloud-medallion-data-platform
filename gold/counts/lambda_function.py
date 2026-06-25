import boto3
import pandas as pd
import awswrangler as wr
from datetime import datetime, timezone, timedelta

BUCKET_NAME = "medallion-data-platform"


def load_yesterday_hn_posts(yesterday_date: str) -> pd.DataFrame:
    """Load silver posts table, filtered to yesterday's HN posts only."""
    try:
        posts_df = wr.s3.read_parquet(
            path=f"s3://{BUCKET_NAME}/silver/posts/",
            dataset=True,
        )
    except Exception:
        return pd.DataFrame(columns=["post_id", "author_username", "content_text", "created_at", "post_type", "score"])

    # created_at is an ISO string like "2026-06-24T10:30:00Z", take the date part only
    posts_df["date_only"] = posts_df["created_at"].str[:10]

    # keep only yesterday's rows and only HN post types (exclude tweet/retweet)
    hn_post_types = {"story", "ask", "comment", "job", "poll"}
    yesterday_posts = posts_df[
        (posts_df["date_only"] == yesterday_date) &
        (posts_df["post_type"].isin(hn_post_types))
    ]

    return yesterday_posts


def build_daily_content_counts(posts_df: pd.DataFrame, yesterday_date: str) -> pd.DataFrame:
    """Count HN posts by type for a single day."""
    if posts_df.empty:
        return pd.DataFrame(columns=["date", "post_type", "count"])

    # group rows by post_type and count how many are in each group
    counts = posts_df.groupby("post_type").size().reset_index(name="count")
    counts["date"] = yesterday_date

    # reorder columns to match the gold table schema
    counts = counts[["date", "post_type", "count"]]

    return counts


def build_daily_users_metric(yesterday_date: str) -> pd.DataFrame:
    """Count total users per platform, as of yesterday."""
    try:
        users_df = wr.s3.read_parquet(
            path=f"s3://{BUCKET_NAME}/silver/users/",
            dataset=True,
        )
    except Exception:
        return pd.DataFrame(columns=["date", "platform", "total_users"])

    # count rows per platform
    counts = users_df.groupby("platform").size().reset_index(name="total_users")
    counts["date"] = yesterday_date

    counts = counts[["date", "platform", "total_users"]]

    return counts


def lambda_handler(event, context):
    """Build daily_content_counts and daily_users_metric gold tables for yesterday."""
    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    year, month, day = yesterday_str[:4], yesterday_str[5:7], yesterday_str[8:10]

    # build daily_content_counts
    hn_posts = load_yesterday_hn_posts(yesterday_str)
    content_counts_df = build_daily_content_counts(hn_posts, yesterday_str)

    try:
        wr.s3.to_parquet(
            df=content_counts_df,
            path=f"s3://{BUCKET_NAME}/gold/daily_content_counts/year={year}/month={month}/day={day}/data.parquet",
        )
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to write daily_content_counts: {e}"}

    # build daily_users_metric
    users_metric_df = build_daily_users_metric(yesterday_str)

    try:
        wr.s3.to_parquet(
            df=users_metric_df,
            path=f"s3://{BUCKET_NAME}/gold/daily_users_metric/year={year}/month={month}/day={day}/data.parquet",
        )
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to write daily_users_metric: {e}"}

    return {
        "statusCode": 200,
        "body": f"Wrote daily_content_counts ({len(content_counts_df)} rows) and daily_users_metric ({len(users_metric_df)} rows) for {yesterday_str}"
    }