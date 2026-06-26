import boto3
import pandas as pd
import awswrangler as wr
from datetime import datetime, timezone, timedelta

BUCKET_NAME = "medallion-data-platform"


def calculate_dq_score(df: pd.DataFrame) -> float:
    """Calculates the Data Quality Score as percentage of non-null values."""
    if df.empty:
        return 100.0
    # df.notnull().mean() - percentage of non-null values per column
    # df.notnull().mean().mean() - average percentage of non-null values across all columns
    return float(df.notnull().mean().mean() * 100)


def load_yesterday_hn_posts(yesterday_date: str) -> pd.DataFrame:
    """Load silver posts table, filtered to yesterday's HN posts only."""
    try:
        posts_df = wr.s3.read_parquet(
            path=f"s3://{BUCKET_NAME}/silver/posts/",
            dataset=True,
        )
    except Exception:
        return pd.DataFrame(columns=["post_id", "author_username", "content_text", "created_at", "post_type", "score"])

    posts_df["date_only"] = posts_df["created_at"].str[:10]

    hn_post_types = {"story", "ask", "comment", "job", "poll"}
    yesterday_posts = posts_df[
        (posts_df["date_only"] == yesterday_date) &
        (posts_df["post_type"].isin(hn_post_types))
        ]

    return yesterday_posts


def build_daily_content_counts(posts_df: pd.DataFrame, yesterday_date: str) -> pd.DataFrame:
    """Count HN posts by type for a single day and calculate DQ score."""
    if posts_df.empty:
        return pd.DataFrame(columns=["date", "post_type", "count", "dq_score"])

    if "post_type" in posts_df.columns:
        posts_df["post_type"] = posts_df["post_type"].astype(str)

    counts = posts_df.groupby("post_type", observed=False).size().reset_index(name="count")
    counts["date"] = yesterday_date
    counts["dq_score"] = round(calculate_dq_score(posts_df), 2)

    return counts[["date", "post_type", "count", "dq_score"]]


def build_daily_users_metric(yesterday_date: str) -> pd.DataFrame:
    """Count total users and new users per platform, as of yesterday, with DQ score."""
    try:
        users_df = wr.s3.read_parquet(
            path=f"s3://{BUCKET_NAME}/silver/users/",
            dataset=True,
        )
    except Exception:
        return pd.DataFrame(columns=["date", "platform", "total_users", "new_users", "dq_score"])

    if "platform" in users_df.columns:
        users_df["platform"] = users_df["platform"].astype(str)

    total_counts = users_df.groupby("platform", observed=False).size().reset_index(name="total_users")

    users_df["created_date"] = users_df["created_at"].str[:10]
    yesterday_users = users_df[users_df["created_date"] == yesterday_date]
    new_counts = yesterday_users.groupby("platform", observed=False).size().reset_index(name="new_users")

    metrics = pd.merge(total_counts, new_counts, on="platform", how="left")

    metrics["new_users"] = metrics["new_users"].fillna(0).astype(int)

    metrics["date"] = yesterday_date
    metrics["dq_score"] = round(calculate_dq_score(users_df), 2)

    metrics = metrics[["total_users", "new_users", "dq_score", "platform", "date"]]

    return metrics


def lambda_handler(event, context):
    """Build daily_content_counts and daily_users_metric gold tables with precise formatting."""
    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    hn_posts = load_yesterday_hn_posts(yesterday_str)
    content_counts_df = build_daily_content_counts(hn_posts, yesterday_str)

    try:
        wr.s3.to_parquet(
            df=content_counts_df,
            path=f"s3://{BUCKET_NAME}/gold/daily_content_counts/",
            dataset=True,
            partition_cols=["date"],
            mode="append",
        )
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to write daily_content_counts: {e}"}

    users_metric_df = build_daily_users_metric(yesterday_str)

    try:
        wr.s3.to_parquet(
            df=users_metric_df,
            path=f"s3://{BUCKET_NAME}/gold/daily_users_metric/",
            dataset=True,
            partition_cols=["platform", "date"],
            mode="append",
        )
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to write daily_users_metric: {e}"}

    return {
        "statusCode": 200,
        "body": f"Wrote daily_content_counts ({len(content_counts_df)} rows) and daily_users_metric ({len(users_metric_df)} rows) for {yesterday_str} with complete metrics and strict layout formatting."
    }