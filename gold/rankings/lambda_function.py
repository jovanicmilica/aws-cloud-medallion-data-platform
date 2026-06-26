import boto3
import pandas as pd
import awswrangler as wr
from datetime import datetime, timezone, timedelta

BUCKET_NAME = "medallion-data-platform"


def calculate_dq_score(df: pd.DataFrame) -> float:
    """Calculates the Data Quality Score as percentage of non-null values."""
    if df.empty:
        return 100.0
    # calculate the percentage of non-null values for each column, then average across all columns
    return float(round(df.notnull().mean().mean() * 100, 2))


def load_x_users() -> pd.DataFrame:
    """Load silver users table, filtered to X platform only"""
    try:
        users_df = wr.s3.read_parquet(
            path=f"s3://{BUCKET_NAME}/silver/users/",
            dataset=True,
            partition_filter=lambda x: x["platform"] == "X",
        )
    except Exception:
        return pd.DataFrame(
            columns=["user_id", "username", "platform", "karma_score", "is_verified", "created_at", "followers_count"])

    return users_df


def load_hn_users() -> pd.DataFrame:
    """Load silver users table, filtered to HackerNews platform only"""
    try:
        users_df = wr.s3.read_parquet(
            path=f"s3://{BUCKET_NAME}/silver/users/",
            dataset=True,
            partition_filter=lambda x: x["platform"] == "HackerNews",
        )
    except Exception:
        return pd.DataFrame(
            columns=["user_id", "username", "platform", "karma_score", "is_verified", "created_at", "followers_count"])

    return users_df


def load_yesterday_hn_posts(yesterday_date: str) -> pd.DataFrame:
    """Load silver posts table, filtered to yesterday's HN posts only"""
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


def build_top_x_users_by_followers(users_df: pd.DataFrame, yesterday_date: str) -> pd.DataFrame:
    """Get top 10 X users by follower count with DQ score"""
    if users_df.empty:
        return pd.DataFrame(columns=["username", "followers_count", "rank_position", "dq_score", "date"])

    ranked = users_df.dropna(subset=["followers_count"]).sort_values("followers_count", ascending=False).head(10).copy()

    result = ranked[["username", "followers_count"]].copy()
    result["rank_position"] = range(1, len(result) + 1)
    result["dq_score"] = calculate_dq_score(result)
    result["date"] = yesterday_date

    return result[["username", "followers_count", "rank_position", "dq_score", "date"]]


def build_top_hn_users_by_karma(users_df: pd.DataFrame, yesterday_date: str) -> pd.DataFrame:
    """Get top 10 highest and top 10 lowest HN users by karma score with DQ score"""
    if users_df.empty:
        return pd.DataFrame(columns=["username", "karma_score", "rank_type", "rank_position", "dq_score", "date"])

    clean_df = users_df.dropna(subset=["karma_score"])

    highest = clean_df.sort_values("karma_score", ascending=False).head(10).copy()
    highest["rank_type"] = "highest"
    highest["rank_position"] = range(1, len(highest) + 1)

    lowest = clean_df.sort_values("karma_score", ascending=True).head(10).copy()
    lowest["rank_type"] = "lowest"
    lowest["rank_position"] = range(1, len(lowest) + 1)

    combined = pd.concat([highest, lowest], ignore_index=True)

    result = combined[["username", "karma_score", "rank_type", "rank_position"]].copy()
    result["dq_score"] = calculate_dq_score(result)
    result["date"] = yesterday_date

    return result[["username", "karma_score", "rank_type", "rank_position", "dq_score", "date"]]


def build_top_hn_posts_by_score(posts_df: pd.DataFrame, yesterday_date: str) -> pd.DataFrame:
    """Get top 10 HN story posts by score with DQ score"""
    if posts_df.empty:
        return pd.DataFrame(columns=["post_id", "content_text", "score", "rank_position", "dq_score", "date"])

    story_posts = posts_df[posts_df["post_type"] == "story"].dropna(subset=["score"])
    ranked = story_posts.sort_values("score", ascending=False).head(10).copy()

    result = ranked[["post_id", "content_text", "score"]].copy()
    result["rank_position"] = range(1, len(result) + 1)
    result["dq_score"] = calculate_dq_score(result)
    result["date"] = yesterday_date

    return result[["post_id", "content_text", "score", "rank_position", "dq_score", "date"]]


def build_top_hn_jobs_by_score(posts_df: pd.DataFrame, yesterday_date: str) -> pd.DataFrame:
    """Get top 10 HN job posts by score with DQ score"""
    if posts_df.empty:
        return pd.DataFrame(columns=["post_id", "content_text", "score", "rank_position", "dq_score", "date"])

    job_posts = posts_df[posts_df["post_type"] == "job"].dropna(subset=["score"])
    ranked = job_posts.sort_values("score", ascending=False).head(10).copy()

    result = ranked[["post_id", "content_text", "score"]].copy()
    result["rank_position"] = range(1, len(result) + 1)
    result["dq_score"] = calculate_dq_score(result)
    result["date"] = yesterday_date

    return result[["post_id", "content_text", "score", "rank_position", "dq_score", "date"]]


def lambda_handler(event, context):
    """Build all four top-N gold ranking tables for yesterday with automatic date partitioning"""
    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    x_users = load_x_users()
    hn_users = load_hn_users()
    hn_posts = load_yesterday_hn_posts(yesterday_str)

    tables = {
        "top_x_users_by_followers": build_top_x_users_by_followers(x_users, yesterday_str),
        "top_hn_users_by_karma": build_top_hn_users_by_karma(hn_users, yesterday_str),
        "top_hn_posts_by_score": build_top_hn_posts_by_score(hn_posts, yesterday_str),
        "top_hn_jobs_by_score": build_top_hn_jobs_by_score(hn_posts, yesterday_str),
    }

    written_summary = []
    for table_name, df in tables.items():
        try:
            wr.s3.to_parquet(
                df=df,
                path=f"s3://{BUCKET_NAME}/gold/{table_name}/",
                dataset=True,
                partition_cols=["date"],
                mode="append",
            )
            written_summary.append(f"{table_name} ({len(df)} rows)")
        except Exception as e:
            return {"statusCode": 500, "body": f"Failed to write {table_name}: {e}"}

    return {
        "statusCode": 200,
        "body": f"Wrote {', '.join(written_summary)} for {yesterday_str} with DQ scores and standard partition formatting."
    }