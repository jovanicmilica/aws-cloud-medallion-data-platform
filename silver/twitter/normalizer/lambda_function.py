import hashlib
import json
import uuid
import boto3
import pandas as pd
import awswrangler as wr
from dataclasses import dataclass, asdict
from typing import Optional
from datetime import datetime, timezone, timedelta

s3 = boto3.client("s3")

BUCKET_NAME = "medallion-data-platform"
USERS_COLUMNS = ["user_id", "username", "platform", "karma_score", "is_verified", "created_at"]


@dataclass
class Post:
    post_id: str
    author_username: Optional[str]
    content_text: Optional[str]
    created_at: str
    post_type: str
    year: str
    month: str
    day: str


@dataclass
class User:
    user_id: str
    username: str
    platform: str
    karma_score: Optional[int]
    is_verified: Optional[bool]
    created_at: Optional[str]


def parse_twitter_date(date_str: str) -> Optional[str]:
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def make_post_id(tweet: dict) -> str:
    raw = f"{tweet.get('user_name', '')}|{tweet.get('date', '')}|{tweet.get('text', '')}"
    return hashlib.sha256(raw.encode()).hexdigest()


def item_to_post(item: dict, year: str, month: str, day: str) -> Optional[Post]:
    if "date" not in item:
        return None

    created_at = parse_twitter_date(item["date"])
    if created_at is None:
        return None

    if item.get("is_retweet") == "True":
        post_type = "retweet"
    else:
        post_type = "tweet"

    return Post(
        post_id=make_post_id(item),
        author_username=item.get("user_name"),
        content_text=item.get("text") or None,
        created_at=created_at,
        post_type=post_type,
        year=year,
        month=month,
        day=day,
    )


def extract_twitter_user(tweet: dict) -> Optional[User]:
    username = tweet.get("user_name")
    if not username:
        return None

    is_verified_raw = tweet.get("user_verified")
    if isinstance(is_verified_raw, bool):
        is_verified = is_verified_raw
    elif isinstance(is_verified_raw, str):
        is_verified = is_verified_raw.strip().lower() == "true"
    else:
        is_verified = None

    return User(
        user_id=str(uuid.uuid4()),
        username=username,
        platform="X",
        karma_score=None,
        is_verified=is_verified,
        created_at=parse_twitter_date(tweet.get("user_created", "")),
    )


def load_existing_users() -> pd.DataFrame:
    try:
        return wr.s3.read_parquet(
            path=f"s3://{BUCKET_NAME}/silver/users/",
            dataset=True,
            partition_filter=lambda x: x["platform"] == "X",
        )
    except Exception:
        return pd.DataFrame(columns=USERS_COLUMNS)


def lambda_handler(event, context):
    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    year, month, day = yesterday_str[:4], yesterday_str[5:7], yesterday_str[8:10]

    raw_key = f"bronze/twitter/year={year}/month={month}/day={day}/raw.json"
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=raw_key)
        raw_items = json.loads(obj["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to load raw.json: {e}"}

    # Posts
    posts = []
    for item in raw_items:
        post = item_to_post(item, year, month, day)
        if post is not None:
            posts.append(post)

    seen_ids: set = set()
    unique_posts = []
    for post in posts:
        if post.post_id not in seen_ids:
            seen_ids.add(post.post_id)
            unique_posts.append(post)

    posts_df = pd.DataFrame([asdict(p) for p in unique_posts])

    # Users
    today_users: dict = {}
    for tweet in raw_items:
        username = tweet.get("user_name")
        if username and username not in today_users:
            user = extract_twitter_user(tweet)
            if user:
                today_users[username] = user

    existing_df = load_existing_users()
    if not existing_df.empty:
        existing_usernames = set(existing_df["username"])
    else:
        existing_usernames = set()

    new_users = []
    for username, u in today_users.items():
        if username not in existing_usernames:
            new_users.append(u)

    if new_users:
        users_collection = []
        for user in new_users:
            users_collection.append(asdict(user))
        new_users_df = pd.DataFrame(users_collection)
    else:
        new_users_df = pd.DataFrame(columns=USERS_COLUMNS)

    all_users_df = pd.concat([existing_df, new_users_df], ignore_index=True)
    all_users_df = all_users_df.drop_duplicates(subset=["username"])

    try:
        wr.s3.to_parquet(
            df=all_users_df,
            path=f"s3://{BUCKET_NAME}/silver/users/",
            dataset=True,
            partition_cols=["platform"],
            mode="overwrite_partitions",
        )
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to write users: {e}"}

    try:
        wr.s3.to_parquet(
            df=posts_df.drop(columns=["year", "month", "day"]),
            path=f"s3://{BUCKET_NAME}/silver/staging/posts/twitter/year={year}/month={month}/day={day}/data.parquet",
        )
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to write posts to staging: {e}"}

    return {
        "statusCode": 200,
        "body": f"Staged {len(unique_posts)} posts, {len(all_users_df)} total users ({len(new_users)} new)",
    }
