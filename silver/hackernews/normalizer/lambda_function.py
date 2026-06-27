import json
import re
import html
import uuid
import boto3
import pandas as pd
import awswrangler as wr
from dataclasses import dataclass, asdict
from typing import Optional
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request

s3 = boto3.client("s3")

BUCKET_NAME = "medallion-data-platform"
HN_USER_URL = "https://hacker-news.firebaseio.com/v0/user/{}.json"
MAX_WORKERS = 50
USERS_COLUMNS = ["user_id", "username", "platform", "karma_score", "is_verified", "created_at", "followers_count"]


@dataclass
class Post:
    post_id: str
    author_username: Optional[str]
    content_text: Optional[str]
    created_at: str
    post_type: str
    score: Optional[int]
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
    followers_count: Optional[int]


def strip_html(text: str) -> Optional[str]:
    if not text:
        return None
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r' +', ' ', text)
    return text.strip() or None


def epoch_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def determine_post_type(item: dict) -> str:
    """Detect Ask HN posts, since HN API never sends type='ask' directly"""
    raw_type = item.get("type", "unknown")
    title = item.get("title", "") or ""

    if raw_type == "story" and title.strip().lower().startswith("ask hn:"):
        return "ask"

    return raw_type


def item_to_post(item: dict, year: str, month: str, day: str) -> Optional[Post]:
    if "id" not in item or "time" not in item:
        return None

    if item.get("text"):
        content_text = strip_html(item["text"])
    elif item.get("title"):
        content_text = item["title"]
    else:
        content_text = None

    return Post(
        post_id=str(item["id"]),
        author_username=item.get("by"),
        content_text=content_text,
        created_at=epoch_to_iso(item["time"]),
        post_type=determine_post_type(item),
        score=item.get("score"),
        year=year,
        month=month,
        day=day,
    )


def fetch_hn_user(username: str) -> Optional[User]:
    try:
        url = HN_USER_URL.format(username)
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
        if not data:
            return None
        created_at = epoch_to_iso(data["created"]) if "created" in data else None
        return User(
            user_id=str(uuid.uuid4()),
            username=username,
            platform="HackerNews",
            karma_score=data.get("karma"),
            is_verified=None,
            created_at=created_at,
            followers_count=None,
        )
    except Exception:
        return None


def fetch_new_users(new_usernames: set) -> list:
    if not new_usernames:
        return []
    users = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for username in new_usernames:
            future = executor.submit(fetch_hn_user, username)
            futures[future] = username
        for future in as_completed(futures):
            result = future.result()
            if result:
                users.append(result)
    return users


def load_existing_users() -> pd.DataFrame:
    try:
        return wr.s3.read_parquet(
            path=f"s3://{BUCKET_NAME}/silver/users/",
            dataset=True,
            partition_filter=lambda x: x["platform"] == "HackerNews",
        )
    except Exception:
        return pd.DataFrame(columns=USERS_COLUMNS)


def lambda_handler(event, context):
    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    year, month, day = yesterday_str[:4], yesterday_str[5:7], yesterday_str[8:10]

    raw_key = f"bronze/hackernews/year={year}/month={month}/day={day}/raw.json"
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

    posts_collection = []
    for post in unique_posts:
        posts_collection.append(asdict(post))
    posts_df = pd.DataFrame(posts_collection)

    # Users
    today_usernames = set(posts_df["author_username"].dropna().unique())
    existing_df = load_existing_users()
    if not existing_df.empty:
        existing_usernames = set(existing_df["username"])
    else:
        existing_usernames = set()

    new_usernames = today_usernames - existing_usernames
    new_hn_users = fetch_new_users(new_usernames)

    if new_hn_users:
        users_collection = []
        for user in new_hn_users:
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
            path=f"s3://{BUCKET_NAME}/silver/staging/posts/hackernews/year={year}/month={month}/day={day}/data.parquet",
        )
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to write posts to staging: {e}"}

    return {
        "statusCode": 200,
        "body": f"Staged {len(unique_posts)} posts, {len(all_users_df)} total users ({len(new_hn_users)} new)",
    }
