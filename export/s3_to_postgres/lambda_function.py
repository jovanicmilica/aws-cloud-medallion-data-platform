import os

import awswrangler as wr
import psycopg2
from psycopg2.extras import execute_values

GOLD_TABLES = [
    ("gold/daily_content_counts/",      "daily_content_counts",      ["date", "post_type"]),
    ("gold/daily_users_metric/",        "daily_users_metric",        ["platform", "date"]),
    ("gold/top_x_users_by_followers/",  "top_x_users_by_followers",  ["date", "rank_position"]),
    ("gold/top_hn_users_by_karma/",     "top_hn_users_by_karma",     ["date", "rank_type", "rank_position"]),
    ("gold/top_hn_posts_by_score/",     "top_hn_posts_by_score",     ["date", "rank_position"]),
    ("gold/top_hn_jobs_by_score/",      "top_hn_jobs_by_score",      ["date", "rank_position"]),
]


def upsert(conn, df, table, pk_cols):
    cols = list(df.columns)
    non_pk = [c for c in cols if c not in pk_cols]
    values = [tuple(x.item() if hasattr(x, 'item') else x for x in row) for row in df.itertuples(index=False, name=None)]
    sql = f"""
        INSERT INTO {table} ({", ".join(cols)})
        VALUES %s
        ON CONFLICT ({", ".join(pk_cols)}) DO UPDATE
        SET {", ".join(f"{c} = EXCLUDED.{c}" for c in non_pk)}
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()


def lambda_handler(event, context):
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )

    bucket = os.environ["S3_BUCKET"]

    for prefix, table, pk_cols in GOLD_TABLES:
        try:
            df = wr.s3.read_parquet(
                path=f"s3://{bucket}/{prefix}",
                dataset=True,
            )
            if df.empty:
                print(f"{table}: no data, skipping")
                continue
            upsert(conn, df, table, pk_cols)
            print(f"{table}: upserted {len(df)} rows")
        except Exception as exc:
            print(f"{table}: ERROR — {exc}")

    conn.close()
