import pandas as pd

print("DAILY CONTENT COUNTS")
daily_content_counts = pd.read_parquet("C:/Users/Korisnik/Downloads/data.parquet")
print(daily_content_counts)

print("\nDAILY USERS METRIC")
daily_users_metric = pd.read_parquet("C:/Users/Korisnik/Downloads/data (1).parquet")
print(daily_users_metric)