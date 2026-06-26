import pandas as pd

# print("TOP X USERS BY FOLLOWERS")
# df = pd.read_parquet("C:/Users/Korisnik/Downloads/top_x_users.parquet")
# print(df)
#
# print("\nTOP HN USERS BY KARMA")
# df = pd.read_parquet("C:/Users/Korisnik/Downloads/top_hn_users_karma.parquet")
# print(df)
#
# print("\nTOP HN POSTS BY SCORE")
# df = pd.read_parquet("C:/Users/Korisnik/Downloads/top_hn_posts.parquet")
# print(df)
#
# print("\nTOP HN JOBS BY SCORE")
# df = pd.read_parquet("C:/Users/Korisnik/Downloads/top_hn_jobs.parquet")
# print(df)

print("HN users")
df = pd.read_parquet("C:/Users/Korisnik/Downloads/dataHN.parquet")
print(df)

print("\nX users")
df = pd.read_parquet("C:/Users/Korisnik/Downloads/dataX.parquet")
print(df)