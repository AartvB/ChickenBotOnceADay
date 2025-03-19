import sqlite3
import pandas as pd
import pytz
from datetime import datetime, timedelta

user = "username"
tz_name = "timezone_name"

conn = sqlite3.connect("reddit_posts.db")
df = pd.read_sql("SELECT * FROM posts WHERE username = ?", conn, params=(user,))
conn.close()

df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', utc=True)

# Convert timestamp to the specific timezone
df["local_time"] = df["timestamp"].dt.tz_convert(pytz.timezone(tz_name))
df["post_date"] = df["local_time"].dt.date  # Extract date part

# Sort posts
df = df.sort_values("post_date", ascending = False)

print("Posts of user", user)
print("")

for index, row in df.iterrows():
    print(row)
print("")

for index, row in df.iterrows():
    print(row['local_time'])
print("")

for index, row in df.iterrows():
    print(row['post_date'])
print("")