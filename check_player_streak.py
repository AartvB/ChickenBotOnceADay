import sqlite3
import pandas as pd
import pytz
from datetime import datetime, timedelta

user = input("The user is named: ")
tz_name = input("The timezone is named: ")

conn = sqlite3.connect("chicken_bot.db")
posts = pd.read_sql("SELECT * FROM chicken_posts WHERE username = ?", conn, params=(user,))
deleted_posts = pd.read_sql("SELECT * FROM deleted_posts WHERE username = ?", conn, params=(user,))
conn.close()

posts["timestamp"] = pd.to_datetime(posts["timestamp"], unit='s', utc=True)

# Convert timestamp to the specific timezone
posts["local_time"] = posts["timestamp"].dt.tz_convert(pytz.timezone(tz_name))
posts["post_date"] = posts["local_time"].dt.date  # Extract date part

# Sort posts
posts = posts.sort_values("post_date", ascending = False)

print(f"Posts of user {user}:\n")
for index, row in posts.iterrows():
    print(f"Date/time: {row['local_time']}, post id: {row['id']}")
print("")
if len(deleted_posts) > 0:
    print(f"Deleted posts of user {user}:\n")
    for index, row in deleted_posts.iterrows():
        print(f"Date/time: {row['local_time']}, post id: {row['id']}")
else:
    print(f"{user} had no deleted posts.")