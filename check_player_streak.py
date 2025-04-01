import sqlite3
import pandas as pd
import pytz

user = input("The user is named: ")
tz_name = input("The timezone is named: ")

conn = sqlite3.connect("chicken_bot.db")
posts = pd.read_sql("SELECT * FROM chicken_posts WHERE username = ?", conn, params=(user,))
deleted_posts = pd.read_sql("SELECT * FROM deleted_posts WHERE username = ?", conn, params=(user,))
cursor = conn.cursor()
cursor.execute("SELECT streak FROM user_streaks WHERE username = ?", (user,))
streak = cursor.fetchone()[0]
conn.close()

posts["timestamp"] = pd.to_datetime(posts["timestamp"], unit='s', utc=True)
deleted_posts["timestamp"] = pd.to_datetime(deleted_posts["timestamp"], unit='s', utc=True)

# Convert timestamp to the specific timezone
posts["local_time"] = posts["timestamp"].dt.tz_convert(pytz.timezone(tz_name))
deleted_posts["local_time"] = deleted_posts["timestamp"].dt.tz_convert(pytz.timezone(tz_name))

# Sort posts
posts = posts.sort_values("local_time", ascending = False)
deleted_posts = deleted_posts.sort_values("local_time", ascending = False)

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

print(f"User {user} has the following streak: {streak}")