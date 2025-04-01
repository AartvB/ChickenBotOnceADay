import sqlite3
import pandas as pd

player_check = input("User: ")

conn = sqlite3.connect("chicken_bot.db")
posts = pd.read_sql("SELECT * FROM chicken_posts", conn)
deleted_posts = pd.read_sql("SELECT * FROM deleted_posts", conn)
cursor = conn.cursor()

posts["timestamp"] = pd.to_datetime(posts["timestamp"], unit='s', utc=True)
deleted_posts["timestamp"] = pd.to_datetime(deleted_posts["timestamp"], unit='s', utc=True)

# Convert timestamp to the specific timezone
posts["local_time"] = posts["timestamp"]
deleted_posts["local_time"] = deleted_posts["timestamp"]

# Sort posts
posts = posts.sort_values("local_time", ascending = False)
deleted_posts = deleted_posts.sort_values("local_time", ascending = False)

for i, user in enumerate(posts['username'].unique()):
    if user in ['killmetwice1234', 'Alternative-Spare-82']:
        continue
    user_posts = posts[posts['username'] == user]
    user_deleted_posts = deleted_posts[deleted_posts['username'] == user]
    user_posts = user_posts.copy()
    user_posts['time_diff'] = user_posts['timestamp'].diff().dt.total_seconds()/60*-1
    cursor.execute("SELECT streak FROM user_streaks WHERE username = ?", (user,))
    streak = cursor.fetchone()[0]
#    if len(user_posts) > streak:
    close_posts = user_posts[user_posts['time_diff'] < 10]
    if len(close_posts) > 0 or player_check == user:
        print(f"User {i+1}/{len(posts['username'].unique())}")
        print(f"Close dates:\n{close_posts[['local_time','id']]}\n")
        print(f"Posts of user {user}:\n")
        for index, row in user_posts.iterrows():
            print(f"Date/time: {row['local_time']}, post id: {row['id']}")
        print("")
        if len(user_deleted_posts) > 0:
            print(f"Deleted posts of user {user}:\n")
            for index, row in user_deleted_posts.iterrows():
                print(f"Date/time: {row['local_time']}, post id: {row['id']}")
        else:
            print(f"{user} had no deleted posts.")

        print(f"User {user} has the following streak: {streak}")

        print(user_posts['time_diff'])
        break
conn.close()