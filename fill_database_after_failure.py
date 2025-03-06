## Make/fill a database filled with all the posts

import praw
import sqlite3

reddit = praw.Reddit('bot1')
reddit.validate_on_submit = True

# Database setup
conn = sqlite3.connect("reddit_posts.db")
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS posts (
        id TEXT PRIMARY KEY,
        username TEXT,
        timestamp INTEGER
    )
''')
conn.commit()

# Function to fetch posts from a subreddit
subreddit = reddit.subreddit("countwithchickenlady")
for post in subreddit.new(limit=1000):  # Fetches the newest posts
    cursor.execute('''
        INSERT OR IGNORE INTO posts (id, username, timestamp, approved)
        VALUES (?, ?, ?, 1)
    ''', (post.id, post.author.name if post.author else "[deleted]", post.created_utc))
    conn.commit()

# Close the database connection
conn.close()