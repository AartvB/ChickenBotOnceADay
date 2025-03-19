import sqlite3

conn = sqlite3.connect("reddit_posts.db")
cursor = conn.cursor()
query = "DELETE FROM posts WHERE id = 'aaaaaaa'"
cursor.execute(query)
conn.commit()
conn.close()
