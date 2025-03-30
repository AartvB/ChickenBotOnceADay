import sqlite3

conn = sqlite3.connect("chicken_bot.db")
cursor = conn.cursor()
query = "DELETE FROM chicken_posts WHERE id = 'aaaaaaa'"
cursor.execute(query)
conn.commit()
conn.close()
