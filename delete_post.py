import sqlite3
import pandas as pd

post_id = input("The post to delete has id: ")

conn = sqlite3.connect("chicken_bot.db")
cursor = conn.cursor()

cursor.execute("SELECT * FROM chicken_posts WHERE id = ?", (post_id,))
result = cursor.fetchone()

cursor.execute('''
    INSERT INTO deleted_posts (id, username, timestamp)
    VALUES (?, ?, ?)
''', (post_id, result[1], result[2]))
conn.commit()

query = "DELETE FROM chicken_posts WHERE id = ?"
cursor.execute(query,(post_id,))
conn.commit()
conn.close()
print(f"Post deleted succesfully: {result}")