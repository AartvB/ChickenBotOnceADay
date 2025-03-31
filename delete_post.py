import sqlite3

post_id = input("The post to delete has id: ")

conn = sqlite3.connect("chicken_bot.db")
cursor = conn.cursor()
query = "DELETE FROM chicken_posts WHERE id = ?"
cursor.execute(query,(post_id,))
conn.commit()
conn.close()
