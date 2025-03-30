import sqlite3

user = input("Username: ")
post_id = input("Post_id: ")
streak_no = int(input("Streak_no: "))
replace = bool(input("Replace old value? (0/1) "))

conn = sqlite3.connect('chicken_bot.db')
cursor = conn.cursor()

if replace:
    query = "DELETE FROM COAD_posts WHERE username = ?"
    cursor.execute(query, (user,)) 
    conn.commit()

cursor.execute('''
INSERT INTO COAD_posts (username, post_id, streak)
VALUES (?, ?, ?)
''',(user,post_id,streak_no,))
conn.commit()
conn.close()

print("Database and table filled successfully.")
