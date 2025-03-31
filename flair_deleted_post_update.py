import praw
import sqlite3
import pandas as pd
import schedule
import time
from shared_functions import find_streaks, update_flair, send_email, update_target_post

reddit = praw.Reddit('bot1')
reddit.validate_on_submit = True

def extra_streak_check():
    find_streaks()
    update_flair()

def check_for_deleted_posts():
    print("Checking for deleted posts")
    conn = sqlite3.connect("chicken_bot.db")
    current_time = int(time.time())    
    df = pd.read_sql_query("SELECT * FROM chicken_posts WHERE timestamp >= ?", conn, params=(current_time-600,))

    cursor = conn.cursor()
    for _, row in df.iterrows():
        post_id = row['id']
        user = row['username']
        submission = reddit.submission(id=post_id)

        print(f"Checking post {submission.title}")
        if submission.selftext == "[deleted]" or submission.author is None:
            print("Post has been deleted!")
            send_email('User removed post', f'{user} deleted post {submission.title}. You can find the post here: https://www.reddit.com/{submission.permalink}.')
            try:
                cursor.execute('''
                    INSERT INTO deleted_posts (id, username, timestamp)
                    VALUES (?, ?, ?)
                ''', (post_id, user, submission.created_utc))
                conn.commit()
                cursor.execute("DELETE FROM chicken_posts WHERE id = ?",
                            (submission.id,))
                conn.commit()

                update_target_post()
                find_streaks(user)
                update_flair(user)
            except Exception as e:
                send_email('Error in handling post deletion',f'An error occuered when I tried to handle the post deletion. Error message:\n{e}')

    conn.close()

schedule.every(1).minute.do(check_for_deleted_posts)
schedule.every(1).hour.do(extra_streak_check)

while True:
    schedule.run_pending()
    time.sleep(1)