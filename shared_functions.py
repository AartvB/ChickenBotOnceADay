import praw
import sqlite3
import pandas as pd
import pytz
from datetime import datetime, timezone, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email_info import email_data

reddit = praw.Reddit('bot1')
reddit.validate_on_submit = True

def send_email(subject, body):
    user = email_data()['account']
    password = email_data()['password']
    to_email = email_data()['receiver']

    # Create email message
    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    # Connect to Gmail SMTP server and send email
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(user, password)
        server.sendmail(user, to_email, msg.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error sending email: {e}")

def find_streaks(username = None):
    print("Checking using flair")
    
    # Connect to the database
    conn = sqlite3.connect("chicken_bot.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Fetch unique users
    cursor.execute("SELECT DISTINCT username FROM chicken_posts")
    users = [row["username"] for row in cursor.fetchall()]

    # Get the current time
    now = datetime.now()

    timezones = pytz.common_timezones
        
    longest_streaks = {}
    
    user_no = 1

    # Process each user separately
    for user in users:                        
        if username is not None:
            if username != user:
                continue
        elif user_no % 20 == 0:
            print(f"user {user_no} out of {len(users)}")
        user_no += 1        

        df = pd.read_sql("SELECT timestamp FROM chicken_posts WHERE username = ?", conn, params=(user,))
        if df.empty:
            continue

        cursor.execute(f"SELECT * FROM COAD_posts WHERE username = ?", (user,))
        
        COAD_streak = cursor.fetchone()

        if COAD_streak:
            subreddit = reddit.subreddit("CountOnceADay")
            last_COAD_post = reddit.submission(id=COAD_streak['post_id'])
            last_COAD_timestamp = last_COAD_post.created_utc
            COAD_streak_number = COAD_streak['streak']
        else:
            last_COAD_timestamp = 0
            COAD_streak_number = 0

        # Ensure timestamp is datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', utc=True)
        last_COAD_datetime = datetime.fromtimestamp(last_COAD_timestamp, tz=timezone.utc)

        max_streak = 0  # Track the longest streak across all timezones
        
        for tz_name in timezones:
            tz = pytz.timezone(tz_name)
    
            # Convert timestamp to the specific timezone
            df["local_time"] = df["timestamp"].dt.tz_convert(tz)
            df["post_date"] = df["local_time"].dt.date  # Extract date part

            last_COAD_date_local = last_COAD_datetime.astimezone(tz)
            last_COAD_date = last_COAD_date_local.date()            

            # Sort posts
            df = df.sort_values("post_date", ascending = False)
    
            # Find the longest current streak
            streak = 0
            COAD_streak = 0
            last_date = None
            today = datetime.now(tz).date()
            yesterday = (datetime.now(tz) - timedelta(days=1)).date()

            for date in df["post_date"]:
                if date == today and last_date is None:
                    streak = 1
                    last_date = date
                    if last_COAD_date == today:
                        COAD_streak = COAD_streak_number
                elif date == yesterday and last_date is None:
                    streak = 1
                    last_date = date
                    if last_COAD_date == today or last_COAD_date == yesterday:
                        COAD_streak = COAD_streak_number
                elif last_date is not None:
                    if date == last_date - timedelta(days=1):
                        if date == last_COAD_date:
                            COAD_streak = COAD_streak_number + streak
                        streak += 1
                        last_date = date
                    else:
                        break
                else: # Last post was earlier than today or yesterday
                    streak = 0
                    break
            max_streak = max(max_streak, streak, COAD_streak)
            
        # Store only the largest streak across all timezones for this user
        longest_streaks[user] = max_streak
            
    # Insert or update streaks in the database    
    for user, streak in longest_streaks.items():
        cursor.execute("""
            INSERT INTO user_streaks (timestamp, username, streak)
            VALUES (CURRENT_TIMESTAMP, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
            timestamp = CURRENT_TIMESTAMP,
            streak = excluded.streak
        """, (user, streak))
    
    conn.commit()
    conn.close()

    print("Finished checking using flair")

def update_flair(username = None):
    # Update user flair
    print("Updating user flair")
    
    # SQLite Database setup
    db_file = 'chicken_bot.db'
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Fetch data from the SQLite database (modify your query as needed)
    cursor.execute("SELECT username, streak FROM user_streaks")
    users_data = cursor.fetchall()
    
    # Select the subreddit where you want to change the flair
    subreddit = reddit.subreddit("countwithchickenlady")

    user_no = 1
    
    # Change user flair based on database data
    for user_data in users_data:       
        reddit_username, user_streak = user_data

        if username is not None:
            if reddit_username != username:
                continue
        elif user_no % 20 == 0:
            print(f"user {user_no} out of {len(users_data)}")
        user_no += 1
 
        user_flair = "Current streak: " + str(user_streak)
        if reddit_username == "chickenbotonceaday":
            user_flair = "Current streak: 3.1415926535"

        try:
            # Check if the user exists in the subreddit
            user = reddit.redditor(reddit_username)
    
            # Set the user's flair
            subreddit.flair.set(user, text=user_flair)
        except Exception as e:
            print(f"Failed to set flair for {reddit_username}: {e}")
    
    # Close the SQLite connection
    conn.close()

    print("Updated all user flair")

def update_target_post(post_limit=5):
    current_count = 0

    """Fetch recent posts and update the target post."""
    subreddit = reddit.subreddit("countwithchickenlady")
    target_post = reddit.submission(id='1iulihu')

    print("New check")
    new_check = True
    
    conn = sqlite3.connect("chicken_bot.db")
    cursor = conn.cursor()

    for submission in reversed(list(subreddit.new(limit=post_limit))):
        print(f"Checking post {submission.title}")
        if submission.title.isnumeric():
            post_number = int(submission.title)

            if post_number == current_count + 1 or new_check:
                current_count = post_number
                text = f"The next number should be: [{post_number + 1}](https://www.reddit.com/r/countwithchickenlady/submit?title={post_number + 1})\n\n^(This comment is automatically updated by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"
                target_post.edit(text)
        
                # Add new post to database
                cursor.execute('''
                    INSERT OR IGNORE INTO chicken_posts (id, username, timestamp, approved)
                    VALUES (?, ?, ?, 1)
                ''', (submission.id, submission.author.name if submission.author else "[deleted]", submission.created_utc))
                conn.commit()
                  
                new_check = False
        
            else:                
                # Fetch data from the SQLite database (modify your query as needed)
                cursor.execute("SELECT approved FROM chicken_posts WHERE id = ?;", (submission.id,))
                approved = cursor.fetchall()
                
                if not approved:
                    print(f"Invalid post detected: {submission.title}")

                    send_email('Removed post', f'I removed post {submission.title} because it did not use the correct number. You can find the post here: https://www.reddit.com/{submission.permalink}.')

                    # Leave a comment explaining the removal
                    comment_text = (
                        f"This post has been removed because the correct next number was {current_count + 1}, but this post is {post_number}. Please check the most recent number before posting.\n\nIt might be possible that someone else simply was slightly faster with their post.\n\nFeel free to post again with the correct new number.\n\n^(This action was performed automatically by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"
                    )
                    submission.reply(comment_text)
            
                    # Remove the incorrect post
                    submission.mod.remove()

                else:
                    current_count = post_number
        else:
            print(f"Non-numeric post detected: {submission.title}")

            send_email('Removed post', f'I removed post {submission.title} because it did not use a number. You can find the post here: https://www.reddit.com/{submission.permalink}.')

            # Leave a comment explaining the removal
            comment_text = "This post has been removed because the title must be a number. Please only post the next number in sequence.\n\n^(This action was performed automatically by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"
            submission.reply(comment_text)

            # Remove the incorrect post
            submission.mod.remove()

    conn.close()