#!pip install praw
#!pip install db-sqlite3

import praw
import sqlite3
import pandas as pd
import pytz
import schedule
import time
from datetime import datetime, timezone, timedelta

reddit = praw.Reddit('bot1')
reddit.validate_on_submit = True

def find_streaks():
    print("Checking using flair")
    
    # Connect to the database
    conn = sqlite3.connect("reddit_posts.db")
    cursor = conn.cursor()
    
    # Fetch unique users
    cursor.execute("SELECT DISTINCT username FROM posts")
    users = [row[0] for row in cursor.fetchall()]
    
    conn.close()

    
    # Get the current time in UTC
    now = datetime.utcnow()
    
    # Create a dictionary mapping UTC offsets to a representative timezone
    timezones = {
        pytz.timezone(tz).utcoffset(now): tz
        for tz in pytz.common_timezones
    }
    
    # Extract a sorted list of unique timezone names
    timezones = sorted(timezones.values())

    
    longest_streaks = {}
    
    user_no = 1

    # Process each user separately
    for user in users:
#        if user != "-MegaMan401-": # TODO!
#            continue
            
        if user_no % 20 == 0:
            print(f"user {user_no} out of {len(users)}")
        user_no += 1
        
        conn = sqlite3.connect("reddit_posts.db")  # Reconnect for each user
        query = "SELECT timestamp FROM posts WHERE username = ?"
        df = pd.read_sql(query, conn, params=(user,))
        conn.close()

        if df.empty:
            continue

        conn = sqlite3.connect("COAD_streaks.db")  # Reconnect for each user
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM user_posts WHERE username = ?", (user,)) ## TODO: UPDATE TABLE NAME!
        
        COAD_streak = cursor.fetchone()

        if COAD_streak:
            subreddit = reddit.subreddit("CountOnceADay")
            last_COAD_post = reddit.submission(id=COAD_streak['post_id'])
            last_COAD_timestamp = last_COAD_post.created_utc
            COAD_streak_number = COAD_streak['streak']
        else:
            last_COAD_timestamp = 0
            COAD_streak_number = 0

        conn.close()

        # Ensure timestamp is datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', utc=True)
        last_COAD_datetime = datetime.fromtimestamp(last_COAD_timestamp, tz=timezone.utc)

        max_streak = 0  # Track the longest streak across all timezones
        
        for tz_name in timezones:
            tz = pytz.timezone(tz_name)

#            if pytz.timezone(tz_name).utcoffset(now) == pytz.timezone('America/Argentina/Buenos_Aires').utcoffset(now): # TODO: These 4 lines
#                print("same timezone")
#            else:
#                continue
    
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

#            print('last_COAD_date') # TODO: These 2 lines
#            print(last_COAD_date)
            for date in df["post_date"]:
#                print('Current date') # TODO: These 2 lines
#                print(date)
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
    conn = sqlite3.connect('current_streaks.db')
    cursor = conn.cursor()
    
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
#find_streaks()

def update_flair():
    # Update user flair
    print("Updating user flair")
    
    # SQLite Database setup
    db_file = 'current_streaks.db'
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
        if user_no % 20 == 0:
            print(f"user {user_no} out of {len(users_data)}")
        user_no += 1
        
        reddit_username, user_streak = user_data
        user_flair = "Current streak: " + str(user_streak)
        try:
            # Check if the user exists in the subreddit
            user = reddit.redditor(reddit_username)
    
            # Set the user's flair
            subreddit.flair.set(user, text=user_flair)
#            print(f"Flair for {reddit_username} set to {user_flair}")
        except Exception as e:
            print(f"Failed to set flair for {reddit_username}: {e}")
    
    # Close the SQLite connection
    conn.close()

    print("Updated all user flair")

# Run bot
reddit = praw.Reddit('bot1')
reddit.validate_on_submit = True

# Configurable variables
POST_LIMIT = 5  # Max number of recent posts to keep in the update

def update_target_post():
    current_count = 0

    """Fetch recent posts and update the target post."""
    subreddit = reddit.subreddit("countwithchickenlady")
    target_post = reddit.submission(id='1iulihu')

    print("New check")
    new_check = True
    for submission in reversed(list(subreddit.new(limit=POST_LIMIT))):
        print(f"Checking post {submission.title}")
        if submission.title.isnumeric():
            post_number = int(submission.title)

            if post_number == current_count + 1 or new_check:
                current_count = post_number
                text = f"The next number should be: [{post_number + 1}](https://www.reddit.com/r/countwithchickenlady/submit?title={post_number + 1})\n\nNote that this post is made by a bot, and can make mistakes. In that case, sort by 'New' and check what number should be next."
                target_post.edit(text)
        
                # Add new post to database
                conn = sqlite3.connect("reddit_posts.db")
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO posts (id, username, timestamp, approved)
                    VALUES (?, ?, ?, 1)
                ''', (submission.id, submission.author.name if submission.author else "[deleted]", submission.created_utc))
                conn.commit()
                conn.close()
                  
                new_check = False
        
            else:
                conn = sqlite3.connect('reddit_posts.db')
                cursor = conn.cursor()
                
                # Fetch data from the SQLite database (modify your query as needed)
                cursor.execute("SELECT approved FROM posts WHERE id = ?;", (submission.id,))
                approved = cursor.fetchall()

                if not approved:
                    print(f"Invalid post detected: {submission.title}")
            
                    # Leave a comment explaining the removal
                    comment_text = (
                        f"This post has been removed because the correct next number was {current_count + 1}, but this post is {post_number}.\nPlease check the most recent number before posting.\nIt might be possible that someone else simply was slightly faster with their post.\nFeel free to post again with the correct new number."
                    )
                    submission.reply(comment_text)
            
                    # Remove the incorrect post
                    submission.mod.remove()
                else:
                    current_count = post_number
                conn.close()
        else:
            print(f"Non-numeric post detected: {submission.title}")

            # Leave a comment explaining the removal
            comment_text = "This post has been removed because the title must be a number. Please only post the next number in sequence."
            submission.reply(comment_text)

            # Remove the incorrect post
            submission.mod.remove()

# Runs the bot, responding to new posts immediately.
subreddit = reddit.subreddit("countwithchickenlady")

update_target_post()
find_streaks()
update_flair()
update_target_post()

# Use the streaming method to continuously check for new submissions
for submission in subreddit.stream.submissions(skip_existing=True):
    try:
        update_target_post()
        find_streaks()
        update_flair()
        update_target_post()
    except Exception as e:
        print(f"Error: {e}")
        continue  # Keep running if an error occurs