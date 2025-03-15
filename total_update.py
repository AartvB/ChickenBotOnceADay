#!pip install praw
#!pip install db-sqlite3

import praw
import sqlite3
import pandas as pd
import pytz
import time
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

def find_streaks():
    print("Checking using flair")
    
    # Connect to the database
    conn = sqlite3.connect("reddit_posts.db")
    cursor = conn.cursor()
    
    # Fetch unique users
    cursor.execute("SELECT DISTINCT username FROM posts")
    users = [row[0] for row in cursor.fetchall()]
    
    conn.close()

    # Get the current time
    now = datetime.now()
    
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
        if reddit_username == "chickenbotonceaday":
            user_flair = "Current streak: 3.1415926535"

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
                text = f"The next number should be: [{post_number + 1}](https://www.reddit.com/r/countwithchickenlady/submit?title={post_number + 1})\n\n^(This comment is automatically updated by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"
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
                conn.close()
        else:
            print(f"Non-numeric post detected: {submission.title}")

            send_email('Removed post', f'I removed post {submission.title} because it did not use a number. You can find the post here: https://www.reddit.com/{submission.permalink}.')

            # Leave a comment explaining the removal
            comment_text = "This post has been removed because the title must be a number. Please only post the next number in sequence.\n\n^(This action was performed automatically by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"
            submission.reply(comment_text)

            # Remove the incorrect post
            submission.mod.remove()

# Runs the bot, responding to new posts immediately.
subreddit = reddit.subreddit("countwithchickenlady")

update_target_post()
find_streaks()
update_flair()
update_target_post()

n_errors = {'execution': {'first_error':0,'n':0,'last_message':'','first_ever_error':0}, 'stream': {'first_error':0,'n':0,'last_message':'','first_ever_error':0}}

# Use the streaming method to continuously check for new submissions
while True:
    try:
        for submission in subreddit.stream.submissions(skip_existing=True):
            if n_errors['stream']['n'] > 0:
                utc_time = datetime.fromtimestamp(n_errors['stream']['first_ever_error'])
                local_time = utc_time.astimezone(pytz.timezone('Europe/Amsterdam'))
                time_first_error = local_time.strftime('%Y-%m-%d %H:%M:%S %Z')
        
                send_email('Stream error solved',f'{n_errors['stream']['n']-1} additional stream errors have happened, but an execution has started without any errors now, so the stream error has been solved. The first error occured at {time_first_error}. Latest error message:\n\n{n_errors['stream']['last_message']}')
                n_errors['stream']['first_error'] = 0
                n_errors['stream']['n'] = 0

            try:
                update_target_post()
                find_streaks()
                update_flair()
                update_target_post()

                if n_errors['execution']['n'] > 0:
                    utc_time = datetime.fromtimestamp(n_errors['execution']['first_ever_error'])
                    local_time = utc_time.astimezone(pytz.timezone('Europe/Amsterdam'))
                    time_first_error = local_time.strftime('%Y-%m-%d %H:%M:%S %Z')

                    send_email('Execution error solved',f'{n_errors['execution']['n']-1} additional execution errors have happened, but an execution has happened without any errors now, so the error has been solved. The first error occured at {time_first_error}. Latest error message:\n\n{n_errors['execution']['last_message']}')
                    n_errors['execution']['first_error'] = 0
                    n_errors['execution']['n'] = 0

            except Exception as e:
                error_count = n_errors['execution']['n']
                n_errors['execution']['last_message'] = e
                if error_count == 0:
                    send_email('Execution error',f'An execution error occurred. Error message:\n{e}')
                    n_errors['execution']['first_ever_error'] = time.time()
                    n_errors['execution']['first_error'] = time.time()
                    n_errors['execution']['n'] = 1
                elif n_errors['execution']['first_error'] < time.time() - 5*60: # Send explanation email every 5 minutes
                    send_email('Execution error',f'Multiple {error_count} execution errors have occurred since last message, and they are still not solved. Latest error message:\n\n{e}')
                    n_errors['execution']['first_error'] = time.time()
                    n_errors['execution']['n'] = 1
                else:
                    n_errors['execution']['n'] += 1

                print(f"Error in execution: {e}")
                continue  # Keep running if an error occurs
    except Exception as e:
        error_count = n_errors['stream']['n']
        n_errors['stream']['last_message'] = e
        if error_count == 0:
            send_email('Stream error',f'A stream error occurred. Error message:\n{e}')
            n_errors['stream']['first_ever_error'] = time.time()
            n_errors['stream']['first_error'] = time.time()
            n_errors['stream']['n'] = 1
        elif n_errors['stream']['first_error'] < time.time() - 5*60: # Send explanation email every 5 minutes
            send_email('Stream error',f'Multiple {error_count} stream errors have occurred since last message, and they are still not solved. Latest error message:\n\n{e}')
            n_errors['stream']['first_error'] = time.time()
            n_errors['stream']['n'] = 1
        else:
            n_errors['stream']['n'] += 1

        print(f"Error in submission stream: {e}")