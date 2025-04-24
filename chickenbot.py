import praw
import sqlite3
import pandas as pd
import pytz
import time
from datetime import datetime, timezone, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from dotenv import load_dotenv
import os

class ChickenBot:
    def __init__(self):
        # Setup reddit bot connection
        self.reddit = praw.Reddit('bot1')
        self.reddit.validate_on_submit = True
        self.subreddit = self.reddit.subreddit("countwithchickenlady")

        # Setup email connection
        load_dotenv()
        self.email_account = os.getenv('ACCOUNT')
        self.email_app_password = os.getenv('PASSWORD')
        self.email_receiver = os.getenv('RECEIVER')

        # Setup database connection
        self.__connection_is_open = False

    def __del__(self):
        self.close_connection()

    def connection_is_open(self):
        return self.__connection_is_open
    
    def open_connection(self):
        if self.__connection_is_open:
            return
        self.__connection_is_open = True
        self.__conn = sqlite3.connect("chicken_bot.db")
        self.__conn.row_factory = sqlite3.Row
        self.__cursor = self.__conn.cursor()

    def handle_connection(self, keep_open = False):
        if not keep_open:
            self.close_connection()

    def close_connection(self):
        if self.connection_is_open():
            self.__conn.close()
            self.__connection_is_open = False
    
    def conn(self):
        self.open_connection()
        return self.__conn

    def cursor(self):
        self.open_connection()
        return self.__cursor
        
    def setup_database(self, keep_open = False):
        self.cursor().execute('''
            CREATE TABLE IF NOT EXISTS chicken_posts (
                    id TEXT PRIMARY KEY,
                    username TEXT,
                    timestamp INTEGER,
                    approved BOOLEAN
            )
        ''')
        self.cursor().execute('''
            CREATE TABLE IF NOT EXISTS  COAD_posts (
                username TEXT PRIMARY KEY,
                post_id TEXT,
                streak INTEGER
            )
        ''')
        self.cursor().execute('''
            CREATE TABLE IF NOT EXISTS  user_streaks (
                    timestamp INTEGER,
                    username TEXT  PRIMARY KEY,
                    streak INTEGER,
                    COAD_streak INTEGER
                )
        ''')
        self.cursor().execute('''
            CREATE TABLE IF NOT EXISTS  deleted_posts (
                id TEXT PRIMARY KEY,
                username TEXT,
                timestamp INTEGER
            )
        ''')
        self.conn().commit()
        self.handle_connection(keep_open)

    def fill_database_after_failure(self, keep_open = False):
        for post in self.subreddit.new(limit=1000):  # Fetches the newest posts
            self.cursor().execute('''
                INSERT OR IGNORE INTO chicken_posts (id, username, timestamp, approved)
                VALUES (?, ?, ?, 1)
            ''', (post.id, self.get_author(post), post.created_utc))
        self.conn().commit()
        self.handle_connection(keep_open)

    def send_email(self, subject, body):
        # Create email message
        msg = MIMEMultipart()
        msg["From"] = self.email_account
        msg["To"] = self.email_receiver
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        # Connect to Gmail SMTP server and send email
        try:
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(self.email_account, self.email_app_password)
            server.sendmail(self.email_account, self.email_receiver, msg.as_string())
            server.quit()
            print("Email sent successfully!")
        except Exception as e:
            print(f"Error sending email: {e}")

    def get_all_posts(self, username, keep_open = False):
        posts = pd.read_sql("SELECT * FROM chicken_posts WHERE username = ?", self.conn(), params=(username,))
        deleted_posts = pd.read_sql("SELECT * FROM deleted_posts WHERE username = ?", self.conn(), params=(username,))
        self.handle_connection(keep_open)
        return posts, deleted_posts

    def get_all_users(self, keep_open = False):
        self.cursor().execute("SELECT DISTINCT username FROM chicken_posts")
        users = [row["username"] for row in self.cursor().fetchall()]
        self.handle_connection(keep_open)
        return users

    def is_user(self, username, keep_open = False):
        return username in self.get_all_users(keep_open = keep_open)
    
    def calculate_streak(self, username, keep_open = False):
        df = pd.read_sql("SELECT timestamp FROM chicken_posts WHERE username = ?", self.conn(), params=(username,))
        if df.empty:
            self.handle_connection(keep_open)
            return 0

        self.cursor().execute(f"SELECT * FROM COAD_posts WHERE username = ?", (username,))
        
        COAD_streak_info = self.cursor().fetchone()

        if COAD_streak_info:
            last_COAD_timestamp = self.reddit.submission(id=COAD_streak_info['post_id']).created_utc
            COAD_streak_number = COAD_streak_info['streak']
            has_COAD_streak = True
        else:
            has_COAD_streak = False

        # Ensure timestamp is datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', utc=True)
        if has_COAD_streak:
            last_COAD_datetime = datetime.fromtimestamp(last_COAD_timestamp, tz=timezone.utc)

        max_streak = 0 # Track the longest streak across all timezones
        max_COAD_streak = 0 # Track the longest COAD streak across all timezones
        
        for tz_name in pytz.common_timezones:
            # TODO: Stop loop if it is clear that it won't get better. For example, the last post was more than 48 hours ago.
            tz = pytz.timezone(tz_name)
    
            # Convert timestamp to the specific timezone
            df["local_time"] = df["timestamp"].dt.tz_convert(tz)
            df["post_date"] = df["local_time"].dt.date  # Extract date part
            df = df.sort_values("post_date", ascending = False)

            if has_COAD_streak:
                last_COAD_date = last_COAD_datetime.astimezone(tz).date()
    
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
                    if has_COAD_streak and last_COAD_date == today:
                        COAD_streak = COAD_streak_number
                elif date == yesterday and last_date is None:
                    streak = 1
                    last_date = date
                    if has_COAD_streak and (last_COAD_date == today or last_COAD_date == yesterday):
                        COAD_streak = COAD_streak_number
                elif last_date is not None:
                    if date == last_date - timedelta(days=1):
                        if has_COAD_streak and date == last_COAD_date:
                            COAD_streak = COAD_streak_number + streak
                        streak += 1
                        last_date = date
                    else:
                        break
                else: # Last post was earlier than today or yesterday
                    streak = 0
                    break
            max_streak = max(max_streak, streak)
            if has_COAD_streak:
                max_COAD_streak = max(max_COAD_streak, COAD_streak)
        self.handle_connection(keep_open)
        return max_streak, max_COAD_streak

    def calculate_all_streaks(self, keep_open = False):
        print("Calculating user streaks")
        
        users = self.get_all_users(keep_open=True)
        streaks = {}

        for user_no, user in enumerate(users):                        
            if (user_no+1) % 20 == 0:
                print(f"user {user_no+1} out of {len(users)}")
            streaks[user] = {}
            streaks[user]['normal'], streaks[user]['COAD'] = self.calculate_streak(user, keep_open=True)
        print("Finished calculating user streaks")

        self.handle_connection(keep_open)
        return streaks

    def record_streak(self, username, keep_open = False):
        self.cursor().execute("""
            INSERT INTO user_streaks (timestamp, username, streak, COAD_streak)
            VALUES (CURRENT_TIMESTAMP, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
            timestamp = CURRENT_TIMESTAMP,
            streak = excluded.streak,
            COAD_streak = excluded.COAD_streak
        """, (username, *self.calculate_streak(username, keep_open=True)))
        self.conn().commit()
        self.handle_connection(keep_open)

    def record_all_streaks(self, keep_open = False):
        print("Recording user streaks")
        for user, streaks in self.calculate_all_streaks(keep_open=True).items():
            self.cursor().execute("""
                INSERT INTO user_streaks (timestamp, username, streak, COAD_streak)
                VALUES (CURRENT_TIMESTAMP, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                timestamp = CURRENT_TIMESTAMP,
                streak = excluded.streak,
                COAD_streak = excluded.COAD_streak
            """, (user, streaks['normal'], streaks['COAD']))
        
        self.conn().commit()
        self.handle_connection(keep_open)

        print("Finished recording user streaks")

    def update_user_flair(self, username, keep_open = False):
        if not self.is_user(username,keep_open=True):
            self.handle_connection(keep_open)
            raise ValueError(f"u/{username} is not a known subreddit user.")
        self.cursor().execute("SELECT streak, COAD_streak FROM user_streaks WHERE username = ?", (username,))
        streak = max(self.cursor().fetchone())

        user_flair = "Current streak: " + str(streak)
        if username == "chickenbotonceaday":
            user_flair = "Current streak: 3.1415926535"

        try:
            # Check if the user exists in the subreddit
            user = self.reddit.redditor(username)

            # Get current flair
            flair_generator = self.subreddit.flair(username)
            current_flair = next(flair_generator, None)['flair_text']

            # Set the user's flair
            if current_flair != user_flair:
                self.subreddit.flair.set(user, text=user_flair)
        except Exception as e:
            print(f"Failed to set flair for {username}: {e}")
        self.handle_connection(keep_open)

    def update_all_flair(self, keep_open = False):
        # Update user flair
        print("Updating user flairs")

        users = self.get_all_users(keep_open=True)

        for user_no, user in enumerate(users):                        
            if (user_no+1) % 20 == 0:
                print(f"user {user_no+1} out of {len(users)}")
            self.update_user_flair(user, keep_open=True)

        self.handle_connection(keep_open)
        print("Finished updating user flairs")

    def get_author(self, submission):
        return submission.author.name if submission.author else "[deleted]"

    def update_target_post(self, post_limit=5, keep_open = False):
        current_count = 0

        target_post = self.reddit.submission(id='1iulihu')

        print("New check")
        for submission in reversed(list(self.subreddit.new(limit=post_limit))):
            print(f"Checking post {submission.title}")
            if submission.title.isnumeric():
                post_number = int(submission.title)
                if post_number == current_count + 1 or current_count == 0:
                    current_count = post_number
                    self.cursor().execute('INSERT OR IGNORE INTO chicken_posts (id, username, timestamp, approved) VALUES (?, ?, ?, 1)',
                                        (submission.id, self.get_author(submission), submission.created_utc))
                    self.conn().commit()

                    self.record_streak(self.get_author(submission),keep_open=True)
                    self.update_user_flair(self.get_author(submission),keep_open=True)
                else:                
                    self.cursor().execute("SELECT approved FROM chicken_posts WHERE id = ?;", (submission.id,))
                    approved = self.cursor().fetchall()
                    
                    if not approved and submission.approved_by is None:
                        print(f"Invalid post detected: {submission.title}")

                        self.send_email('Removed post', f'I removed post {submission.title} by {self.get_author(submission)} because it did not use the correct number. You can find the post here: https://www.reddit.com/{submission.permalink}.')

                        # Leave a comment explaining the removal
                        comment_text = (
                            f"This post has been removed because the correct next number was {current_count + 1}, but this post is {post_number}. Please check the most recent number before posting.\n\nIt might be possible that someone else simply was slightly faster with their post.\n\nFeel free to post again with the correct new number.\n\n^(This action was performed automatically by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"
                        ) # TODO: ADD MORE VARIATION, FOR EXAMPLE WHEN IT IS ONLY 1 BELOW.
                        submission.reply(comment_text)
                
                        # Remove the incorrect post
                        submission.mod.remove()

                    else:
                        current_count = max(post_number, current_count)
                        
                        # Add new post to database
                        self.cursor().execute('INSERT OR IGNORE INTO chicken_posts (id, username, timestamp, approved) VALUES (?, ?, ?, 1)',
                                            (submission.id, self.get_author(submission), submission.created_utc))
                        self.conn().commit()

                        self.record_streak(self.get_author(submission),keep_open=True)
                        self.update_user_flair(self.get_author(submission),keep_open=True)
            else:
                print(f"Non-numeric post detected: {submission.title}")

                self.send_email('Removed post', f'I removed post {submission.title} by {self.get_author(submission)} because it did not use a number. You can find the post here: https://www.reddit.com/{submission.permalink}.')

                # Leave a comment explaining the removal
                comment_text = "This post has been removed because the title must be a number. Please only post the next number in sequence.\n\n^(This action was performed automatically by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"
                submission.reply(comment_text)

                # Remove the incorrect post
                submission.mod.remove()

        target_post.edit(f"The next number should be: [{current_count + 1}](https://www.reddit.com/r/countwithchickenlady/submit?title={current_count + 1})\n\n^(This comment is automatically updated by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)")
        self.handle_connection(keep_open)

    def add_COAD_streak(self, keep_open = False):
        print("Enter the details of the COAD streak to be added below.")
        username = input("Username: ")
        post_id = input("Post_id of the latest COAD post: ")
        streak_no = int(input("Streak number: "))

        self.cursor().execute("DELETE FROM COAD_posts WHERE username = ?", (username,)) 
        self.conn().commit()

        self.cursor().execute("INSERT INTO COAD_posts (username, post_id, streak) VALUES (?, ?, ?)", (username,post_id,streak_no,))
        self.conn().commit()
        self.handle_connection(keep_open)

        print(f"The COAD streak of {username} has been updated succesfully.")

    def detect_close_posts(self, username, keep_open = False):
        posts, deleted_posts = self.get_all_posts(username, keep_open=True)

        posts["datetime"] = pd.to_datetime(posts["timestamp"], unit='s', utc=True)
        deleted_posts["datetime"] = pd.to_datetime(deleted_posts["timestamp"], unit='s', utc=True)

        posts['time_diff'] = posts['datetime'].diff().dt.total_seconds()/60*-1
        close_posts = posts[posts['time_diff'] < 10]

        self.handle_connection(keep_open)
        return close_posts

    def report_close_posts(self, username, keep_open = False):
        close_posts = self.detect_close_posts(username, keep_open=True)
        if len(close_posts) > 0:
            result = f"User {username}.\n"
            result += f"Close dates:\n{close_posts[['local_time','id']]}\n"
            result += f"Posts of user {username}:\n"

            posts = self.get_all_posts(username, keep_open=True)[0]
            posts["local_time"] = pd.to_datetime(posts["timestamp"], unit='s', utc=True)
            posts = posts.sort_values("local_time", ascending = False).copy()

            for index, row in posts.iterrows():
                result += f"Date/time: {row['local_time']}, post id: {row['id']}\n"

            self.cursor().execute("SELECT streak, COAD_streak FROM user_streaks WHERE username = ?", (username,))

            result += f"User {username} has the following streak: {max(self.cursor().fetchone())}\n"

            result += posts['time_diff']
        else:
            result = f"{username} does not have any close posts."
        self.handle_connection(keep_open)
        return result

    def detect_all_close_posts(self, users_to_skip = [], keep_open = False):
        close_posts = {}
        for i, user in enumerate(self.get_all_users()):
            if user in users_to_skip:
                continue
            close_posts[user] = self.detect_close_posts(user,keep_open=True)
        self.handle_connection(keep_open)
        return close_posts
    
    def report_all_close_posts(self, users_to_skip = [], keep_open = False):
        results = {}
        for i, user in enumerate(self.get_all_users()):
            if user in users_to_skip:
                continue
            results[user] = self.report_close_posts(user,keep_open=True)
        self.handle_connection(keep_open)
        return results

    def check_player_streak(self, keep_open = False):
        print("Enter the details of the player for whom you want to investigate the streak below.")

        username = input("The user is named: ")
        tz_name = input("The timezone is named: ")

        posts, deleted_posts = self.get_all_posts(username, keep_open=True)
        self.cursor().execute("SELECT streak, COAD_streak FROM user_streaks WHERE username = ?", (username,))
        streak = max(self.cursor().fetchone())

        posts["datetime"] = pd.to_datetime(posts["timestamp"], unit='s', utc=True)
        posts["local_time"] = posts["datetime"].dt.tz_convert(pytz.timezone(tz_name))
        posts = posts.sort_values("local_time", ascending = False)

        deleted_posts["datetime"] = pd.to_datetime(deleted_posts["timestamp"], unit='s', utc=True)
        deleted_posts["local_time"] = deleted_posts["datetime"].dt.tz_convert(pytz.timezone(tz_name))
        deleted_posts = deleted_posts.sort_values("local_time", ascending = False)

        print(f"Posts of user {username}:\n")
        for index, row in posts.iterrows():
            print(f"Date/time: {row['local_time']}, post id: {row['id']}")
        print("")
        if len(deleted_posts) > 0:
            print(f"Deleted posts of user {username}:\n")
            for index, row in deleted_posts.iterrows():
                print(f"Date/time: {row['local_time']}, post id: {row['id']}")
        else:
            print(f"{username} had no deleted posts.")

        print(f"User {username} has the following streak: {streak}")

        self.handle_connection(keep_open)

    def delete_post(self, keep_open = False):
        post_id = input("The post to delete has id: ")

        self.cursor().execute("SELECT * FROM chicken_posts WHERE id = ?", (post_id,))
        result = self.cursor().fetchone()

        self.cursor().execute("INSERT INTO deleted_posts (id, username, timestamp) VALUES (?, ?, ?)", (post_id, result[1], result[2]))
        self.cursor().execute("DELETE FROM chicken_posts WHERE id = ?",(post_id,))
        self.conn().commit()

        self.handle_connection(keep_open)
        print(f"Post deleted succesfully: {result}")

    def run_sql(self, query, keep_open = False):
        self.cursor().execute(query)
        self.conn().commit()
        self.handle_connection(keep_open)

    def check_for_deleted_posts(self, keep_open=False):
        print("Checking for deleted posts")

        current_time = int(time.time())
        df = pd.read_sql_query("SELECT * FROM chicken_posts WHERE timestamp >= ?", self.conn(), params=(current_time-600,))
        for _, row in df.iterrows():
            post_id = row['id']
            user = row['username']
            submission = self.reddit.submission(id=post_id)

            print(f"Checking post {submission.title}")
            if submission.selftext == "[deleted]" or submission.author is None:
                print("Post has been deleted!")
                self.send_email('User removed post', f'{user} deleted post {submission.title}. You can find the post here: https://www.reddit.com/{submission.permalink}.')
                try:
                    self.cursor().execute("INSERT INTO deleted_posts (id, username, timestamp) VALUES (?, ?, ?)",
                                          (post_id, user, submission.created_utc))
                    self.conn().commit()
                    self.cursor().execute("DELETE FROM chicken_posts WHERE id = ?", (submission.id,))
                    self.conn().commit()

                    self.update_target_post(keep_open=True)
                    self.record_streak(user,keep_open=True)
                    self.update_user_flair(user, keep_open=True)
                except Exception as e:
                    self.send_email('Error in handling post deletion',f'An error occuered when I tried to handle the post deletion. Error message:\n{e}')

        self.handle_connection(keep_open)
    
    def start_maintenance(self):
        self.reddit.submission(id='1iulihu').edit(f"The bot is currently under maintenance. Our apologies for the inconvenience. Please [sort by new](https://www.reddit.com/r/countwithchickenlady/new/) to see what the next number in the sequence should be, and use this number as the title for your new post.\n\n^(If you think the bot made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)")

    def end_maintenance(self):
        self.update_target_post(post_limit=20)