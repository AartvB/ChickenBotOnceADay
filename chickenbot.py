import praw
import sqlite3
import pandas as pd
import pytz
import time
from datetime import datetime, timezone, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import shutil
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
                    approved BOOLEAN,
                    title TEXT,
                    current_streak INTEGER,
                    current_COAD_streak INTEGER
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

    def backup_database(self):
        shutil.copy2('chicken_bot.db', f"chicken_bot backup {datetime.now().strftime('%Y-%m-%d %H.%M.%S')}.db")

    def fill_database_after_failure(self, keep_open = False):
        for post in self.subreddit.new(limit=1000):  # Fetches the newest posts
            self.cursor().execute('''
                INSERT OR IGNORE INTO chicken_posts (id, username, timestamp, approved, title)
                VALUES (?, ?, ?, 1, ?)
            ''', (post.id, self.get_author(post), post.created_utc, post.title))
        self.conn().commit()
        self.record_all_streaks(keep_open=True)
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
    
    def calculate_streak(self, username, timestamp = None, keep_open = False):
        if timestamp is None:
            timestamp = time.time()

        df = pd.read_sql("SELECT timestamp FROM chicken_posts WHERE username = ? AND timestamp <= ?", self.conn(), params=(username,timestamp))
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

            today_datetime = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            today = today_datetime.date()
            yesterday = (today_datetime - timedelta(days=1)).date()

            if has_COAD_streak and (last_COAD_date == today or last_COAD_date == yesterday):
                COAD_streak = COAD_streak_number

            for date in df["post_date"]:
                if (date == today or date == yesterday) and last_date is None:
                    streak = 1
                    last_date = date
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

    def record_post_streak(self, post_id, replace = True, keep_open = False):
        self.cursor().execute(f"SELECT username, timestamp, current_streak, current_COAD_streak FROM chicken_posts WHERE id = ?", (post_id,))
        username, timestamp, streak, COAD_streak = self.cursor().fetchone()
        if not replace and streak is not None and COAD_streak is not None:
            self.handle_connection(keep_open)
            return
        streak, COAD_streak = self.calculate_streak(username, timestamp=timestamp, keep_open=True)
        self.cursor().execute("UPDATE chicken_posts SET current_streak = ?, current_COAD_streak = ? WHERE id = ?", (streak, COAD_streak, post_id))
        self.conn().commit()
        self.handle_connection(keep_open)

    def record_post_streaks_user(self, username, keep_open = False):
        posts = pd.read_sql("SELECT id FROM chicken_posts WHERE username = ?", self.conn(), params=(username,))
        for i, post_id in enumerate(posts['id']):
            if (i+1) % 20 == 0:
                print("Recording post streaks for user", username, "post", i+1, "out of", len(posts))
            self.record_post_streak(post_id, keep_open=True)
        self.handle_connection(keep_open)

    def record_all_empty_post_streaks(self, keep_open = False):
        print("Recording empty post streaks")
        posts = pd.read_sql("SELECT id FROM chicken_posts WHERE current_streak IS NULL OR current_COAD_streak IS NULL", self.conn())
        for i, post_id in enumerate(posts['id']):
#            if (i+1) % 20 == 0:
            print("Recording empty post streaks", i+1, "out of", len(posts))
            self.record_post_streak(post_id, keep_open=True)
        self.handle_connection(keep_open)
        print("Finished recording empty post streaks")

    def update_user_flair(self, username, keep_open = False):
        self.cursor().execute("SELECT streak, COAD_streak FROM user_streaks WHERE username = ?", (username,))
        try:
            streak = max(self.cursor().fetchone())
        except Exception as e:
            print(f"Failed to fetch streak for {username}: {e}")
            self.handle_connection(keep_open)
            return

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
            self.cursor().execute("SELECT approved FROM chicken_posts WHERE id = ?;", (submission.id,))
            approved = self.cursor().fetchall()

            if submission.title.isnumeric():
                post_number = int(submission.title)
                if post_number == current_count + 1 or current_count == 0:
                    current_count = post_number
                    self.cursor().execute('INSERT OR IGNORE INTO chicken_posts (id, username, timestamp, approved, title) VALUES (?, ?, ?, 1, ?)',
                                        (submission.id, self.get_author(submission), submission.created_utc, submission.title))
                    self.conn().commit()

                    self.record_streak(self.get_author(submission),keep_open=True)
                    self.update_user_flair(self.get_author(submission),keep_open=True)
                    self.record_post_streak(submission.id,replace=False,keep_open=True)
                else:                                    
                    if not approved and submission.approved_by is None:
                        print(f"Invalid post detected: {submission.title}")

                        self.send_email('Removed post', f'I removed post {submission.title} by {self.get_author(submission)} because it did not use the correct number. You can find the post here: https://www.reddit.com/{submission.permalink}.')

                        # Leave a comment explaining the removal
                        comment_text = (
                            f"This post has been removed because the correct next number was {current_count + 1}, but this post has '{post_number}' as title. Please check the most recent number before posting.\n\nIt might be possible that someone else simply was slightly faster with their post.\n\nFeel free to post again with the correct new number.\n\n^(This action was performed automatically by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"
                        ) # TODO: ADD MORE VARIATION, FOR EXAMPLE WHEN IT IS ONLY 1 BELOW.
                
                        # Remove the incorrect post
                        submission.mod.remove()
                        submission.mod.send_removal_message(comment_text)

                    else:
                        current_count = max(post_number, current_count)
                        
                        # Add new post to database
                        self.cursor().execute('INSERT OR IGNORE INTO chicken_posts (id, username, timestamp, approved, title) VALUES (?, ?, ?, 1, ?)',
                                            (submission.id, self.get_author(submission), submission.created_utc, submission.title))
                        self.conn().commit()

                        self.record_streak(self.get_author(submission),keep_open=True)
                        self.update_user_flair(self.get_author(submission),keep_open=True)
            elif not approved and submission.approved_by is None:
                print(f"Non-numeric post detected: {submission.title}")

                self.send_email('Removed post', f'I removed post {submission.title} by {self.get_author(submission)} because it did not use a number. You can find the post here: https://www.reddit.com/{submission.permalink}.')

                # Leave a comment explaining the removal
                comment_text = "This post has been removed because the title must be a number. Please only post the next number in sequence.\n\n^(This action was performed automatically by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"

                # Remove the incorrect post
                submission.mod.remove()
                submission.mod.send_removal_message(comment_text)

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
        
        self.record_streak(username,keep_open=True)
        self.update_user_flair(username, keep_open=True)
        self.record_post_streaks_user(username,keep_open=True)

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

        self.cursor().execute(f"SELECT * FROM COAD_posts WHERE username = ?", (username,))        
        COAD_post_info = self.cursor().fetchone()
        if COAD_post_info:
            last_COAD_timestamp = self.reddit.submission(id=COAD_post_info['post_id']).created_utc
            last_COAD_post = datetime.fromtimestamp(last_COAD_timestamp, tz=pytz.timezone(tz_name))
            print(f"COAD post:\nDate/time: {last_COAD_post}, post id: {COAD_post_info['post_id']}, streak: {COAD_post_info['streak']}\n")

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

        self.record_post_streaks_user(result[1],keep_open=True)

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
                    self.record_post_streaks_user(user,keep_open=True)
                except Exception as e:
                    self.send_email('Error in handling post deletion',f'An error occuered when I tried to handle the post deletion. Error message:\n{e}')

        self.handle_connection(keep_open)
    
    def start_maintenance(self):
        print('Started maintenance')
        self.reddit.submission(id='1iulihu').edit(f"The bot is currently under maintenance. Our apologies for the inconvenience. Please [sort by new](https://www.reddit.com/r/countwithchickenlady/new/) to see what the next number in the sequence should be, and use this number as the title for your new post.\n\n^(If you think the bot made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)")

    def end_maintenance(self, keep_open=False):
        print('Ended maintenance')
        self.update_target_post(post_limit=20, keep_open=True)
        self.handle_connection(keep_open)

    def update_count_leaderboard(self, keep_open=False):
        print("Updating count leaderboard")

        posts = pd.read_sql("SELECT username, COUNT(*) as counts FROM chicken_posts GROUP BY username ORDER BY counts DESC LIMIT 1000", self.conn())
        posts['rank'] = posts['counts'].rank(method='min', ascending=False).astype(int)
        posts = posts[['rank', 'username', 'counts']]
        posts = posts.rename(columns={'rank':'Rank', 'username':'Username', 'counts':'Counts'})
        leaderboard = posts.to_markdown(index=False)

        wiki_text = "#All counters of our beautiful sub!\n\nThis shows the top 1000 posters of our sub!\n\n"+leaderboard

        self.subreddit.wiki['counts'].edit(wiki_text, reason = 'Hourly update')
        self.handle_connection(keep_open)

    def update_100_count_leaderboard(self, keep_open=False):
        print("Updating 100 counts leaderboard")

        posts = pd.read_sql("SELECT username, title, id, timestamp FROM chicken_posts WHERE title LIKE '%00' ORDER BY CAST(title AS UNSIGNED) DESC LIMIT 1000", self.conn()) # TODO: Implement LIMIT 1000 differently
        posts['title'] = posts['title'].astype('int64')
        posts = posts.loc[posts.groupby('title')['timestamp'].idxmin().values]
        posts = posts.sort_values("title", ascending = False)
        posts['title'] = posts.apply(lambda row: f"[{row['title']}](https://www.reddit.com/r/countwithchickenlady/comments/{row['id']})", axis=1)
        posts['Date (UTC)'] = pd.to_datetime(posts['timestamp'], unit='s', utc=True).dt.date
        posts = posts[['username', 'title', 'Date (UTC)']]
        posts = posts.rename(columns={'username':'Username', 'title':'Count'})
        full_list = posts.to_markdown(index=False)

        ranking = posts['Username'].value_counts().reset_index()
        ranking.columns = ['Username', "Number of 100's"]
        ranking['Rank'] = ranking["Number of 100's"].rank(method='min', ascending=False).astype(int)
        ranking = ranking[['Rank', 'Username', "Number of 100's"]]
        leaderboard = ranking.to_markdown(index=False)

        wiki_text = "#100 counts\n\nThis page shows which users have counted to a number divisible by 100, and how many times!\n\n"+leaderboard+"\n\n"+full_list

        self.subreddit.wiki['100s'].edit(wiki_text, reason = 'Hourly update')
        self.handle_connection(keep_open)

    def update_1000_count_leaderboard(self, keep_open=False):
        print("Updating 1000 counts leaderboard")

        posts = pd.read_sql("SELECT username, title, id, timestamp FROM chicken_posts WHERE title LIKE '%000' ORDER BY CAST(title AS UNSIGNED) DESC LIMIT 1000", self.conn()) # TODO: Implement LIMIT 1000 differently
        posts['title'] = posts['title'].astype('int64')
        posts = posts.loc[posts.groupby('title')['timestamp'].idxmin().values]
        posts = posts.sort_values("title", ascending = False)
        posts['title'] = posts.apply(lambda row: f"[{row['title']}](https://www.reddit.com/r/countwithchickenlady/comments/{row['id']})", axis=1)
        posts['Date (UTC)'] = pd.to_datetime(posts['timestamp'], unit='s', utc=True).dt.date
        posts = posts[['username', 'title', 'Date (UTC)']]
        posts = posts.rename(columns={'username':'Username', 'title':'Count'})
        full_list = posts.to_markdown(index=False)

        ranking = posts['Username'].value_counts().reset_index()
        ranking.columns = ['Username', "Number of 1000's"]
        ranking['Rank'] = ranking["Number of 1000's"].rank(method='min', ascending=False).astype(int)
        ranking = ranking[['Rank', 'Username', "Number of 1000's"]]
        leaderboard = ranking.to_markdown(index=False)

        wiki_text = "#1000 counts\n\nThis page shows which users have counted to a number divisible by 1000, and how many times!\n\n"+leaderboard+"\n\n"+full_list

        self.subreddit.wiki['1000s'].edit(wiki_text, reason = 'Hourly update')
        self.handle_connection(keep_open)

    def update_top_posts_leaderboards(self, keep_open=False):
        print("Updating top posts leaderboards")

        start_time = time.time()
        posts = pd.read_sql("SELECT id, username, title, timestamp FROM chicken_posts", self.conn())
        posts = posts.rename(columns={'username':'Username', 'title':'Count'})
        posts['Upvotes'] = None
        posts['Comments'] = None
        for index, row in posts.iterrows():
            if (index+1) % 100 == 0:
                print(f"Post {index+1} out of {len(posts)}")
            try:
                post = self.reddit.submission(id=row['id'])
                posts.loc[index, 'Upvotes'] = post.score
                post.comments.replace_more(limit=None)
                posts.loc[index, 'Comments'] = len(post.comments.list())
            except Exception as e:
                print(e)
                time.sleep(30)
        
        posts['Count'] = posts.apply(lambda row: f"[{row['Count']}](https://www.reddit.com/r/countwithchickenlady/comments/{row['id']})", axis=1)
        posts['Date (UTC)'] = pd.to_datetime(posts['timestamp'], unit='s', utc=True).dt.date
        posts = posts.drop(columns=['id'])

        df_upvotes = posts[['Upvotes','Username','Count', 'Date (UTC)']]
        df_upvotes = df_upvotes.copy()
        df_upvotes['Rank'] = df_upvotes["Upvotes"].rank(method='min', ascending=False)
        df_upvotes = df_upvotes.sort_values(by=['Upvotes'], ascending=False)
        df_upvotes = df_upvotes.head(100)
        df_upvotes = df_upvotes[['Rank', 'Upvotes', 'Username', 'Count', 'Date (UTC)']]
        upvote_leaderboard = df_upvotes.to_markdown(index=False)

        ranking_upvotes = df_upvotes['Username'].value_counts().reset_index()
        ranking_upvotes.columns = ['Username', "Number of appearences in top 100"]
        ranking_upvotes['Rank'] = ranking_upvotes["Number of appearences in top 100"].rank(method='min', ascending=False).astype(int)
        ranking_upvotes = ranking_upvotes[['Rank', 'Username', "Number of appearences in top 100"]]
        appearences_upvotes_leaderboard = ranking_upvotes.to_markdown(index=False)

        df_comments = posts[['Comments','Username','Count', 'Date (UTC)']]
        df_comments = df_comments.copy()
        df_comments['Rank'] = df_comments["Comments"].rank(method='min', ascending=False)
        df_comments = df_comments.sort_values(by=['Comments'], ascending=False)
        df_comments = df_comments.head(100)
        df_comments = df_comments[['Rank', 'Comments', 'Username', 'Count', 'Date (UTC)']]
        comment_leaderboard = df_comments.to_markdown(index=False)

        ranking_comments = df_comments['Username'].value_counts().reset_index()
        ranking_comments.columns = ['Username', "Number of appearences in top 100"]
        ranking_comments['Rank'] = ranking_comments["Number of appearences in top 100"].rank(method='min', ascending=False).astype(int)
        ranking_comments = ranking_comments[['Rank', 'Username', "Number of appearences in top 100"]]
        appearences_comments_leaderboard = ranking_comments.to_markdown(index=False)

        wiki_text_comments = "#Most comments\n\nThis page shows the posts with the most comments of this sub!\n\n##Leaderboard\n"+appearences_comments_leaderboard+"\n\n##Comments\n"+comment_leaderboard
        self.subreddit.wiki['most_comments'].edit(wiki_text_comments, reason = 'Daily update')

        wiki_text_upvotes = "#Top posts\n\nThis page shows the posts with the most upvotes of this sub!\n\n##Leaderboard\n"+appearences_upvotes_leaderboard+"\n\n##Upvotes\n"+upvote_leaderboard
        self.subreddit.wiki['most_upvotes'].edit(wiki_text_upvotes, reason = 'Daily update')

        time_taken = time.time() - start_time

        hours, rem = divmod(time_taken, 3600)
        minutes, seconds = divmod(rem, 60)
        hours = int(time_taken // 3600)
        minutes = int((time_taken % 3600) // 60)
        seconds = int(time_taken % 60)
        self.send_email(
            'Top posts leaderboards updated',
            f'The top posts leaderboards have been updated. It took {time_taken:.2f} seconds to update them. That is {hours}h {minutes}m {seconds}s'
        )

        self.handle_connection(keep_open)

    def update_identical_digits_leaderboard(self, keep_open=False):
        print("Updating identical digits leaderboard")

        posts = pd.read_sql("SELECT id, username, title, timestamp FROM chicken_posts", self.conn())
        posts = posts[posts['title'].str.fullmatch(r'(\d)\1*')]
        posts['title'] = posts['title'].astype('int64')
        posts = posts.loc[posts.groupby('title')['timestamp'].idxmin().values]
        posts = posts.sort_values("title", ascending = False)
        posts['title'] = posts.apply(lambda row: f"[{row['title']}](https://www.reddit.com/r/countwithchickenlady/comments/{row['id']})", axis=1)
        posts['Date (UTC)'] = pd.to_datetime(posts['timestamp'], unit='s', utc=True).dt.date
        posts = posts[['username', 'title', 'Date (UTC)']]
        posts = posts.rename(columns={'username':'Username', 'title':'Count'})
        full_list = posts.to_markdown(index=False)

        ranking = posts['Username'].value_counts().reset_index()
        ranking.columns = ['Username', "Number of appearences"]
        ranking['Rank'] = ranking["Number of appearences"].rank(method='min', ascending=False).astype(int)
        ranking = ranking[['Rank', 'Username', "Number of appearences"]]
        leaderboard = ranking.to_markdown(index=False)

        wiki_text = "#Identical digits\n\nThis page shows which users have counted to a number that has only identcal digits, and how many times!\n\n"+leaderboard+"\n\n"+full_list

        self.subreddit.wiki['identical_digits'].edit(wiki_text, reason = 'Hourly update')
        self.handle_connection(keep_open)

    def update_streak_leaderboard(self, keep_open=False):
        print("Updating streak leaderboard")

        current_streaks = pd.read_sql("SELECT username, streak, COAD_streak FROM user_streaks", self.conn())
    
        current_normal_streaks = current_streaks.sort_values("streak", ascending = False).head(100)
        current_normal_streaks = current_normal_streaks.rename(columns={'username':'Username', 'streak':'Streak'})
        current_normal_streaks['Rank'] = current_normal_streaks['Streak'].rank(method='min', ascending=False).astype(int)
        current_normal_streaks = current_normal_streaks[['Rank', 'Username', 'Streak']]
        current_normal_streaks = current_normal_streaks[current_normal_streaks['Streak'] > 0]
        current_normal_streaks = current_normal_streaks.to_markdown(index=False)

        current_COAD_streaks = current_streaks.copy()
        current_COAD_streaks['Streak'] = current_streaks[['streak', 'COAD_streak']].max(axis=1)
        current_COAD_streaks = current_COAD_streaks.sort_values("Streak", ascending = False).head(100)
        current_COAD_streaks = current_COAD_streaks.rename(columns={'username':'Username'})
        current_COAD_streaks['Rank'] = current_COAD_streaks['Streak'].rank(method='min', ascending=False).astype(int)
        current_COAD_streaks = current_COAD_streaks[['Rank', 'Username', 'Streak']]
        current_COAD_streaks = current_COAD_streaks[current_COAD_streaks['Streak'] > 0]
        current_COAD_streaks = current_COAD_streaks.to_markdown(index=False)

        max_normal_streaks = pd.read_sql("SELECT username, MAX(current_streak) as streak FROM chicken_posts GROUP BY username", self.conn())
        max_normal_streaks = max_normal_streaks.sort_values("streak", ascending = False).head(100)
        max_normal_streaks = max_normal_streaks.rename(columns={'username':'Username', 'streak':'Streak'})
        max_normal_streaks['Rank'] = max_normal_streaks['Streak'].rank(method='min', ascending=False).astype(int)
        max_normal_streaks = max_normal_streaks[['Rank', 'Username', 'Streak']]
        max_normal_streaks = max_normal_streaks[max_normal_streaks['Streak'] > 0]
        max_normal_streaks = max_normal_streaks.to_markdown(index=False)

        max_COAD_streaks = pd.read_sql("SELECT username, MAX(current_streak) AS max_current_streak, MAX(current_COAD_streak) as max_current_COAD_streak FROM chicken_posts GROUP BY username", self.conn())
        max_COAD_streaks['Streak'] = max_COAD_streaks[['max_current_streak', 'max_current_COAD_streak']].max(axis=1)
        max_COAD_streaks = max_COAD_streaks.sort_values("Streak", ascending = False).head(100)
        max_COAD_streaks = max_COAD_streaks.rename(columns={'username':'Username'})
        max_COAD_streaks['Rank'] = max_COAD_streaks['Streak'].rank(method='min', ascending=False).astype(int)
        max_COAD_streaks = max_COAD_streaks[['Rank', 'Username', 'Streak']]
        max_COAD_streaks = max_COAD_streaks[max_COAD_streaks['Streak'] > 0]
        max_COAD_streaks = max_COAD_streaks.to_markdown(index=False)

        wiki_text = "#Top streaks\n\nThis page shows the top streaks of users of our sub!\n\n##This sub only\n\nThis shows the top streaks built up in this sub only.\n\n###Currently running streaks\n"+current_normal_streaks+"\n\n###Top streaks ever\n"+max_normal_streaks
        wiki_text += "\n\n##This sub and r/CountOnceADay\n\nThis shows the top streaks built up in this sub and possibly carried over from r/CountOnceADay.\n\n###Currently running streaks\n"+current_COAD_streaks+"\n\n###Top streaks ever\n"+max_COAD_streaks

        self.subreddit.wiki['top_streaks'].edit(wiki_text, reason = 'Hourly update')
        self.handle_connection(keep_open)