import praw
import sqlite3
import pandas as pd
import math
import pytz
import time
from datetime import datetime, timezone, timedelta
import shutil
import re

def wrap_method(method):
    # (Almost) all methods are wrapped within this method.
    # It makes sure the database connection stays open when the
    # keyword 'keep_open' is set to True.
    # If the keyword is omitted, the database connection will be closed,
    # unless somewhere else up in the call stack, keep_open was set to True

    # This way, the database connection is automatically handled,
    # and doesn't need any attention.
    def wrapped(self, *args, **kwargs): 
        previously_keep_open = self._keep_open
        self._keep_open = kwargs.pop('keep_open', previously_keep_open) or previously_keep_open

        self._call_stack.append(method.__name__)

        result = method(self, *args, **kwargs)

        self._call_stack.pop()
        if not self._call_stack:
            self.handle_connection(self._keep_open)

        if not previously_keep_open:
            self._keep_open = False

        return result
    return wrapped

class AutoPostCallMeta(type):
    # metclass to use the wrap_method defined above.
    def __new__(cls, name, bases, class_dict):
        new_dict = {}
        for attr_name, attr_value in class_dict.items():
            if callable(attr_value) and not attr_name.startswith("__") and attr_name not in ['handle_connection', 'connection_is_open', 'open_connection', 'close_connection', 'conn', 'cursor']:
                attr_value = wrap_method(attr_value)
            new_dict[attr_name] = attr_value

        return type.__new__(cls, name, bases, new_dict)

class ChickenBot(metaclass=AutoPostCallMeta):
    def __init__(self):
        # Setup reddit bot connection
        self.reddit = praw.Reddit('bot1')
        self.reddit.validate_on_submit = True

        # Choose subreddit
        choice = input("For which subreddit do you want to activate the bot? Enter 1 for 'countwithchickenlady' or 2 for 'CWCLafterdark': ")
        if choice == "1":
            self.subredditname = "countwithchickenlady"
            self.current_count_link = "https://www.reddit.com/r/countwithchickenlady/comments/1iulihu"
            self.target_post = self.reddit.submission(id='1iulihu')
            self.db = "chicken_bot"
        elif choice == "2":
            self.subredditname = "CWCLafterdark"
            self.current_count_link = "https://www.reddit.com/r/CWCLafterdark/comments/1nygkqd/"
            self.target_post = self.reddit.submission(id='1nygkqd')
            self.db = "chkcken_bot_18plus"
        else:
            print("Invalid input!")
            raise ValueError("Invalid input! Please enter 1 or 2.")
        
        self.subreddit = self.reddit.subreddit(self.subredditname)

    _connection_is_open = False
    _keep_open = False
    _call_stack = []

    def __del__(self):
        self.close_connection()

    def connection_is_open(self):
        return self._connection_is_open
    
    def open_connection(self):
        if self._connection_is_open:
            return
        self._connection_is_open = True
        self._conn = sqlite3.connect(self.db+'.db')
        self._conn.row_factory = sqlite3.Row
        self._cursor = self._conn.cursor()

    def handle_connection(self, keep_open):
        if not keep_open:
            self.close_connection()

    def close_connection(self):
        if self.connection_is_open():
            self._conn.close()
            self._connection_is_open = False
    
    def conn(self):
        self.open_connection()
        return self._conn

    def cursor(self):
        self.open_connection()
        return self._cursor
        
    def setup_database(self):
        self.cursor().execute('''
            CREATE TABLE IF NOT EXISTS chicken_posts (
                    id TEXT PRIMARY KEY,
                    username TEXT,
                    timestamp INTEGER,
                    approved BOOLEAN,
                    title TEXT,
                    current_streak INTEGER,
                    current_COAD_streak INTEGER,
                    upvotes INTEGER DEFAULT NULL,
                    comments INTEGER DEFAULT NULL
            )
        ''')
        self.cursor().execute('''
            CREATE TABLE IF NOT EXISTS COAD_posts (
                username TEXT PRIMARY KEY,
                post_id TEXT,
                streak INTEGER
            )
        ''')
        self.cursor().execute('''
            CREATE TABLE IF NOT EXISTS user_streaks (
                    timestamp INTEGER,
                    username TEXT PRIMARY KEY,
                    streak INTEGER,
                    COAD_streak INTEGER
                )
        ''')
        self.cursor().execute('''
            CREATE TABLE IF NOT EXISTS deleted_posts (
                id TEXT PRIMARY KEY,
                username TEXT,
                timestamp INTEGER
            )
        ''')
        self.conn().commit()

    def backup_database(self):
        shutil.copy2(self.db+'.db', f"{self.db} backup {datetime.now().strftime('%Y-%m-%d %H.%M.%S')}.db")

    def fill_database_after_failure(self):
        # If the bot breaks, the posts aren't scanned for having the correct number anymore.
        # Additionally, the post that shows the correct number, gets stuck.
        # Many users will upload posts with the wrong count.
        # If the bot is activated, it will remove all these posts.
        # To avoid this (and register all recent posts as valid), run this function.
        for post in self.subreddit.new(limit=1000):  # Fetches the newest posts
            self.cursor().execute('''
                INSERT OR IGNORE INTO chicken_posts (id, username, timestamp, approved, title)
                VALUES (?, ?, ?, 1, ?)
            ''', (post.id, self.get_author(post), post.created_utc, post.title))
        self.conn().commit()
        self.record_all_streaks()

    def get_all_posts(self, usernamee):
        posts = pd.read_sql("SELECT * FROM chicken_posts WHERE username = ?", self.conn(), params=(username,))
        deleted_posts = pd.read_sql("SELECT * FROM deleted_posts WHERE username = ?", self.conn(), params=(username,))
        return posts, deleted_posts

    def get_all_users(self):
        self.cursor().execute("SELECT DISTINCT username FROM chicken_posts")
        users = [row["username"] for row in self.cursor().fetchall()]
        return users

    def is_user(self, username):
        return username in self.get_all_users()
    
    def calculate_streak(self, username, timestamp = None):
        # Not that the streka is not recorded in the database! Use record_streak for that!
        if timestamp is None:
            timestamp = time.time()

        df = pd.read_sql("SELECT timestamp FROM chicken_posts WHERE username = ? AND timestamp <= ?", self.conn(), params=(username,timestamp))
        self.cursor().execute("SELECT * FROM COAD_posts WHERE username = ?", (username,))
        
        # People who moved from r/CountOnceADay,
        # were allowed to continue the streak that was built up over there.
        # The COAD_streak is the streak that includes that previous streak.
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

            today_datetime = datetime.fromtimestamp(timestamp, tz=tz)
            today = today_datetime.date()
            yesterday = (today_datetime - timedelta(days=1)).date()

            if has_COAD_streak and (last_COAD_date == today or last_COAD_date == yesterday):
                COAD_streak = COAD_streak_number

            for date in df["post_date"]:
                if (date == today or date == yesterday) and last_date is None: # This was the first post, and it was today or yesterday
                    streak = 1
                    last_date = date
                elif last_date is not None: # This was not the first post
                    if date == last_date - timedelta(days=1): # The previous post was yesterday
                        if has_COAD_streak and date == last_COAD_date:
                            COAD_streak = COAD_streak_number + streak
                        streak += 1
                        last_date = date
                    else:
                        break
                else: # PRevious post was earlier than today or yesterday
                    streak = 0
                    break
            if has_COAD_streak and last_date is not None and last_COAD_date == last_date - timedelta(days=1):
                COAD_streak = COAD_streak_number + streak
            max_streak = max(max_streak, streak)
            if has_COAD_streak:
                max_COAD_streak = max(max_COAD_streak, COAD_streak)
        return max_streak, max_COAD_streak

    def record_streak(self, username):
        self.cursor().execute("""
            INSERT INTO user_streaks (timestamp, username, streak, COAD_streak)
            VALUES (CURRENT_TIMESTAMP, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
            timestamp = CURRENT_TIMESTAMP,
            streak = excluded.streak,
            COAD_streak = excluded.COAD_streak
        """, (username, *self.calculate_streak(username, keep_open=True)))
        self.conn().commit()

    def record_all_streaks(self):
        print("Calculating user streaks")
        
        users = self.get_all_users(keep_open=True)
        streaks = {}

        for user_no, user in enumerate(users):
            if (user_no+1) % 20 == 0:
                print(f"user {user_no+1} out of {len(users)}")
            streaks[user] = {}
            while True:
                try:
                    streaks[user]['normal'], streaks[user]['COAD'] = self.calculate_streak(user, keep_open=True)
                except:
                    print("Error, try again in 10 seconds")
                    time.sleep(10)
                break
    
        for user, streak in streaks.items():
            self.cursor().execute("""
                INSERT INTO user_streaks (timestamp, username, streak, COAD_streak)
                VALUES (CURRENT_TIMESTAMP, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                timestamp = CURRENT_TIMESTAMP,
                streak = excluded.streak,
                COAD_streak = excluded.COAD_streak
            """, (user, streak['normal'], streak['COAD']))
        
        self.conn().commit()
        print("Finished recording user streaks")

    def record_post_streak(self, post_id, replace = True):
        # Records the streak at the moment the post was made
        self.cursor().execute(f"SELECT username, timestamp, current_streak, current_COAD_streak FROM chicken_posts WHERE id = ?", (post_id,))
        username, timestamp, streak, COAD_streak = self.cursor().fetchone()
        if not replace and streak is not None and COAD_streak is not None:
            return
        streak, COAD_streak = self.calculate_streak(username, timestamp=timestamp, keep_open=True)
        self.cursor().execute("UPDATE chicken_posts SET current_streak = ?, current_COAD_streak = ? WHERE id = ?", (streak, COAD_streak, post_id))
        self.conn().commit()

    def record_post_streaks_user(self, username):
        posts = pd.read_sql("SELECT id FROM chicken_posts WHERE username = ?", self.conn(), params=(username,))
        for i, post_id in enumerate(posts['id']):
            if (i+1) % 20 == 0:
                print("Recording post streaks for user", username, "post", i+1, "out of", len(posts))
            self.record_post_streak(post_id, keep_open=True)
        self.record_streak(username)

    def record_empty_post_streaks(self, batch_size = 500):
        # batch_size makes sure that each time this function is called,
        # only a small part of empty post streaks is recorded, to make sure
        # it isn't too much work for the VM. This function should be called
        # multiple times, until all empty post_streaks are recorded.
        posts = pd.read_sql("SELECT id FROM chicken_posts WHERE current_streak IS NULL OR current_COAD_streak IS NULL", self.conn())
        print(f"Recording empty post streaks: {min(batch_size,len(posts))} posts will be handled during this function call.")
        for i, post_id in enumerate(posts['id']):
            if i == batch_size:
                print(f"Batch size reached, {len(posts)-i} left to do!")
                break
            print("Recording empty post streaks", i+1, "out of", len(posts))
            for j in range(0,10):
                try:
                    self.record_post_streak(post_id, keep_open=True)
                except:
                    print("Error, try again in 10 seconds")
                    time.sleep(10)
        print("Finished recording empty post streaks")

    def record_post_statistic(self, post_id):
        # Record upvote and comment count.
        # Used for post leaderboard.
        self.cursor().execute("SELECT 1 FROM chicken_posts WHERE id = ?",(post_id,))
        if (self.cursor().fetchone() is None):
            raise Exception(f"Post {post_id} has not been found in the database!")
        post = self.reddit.submission(post_id)
        upvotes = post.score
        post.comments.replace_more(limit=None)
        comments = len(post.comments.list())

        self.cursor().execute("UPDATE chicken_posts SET comments = ?, upvotes = ? WHERE id = ?", (comments, upvotes, post_id))
        self.conn().commit()

    def record_post_statistics(self, n_days_history = 21):
        # Record upvote and comment count for all posts in the past n_days_history days.
        # Used for post leaderboard.
        posts = pd.read_sql("SELECT id FROM chicken_posts WHERE timestamp >= ? OR comments IS NULL OR upvotes IS NULL", self.conn(), params=(time.time()-n_days_history*24*60*60,))
        print(f'Recording post statistics for the past {n_days_history} days: {len(posts)} posts')
        for i, row in posts.iterrows():
            if (i+1) % 100 == 0:
                print(f"Post {i+1} out of {len(posts)}")
            while True:
                try:
                    self.record_post_statistic(row['id'],keep_open=True)
                    break
                except Exception as e:
                    print(e)
                    time.sleep(30)
        print('Finished recording post statistics')

    def get_text_from_flair(self, text):
        match = re.match(r'^Streak: \d+$', text)
        if match:
            return ''
        match = re.match(r'^(.*) - Streak: \d+$', text)
        return match.group(1) if match else text

    def update_user_flair(self, username):
        self.cursor().execute("SELECT streak, COAD_streak FROM user_streaks WHERE username = ?", (username,))
        try:
            streak = max(self.cursor().fetchone())
        except Exception as e:
            print(f"Failed to fetch streak for {username}: {e}")
            return
        
        try:
            # Check if the user exists in the subreddit
            user = self.reddit.redditor(username)

            # Get current flair
            flair_generator = self.subreddit.flair(username)
            current_flair = next(flair_generator, None)['flair_text']

            flair_text = self.get_text_from_flair(current_flair)

            user_flair = ""
            if flair_text != '':
                user_flair = flair_text + ' - '
            user_flair += "Streak: " + str(streak)
            if username == "chickenbotonceaday":
                user_flair = "Streak: 3.1415926535"

            # Set the user's flair
            if current_flair != user_flair:
                self.subreddit.flair.set(user, text=user_flair)
        except Exception as e:
            print(f"Failed to set flair for {username}: {e}")

    def update_all_flair(self):
        # Update user flair
        print("Updating user flairs")

        users = self.get_all_users(keep_open=True)

        for user_no, user in enumerate(users):                        
            if (user_no+1) % 20 == 0:
                print(f"user {user_no+1} out of {len(users)}")
            self.update_user_flair(user, keep_open=True)

        print("Finished updating user flairs")

    def get_author(self, submission):
        return submission.author.name if submission.author else "[deleted]"

    def update_target_post(self, post_limit=8):
        # Checks the most recent post_limit posts.
        # Checks if it has been manually allowed by a moderator.
        # Checks if it is already in the allowed posts database.
        # Checks if they have the correct title.
        # Checks if the user has not posted 3 times in the last 2 calendar days.
        # Updates the user streak.
        # Updates the post that tells the correct number.
        current_count = 0
        if self.subreddit == 'countwithchickenlady':
            current_count = 20126 # edit to new value after bot failure
        elif self.subreddit == 'CWCLafterdark':
            current_count = 531 # edit to new value after bot failure

        print("New check")
        for submission in reversed(list(self.subreddit.new(limit=post_limit))):
            print(f"Checking post {submission.title}")
            self.cursor().execute("SELECT approved FROM chicken_posts WHERE id = ?;", (submission.id,))
            approved = self.cursor().fetchall()

            if submission.title.isnumeric():
                post_number = int(submission.title)
                if post_number == current_count + 1 or current_count == 0:
                    post_was_removed = False
                    deletion_occured = True
                    while deletion_occured:
                        if not approved and submission.approved_by is None:
                            earlier_posts = pd.read_sql("SELECT id, timestamp, title FROM chicken_posts WHERE username = ? ORDER BY timestamp DESC LIMIT 2", self.conn(), params=(self.get_author(submission),))
                            earlier_posts.loc[len(earlier_posts)] = [submission.id, submission.created_utc, submission.title]

                            # Ensure timestamp is datetime
                            earlier_posts["datetime"] = pd.to_datetime(earlier_posts["timestamp"], unit='s', utc=True)

                            double_post = True # Check if a user posted twice on the same calendar day
                            for tz_name in pytz.common_timezones:
                                tz = pytz.timezone(tz_name)
                        
                                # Convert timestamp to the specific timezone
                                earlier_posts["local_time"] = earlier_posts["datetime"].dt.tz_convert(tz)
                                earlier_posts["post_date"] = earlier_posts["local_time"].dt.date  # Extract date part
                                earlier_posts = earlier_posts.sort_values("post_date", ascending = False)

                                # Check if all values in earlier_posts['post_date'] are unique
                                if earlier_posts["post_date"].is_unique:
                                    double_post = False
                                    break

                            if double_post:
                                deletion_found_now = False
                                for _, row in earlier_posts.iterrows():
                                    earlier_submission = self.reddit.submission(id=row['id'])
                                    if (earlier_submission.selftext == "[deleted]" or earlier_submission.author is None) and earlier_submission.created_utc > time.time() - 5*60:
                                        deletion_found_now = True
                                        break

                                if deletion_found_now:
                                    print("Waiting 15 seconds due to post deletion")
                                    time.sleep(15)
                                else:
                                    deletion_occured = False

                                    print(f"Double post detected: {submission.title}")

                                    comment_text = "This post has been removed because of your latest two or three posts, at least two have been on the same calendar day. You may post only once per calendar day. Please wait until the next calendar day to post again.\nThe posts were as follows:\n\n"
                                    now = time.time()

                                    for index, row in earlier_posts.iterrows():
                                        earlier_submission = self.reddit.submission(id=row['id'])
                                        # Convert Unix timestamp to datetime
                                        past = row['timestamp']

                                        # Difference
                                        diff = now - past
                                        
                                        days = math.floor(diff / 86400)  # 86400 seconds in a day
                                        hours = math.floor((diff % 86400) / 3600)
                                        minutes = math.floor((diff % 3600) / 60)
                                        seconds = math.floor(diff % 60)

                                        comment_text += f"{days} days, {hours} hours, {minutes} minutes and {seconds} seconds ago: [{row['title']}](https://www.reddit.com/{earlier_submission.permalink})\n\n"
                                    comment_text += "^(This action was performed automatically by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"

                                    # Remove the incorrect post
                                    submission.mod.remove()
                                    submission.mod.send_removal_message(comment_text)

                                    post_was_removed = True
                            else:
                                deletion_occured = False
                        else:
                            deletion_occured = False
                    if not post_was_removed:
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

                        # Leave a comment explaining the removal
                        comment_text = (
                            f"This post has been removed because the correct next number was {current_count + 1}, but this post has '{post_number}' as title. Please check the most recent number before posting. You can find the correct number in [this]({self.current_count_link}) post.\n\nIt might be possible that someone else simply was slightly faster with their post.\n\nFeel free to post again with the correct new number.\n\n^(This action was performed automatically by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"
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

                # Leave a comment explaining the removal
                comment_text = f"This post has been removed because the title must be a number. Please only post the next number in sequence. You can find the correct number in [this]({self.current_count_link}) post.\n\n^(This action was performed automatically by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)"

                # Remove the incorrect post
                submission.mod.remove()
                submission.mod.send_removal_message(comment_text)

        self.target_post.edit(f"The next number should be: [{current_count + 1}](https://www.reddit.com/r/{self.subredditname}/submit?title={current_count + 1})\n\n^(This comment is automatically updated by a bot. If you think it made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)")

    def add_COAD_streak(self):
        # People who moved from r/CountOnceADay,
        # are allowed to continue the streak that was built up over there.
        # This function can be used to add a COAD_streak.
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
        self.record_post_streaks_user(username)

        print(f"The COAD streak of {username} has been updated succesfully.")

    def check_player_streak(self):
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
            print(f"Date/time: {row['local_time']}, post: https://www.reddit.com/r/{self.subreddit}/comments/{row['id']}, recorded streak: {row['current_streak']}")
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

    def delete_post(self):
        post_id = input("The post to delete has id: ")

        self.cursor().execute("SELECT * FROM chicken_posts WHERE id = ?", (post_id,))
        result = self.cursor().fetchone()

        self.cursor().execute("INSERT INTO deleted_posts (id, username, timestamp) VALUES (?, ?, ?)", (post_id, result[1], result[2]))
        self.cursor().execute("DELETE FROM chicken_posts WHERE id = ?",(post_id,))
        self.conn().commit()

        print("Deleted post from database, now updating user streaks and flair.")

        self.record_post_streaks_user(result[1],keep_open=True)
        self.update_user_flair(result[1])

        print(f"Post deleted succesfully: Post {post_id} created by u/{result[1]}")

    def run_sql(self, query):
        self.cursor().execute(query)
        self.conn().commit()

    def check_for_deleted_posts(self):
        # If someone (including moderators) deletes a post within 10 minute of posting it,
        # it doesn't count for the streak. Otherwise it will.
        print("Checking for deleted posts")

        current_time = int(time.time())
        df = pd.read_sql("SELECT * FROM chicken_posts WHERE timestamp >= ?", self.conn(), params=(current_time-600,))
        for _, row in df.iterrows():
            post_id = row['id']
            user = row['username']
            submission = self.reddit.submission(id=post_id)

            print(f"Checking post {submission.title}")
            if submission.selftext == "[deleted]" or submission.author is None:
                print("Post has been deleted!")
                try:
                    self.cursor().execute("INSERT INTO deleted_posts (id, username, timestamp) VALUES (?, ?, ?)",
                                          (post_id, user, submission.created_utc))
                    self.conn().commit()
                    self.cursor().execute("DELETE FROM chicken_posts WHERE id = ?", (post_id,))
                    self.conn().commit()

                    self.update_target_post(keep_open=True)
                    self.record_streak(user,keep_open=True)
                    self.update_user_flair(user, keep_open=True)
                    self.record_post_streaks_user(user,keep_open=True)
                except Exception as e:
                    print(f'An error occuered when I tried to handle the post deletion. Error message:\n{e}')
    
    def start_maintenance(self):
        # Run this code if the bot is not running.
        print('Started maintenance')
        self.target_post.edit(f"The bot is currently under maintenance. Our apologies for the inconvenience. Please [sort by new](https://www.reddit.com/r/{self.subredditname}/new/) to see what the next number in the sequence should be, and use this number as the title for your new post.\n\n^(If you think the bot made a mistake, contact the mods via modmail. The code for this bot is fully open source, and can be found [here](https://github.com/AartvB/ChickenBotOnceADay).)")

    def end_maintenance(self):
        # Run this code if the bot was under maintenance for a short while.
        print('Ended maintenance')
        self.update_target_post(post_limit=20)

    def update_count_leaderboard(self):
        # The leaderboard on the wiki that shows the people with the most posts.
        print("Updating count leaderboard")

        posts = pd.read_sql("SELECT username, COUNT(*) as counts FROM chicken_posts GROUP BY username ORDER BY counts DESC LIMIT 1000", self.conn())
        posts['rank'] = posts['counts'].rank(method='min', ascending=False).astype(int)
        posts = posts[['rank', 'username', 'counts']]
        posts = posts.rename(columns={'rank':'Rank', 'username':'Username', 'counts':'Counts'})
        leaderboard = posts.to_markdown(index=False)

        wiki_text = "#All counters of our beautiful sub!\n\nThis shows the top 1000 posters of our sub!\n\n"+leaderboard

        self.subreddit.wiki['counts'].edit(wiki_text, reason = 'Hourly update')

    def update_whole_counts_leaderboard(self):
        # The leaderboards on the wiki that shows the people who
        # counted to a multiple of 10, 100, 1000 etc.
        print("Updating whole counts leaderboards")
        n_zeroes = 1
        while True:
            zeroes_string = n_zeroes*'0'
            print(f"Updating 1{zeroes_string}s leaderboard")

            posts = pd.read_sql("SELECT username, title, id, timestamp FROM chicken_posts WHERE title LIKE ?", self.conn(),params=(f"%{zeroes_string}",))
            if (len(posts) == 0):
                break
            posts['title'] = posts['title'].astype('int64')
            posts = posts.loc[posts.groupby('title')['timestamp'].idxmin().values]
            posts = posts.sort_values("title", ascending = False)
            posts['title'] = posts.apply(lambda row: f"[{row['title']}](https://www.reddit.com/r/{self.subredditname}/comments/{row['id']})", axis=1)
            posts['Date (UTC)'] = pd.to_datetime(posts['timestamp'], unit='s', utc=True).dt.date
            posts = posts[['username', 'title', 'Date (UTC)']]
            posts = posts.rename(columns={'username':'Username', 'title':'Count'})
            full_list = posts.head(1000).to_markdown(index=False)

            ranking = posts['Username'].value_counts().reset_index()
            ranking.columns = ['Username', f"Number of 1{zeroes_string}'s"]
            ranking['Rank'] = ranking[f"Number of 1{zeroes_string}'s"].rank(method='min', ascending=False).astype(int)
            ranking = ranking[['Rank', 'Username', f"Number of 1{zeroes_string}'s"]]
            leaderboard = ranking.to_markdown(index=False)

            wiki_text = f"#1{zeroes_string} counts\n\nThis page shows which users have counted to a number divisible by 1{zeroes_string}, and how many times!\n\n"+leaderboard+"\n\n"+full_list

            self.subreddit.wiki[f'1{zeroes_string}s'].edit(wiki_text, reason = f'New 1{zeroes_string} number')
            n_zeroes += 1

    def update_top_posts_leaderboards(self):
        # The leaderboard on the wiki that shows the posts with the
        # most upvotes and the most comments.

        print("Updating top posts leaderboards")

        self.record_post_statistics()

        posts = pd.read_sql("SELECT id, username, title, upvotes, comments, timestamp FROM chicken_posts", self.conn())
        posts = posts.rename(columns={'username':'Username', 'title':'Count', 'upvotes':'Upvotes','comments': 'Comments'})

        posts['Count'] = posts.apply(lambda row: f"[{row['Count']}](https://www.reddit.com/r/{self.subredditname}/comments/{row['id']})", axis=1)
        posts['Date (UTC)'] = pd.to_datetime(posts['timestamp'], unit='s', utc=True).dt.date

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

        wiki_text_comments = "#Most comments\n\nThis page shows the posts with the most comments of this sub!\n\nNote: Comment count is stored locally, and will only be updated up to 21 days after the post is posted. Let us know (via mod mail) if the comment count of a specific post has increase significantly since then, so we can update the comment count manually.\n\n##Leaderboard\n"+appearences_comments_leaderboard+"\n\n##Comments\n"+comment_leaderboard
        self.subreddit.wiki['most_comments'].edit(wiki_text_comments, reason = 'Daily update')

        wiki_text_upvotes = "#Top posts\n\nThis page shows the posts with the most upvotes of this sub!\n\nNote: Upvote count is stored locally, and will only be updated up to 21 days after the post is posted. Let us know (via mod mail) if the upvote count of a specific post has increase significantly since then, so we can update the upvote count manually.\n\n##Leaderboard\n"+appearences_upvotes_leaderboard+"\n\n##Upvotes\n"+upvote_leaderboard
        self.subreddit.wiki['most_upvotes'].edit(wiki_text_upvotes, reason = 'Daily update')


    def update_identical_digits_leaderboard(self):
        # The leaderboard on the wiki that shows the posts with identical digits.

        print("Updating identical digits leaderboard")

        posts = pd.read_sql("SELECT id, username, title, timestamp FROM chicken_posts", self.conn())
        posts = posts[posts['title'].str.fullmatch(r'(\d)\1*')]
        posts['title'] = posts['title'].astype('int64')
        posts = posts.loc[posts.groupby('title')['timestamp'].idxmin().values]
        posts = posts.sort_values("title", ascending = False)
        posts['title'] = posts.apply(lambda row: f"[{row['title']}](https://www.reddit.com/r/{self.subredditname}/comments/{row['id']})", axis=1)
        posts['Date (UTC)'] = pd.to_datetime(posts['timestamp'], unit='s', utc=True).dt.date
        posts = posts[['username', 'title', 'Date (UTC)']]
        posts = posts.rename(columns={'username':'Username', 'title':'Count'})
        full_list = posts.to_markdown(index=False)

        ranking = posts['Username'].value_counts().reset_index()
        ranking.columns = ['Username', "Number of appearences"]
        ranking['Rank'] = ranking["Number of appearences"].rank(method='min', ascending=False).astype(int)
        ranking = ranking[['Rank', 'Username', "Number of appearences"]]
        leaderboard = ranking.to_markdown(index=False)

        wiki_text = "#Identical digits\n\nThis page shows which users have counted to a number that has only identical digits, and how many times!\n\n"+leaderboard+"\n\n"+full_list

        self.subreddit.wiki['identical_digits'].edit(wiki_text, reason = 'New identical digit number')

    def update_palindrome_leaderboard(self):
        # The leaderboard on the wiki that shows the posts with titles that are palindromes.
        print("Updating palindrome leaderboard")

        posts = pd.read_sql("SELECT id, username, title, timestamp FROM chicken_posts", self.conn())
        posts = posts[posts['title'] == posts['title'].str[::-1]]
        posts['title'] = posts['title'].astype('int64')
        posts = posts.loc[posts.groupby('title')['timestamp'].idxmin().values]
        posts = posts.sort_values("title", ascending = False)
        posts['title'] = posts.apply(lambda row: f"[{row['title']}](https://www.reddit.com/r/{self.subredditname}/comments/{row['id']})", axis=1)
        posts['Date (UTC)'] = pd.to_datetime(posts['timestamp'], unit='s', utc=True).dt.date
        posts = posts[['username', 'title', 'Date (UTC)']]
        posts = posts.rename(columns={'username':'Username', 'title':'Count'})
        full_list = posts.to_markdown(index=False)

        ranking = posts['Username'].value_counts().reset_index()
        ranking.columns = ['Username', "Number of appearences"]
        ranking['Rank'] = ranking["Number of appearences"].rank(method='min', ascending=False).astype(int)
        ranking = ranking[['Rank', 'Username', "Number of appearences"]]
        leaderboard = ranking.to_markdown(index=False)

        wiki_text = "#Palindromes\n\nThis page shows which users have counted to palindrome numbers (numbers that are the same backwards as forwards), and how many times!\n\n"+leaderboard+"\n\n"+full_list

        self.subreddit.wiki['palindrome_numbers'].edit(wiki_text, reason = 'New palindrome number')

    def update_streak_leaderboard(self):
        # The leaderboard on the wiki that shows the people with the highest streaks
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
        if self.subreddit == 'countwithchickenlady':
            wiki_text += "\n\n##This sub and r/CountOnceADay\n\nThis shows the top streaks built up in this sub and possibly carried over from r/CountOnceADay.\n\n###Currently running streaks\n"+current_COAD_streaks+"\n\n###Top streaks ever\n"+max_COAD_streaks

        self.subreddit.wiki['top_streaks'].edit(wiki_text, reason = 'Hourly update')