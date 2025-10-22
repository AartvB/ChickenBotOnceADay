# Use the streaming method to continuously check for new submissions
# Checks if the post has been manually allowed by a moderator.
# Checks if the post is already in the allowed posts database.
# Checks if the post has the correct title.
# Checks if the user has not posted 3 times in the last 2 calendar days.
# Updates the user streak.
# Updates the post that tells the correct number.

from chickenbot import ChickenBot
import time

cb = ChickenBot()
cb.update_target_post()

while True:
    try:
        for submission in cb.subreddit.stream.submissions(skip_existing=True):
            while True:
                try:
                    cb.update_target_post()
                    break
                except Exception as e:
                    print(f"Error in execution: {e}")
                    time.sleep(30)

    except Exception as e:
        print(f"Error in submission stream: {e}")
        time.sleep(30)