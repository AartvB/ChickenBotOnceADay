# If someone (including moderators) deletes a post within 10 minute of posting it,
# it doesn't count for the streak. This script checks this every 15 seconds.
# Furthermore, this script updates the streaks of people every hour. For example,
# if someone hasn't posted in a full day, their streak will be set to 0 again.

from chickenbot import ChickenBot
import schedule
import time

cb = ChickenBot()

def check_for_deleted_posts():
    cb.check_for_deleted_posts()

def extra_streak_check():
    cb.record_empty_post_streaks()
    cb.record_all_streaks()
    cb.update_all_flair()

schedule.every(15).seconds.do(check_for_deleted_posts)
schedule.every(1).hour.do(extra_streak_check)

# Run the first check immediately
check_for_deleted_posts()
extra_streak_check()

while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        print(e)