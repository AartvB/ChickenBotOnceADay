from chickenbot import ChickenBot
import schedule
import time

cb = ChickenBot()

def check_for_deleted_posts():
    cb.check_for_deleted_posts()

def extra_streak_check():
    cb.record_empty_post_streaks(keep_open=True)
    cb.record_all_streaks(keep_open=True)
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