from chickenbot import ChickenBot
import schedule
import time

cb = ChickenBot()

def check_for_deleted_posts():
    cb.check_for_deleted_posts()

def extra_streak_check():
    cb.calculate_all_streaks(keep_open=True)
    cb.update_all_flair()

schedule.every(1).minute.do(check_for_deleted_posts)
schedule.every(1).hour.do(extra_streak_check)

while True:
    schedule.run_pending()
    time.sleep(1)