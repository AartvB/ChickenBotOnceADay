from chickenbot import ChickenBot
import schedule
import time

cb = ChickenBot()

def check_for_deleted_posts():
    cb.check_for_deleted_posts()

def extra_streak_check():
    cb.calculate_all_streaks(keep_open=True)
    cb.update_all_flair()

def update_leaderboards():
    cb.update_count_leaderboard()

schedule.every(1).minute.do(check_for_deleted_posts)
schedule.every(1).hour.do(extra_streak_check)
schedule.every(1).hour.do(update_leaderboards)

while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        print(e)