from chickenbot import ChickenBot
import schedule
import time

cb = ChickenBot()

def update_count_leaderboard():
    cb.update_count_leaderboard()

def update_100_count_leaderboard():    
    cb.update_100_count_leaderboard()

def update_1000_count_leaderboard():    
    cb.update_1000_count_leaderboard()

def update_top_posts_leaderboards():
    cb.update_top_posts_leaderboards()

def update_identical_digits_leaderboard():
    cb.update_identical_digits_leaderboard()

schedule.every(1).hour.do(update_count_leaderboard)
schedule.every(1).hour.do(update_100_count_leaderboard)
schedule.every(1).hour.do(update_1000_count_leaderboard)
schedule.every(1).hour.do(update_identical_digits_leaderboard)
schedule.every(1).day.do(update_top_posts_leaderboards)

update_count_leaderboard()
update_100_count_leaderboard()
update_1000_count_leaderboard()
update_identical_digits_leaderboard()
update_top_posts_leaderboards()

while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        print(e)