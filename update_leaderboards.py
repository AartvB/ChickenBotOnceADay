from chickenbot import ChickenBot
import schedule
import time

cb = ChickenBot()

def update_count_leaderboard():
    cb.update_count_leaderboard()

def update_100_count_leaderboard():    
    cb.update_100_count_leaderboard()

schedule.every(1).hour.do(update_count_leaderboard)
schedule.every(1).hour.do(update_100_count_leaderboard)

while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        print(e)