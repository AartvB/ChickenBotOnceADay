from chickenbot import ChickenBot
import schedule
import time

cb = ChickenBot()

def update_count_leaderboard():
    cb.update_count_leaderboard()

def update_whole_counts_leaderboard():    
    cb.update_whole_counts_leaderboard()

def update_top_posts_leaderboards():
    cb.update_top_posts_leaderboards()

def update_identical_digits_leaderboard():
    cb.update_identical_digits_leaderboard()

def update_palindrome_leaderboard():
    cb.update_palindrome_leaderboard()

def update_streak_leaderboard():
    cb.update_streak_leaderboard()

schedule.every(1).hour.do(update_count_leaderboard)
schedule.every(1).hour.do(update_whole_counts_leaderboard)
schedule.every(1).hour.do(update_identical_digits_leaderboard)
schedule.every(1).hour.do(update_streak_leaderboard)
schedule.every(1).hour.do(update_palindrome_leaderboard)
schedule.every(1).day.do(update_top_posts_leaderboards)

update_count_leaderboard()
update_whole_counts_leaderboard()
update_identical_digits_leaderboard()
update_streak_leaderboard()
update_palindrome_leaderboard()
update_top_posts_leaderboards()

while True:
    try:
        schedule.run_pending()
        time.sleep(60)
    except Exception as e:
        print(e)