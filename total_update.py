#!pip install praw
#!pip install db-sqlite3

import praw
import pytz
import time
from datetime import datetime
from shared_functions import send_email, find_streaks, update_flair, update_target_post

# Run bot
reddit = praw.Reddit('bot1')
reddit.validate_on_submit = True

# Runs the bot, responding to new posts immediately.
subreddit = reddit.subreddit("countwithchickenlady")

update_target_post()
find_streaks()
update_flair()
update_target_post()

n_errors = {'execution': {'first_error':0,'n':0,'last_message':'','first_ever_error':0}, 'stream': {'first_error':0,'n':0,'last_message':'','first_ever_error':0}}

# Use the streaming method to continuously check for new submissions
while True:
    try:
        for submission in subreddit.stream.submissions(skip_existing=True):
            if n_errors['stream']['n'] > 0:
                utc_time = datetime.fromtimestamp(n_errors['stream']['first_ever_error'])
                local_time = utc_time.astimezone(pytz.timezone('Europe/Amsterdam'))
                time_first_error = local_time.strftime('%Y-%m-%d %H:%M:%S %Z')
        
                send_email('Stream error solved',f'{n_errors['stream']['n']-1} additional stream errors have happened, but an execution has started without any errors now, so the stream error has been solved. The first error occured at {time_first_error}. Latest error message:\n\n{n_errors['stream']['last_message']}')
                n_errors['stream']['first_error'] = 0
                n_errors['stream']['n'] = 0

            while True:
                try:
                    update_target_post()
                    find_streaks()
                    update_flair()
                    update_target_post()

                    if n_errors['execution']['n'] > 0:
                        utc_time = datetime.fromtimestamp(n_errors['execution']['first_ever_error'])
                        local_time = utc_time.astimezone(pytz.timezone('Europe/Amsterdam'))
                        time_first_error = local_time.strftime('%Y-%m-%d %H:%M:%S %Z')

                        send_email('Execution error solved',f'{n_errors['execution']['n']-1} additional execution errors have happened, but an execution has happened without any errors now, so the error has been solved. The first error occured at {time_first_error}. Latest error message:\n\n{n_errors['execution']['last_message']}')
                        n_errors['execution']['first_error'] = 0
                        n_errors['execution']['n'] = 0
                    break

                except Exception as e:
                    error_count = n_errors['execution']['n']
                    n_errors['execution']['last_message'] = e
                    if error_count == 0:
                        send_email('Execution error',f'An execution error occurred. Error message:\n{e}')
                        n_errors['execution']['first_ever_error'] = time.time()
                        n_errors['execution']['first_error'] = time.time()
                        n_errors['execution']['n'] = 1
                    elif n_errors['execution']['first_error'] < time.time() - 5*60: # Send explanation email every 5 minutes
                        send_email('Execution error',f'Multiple {error_count} execution errors have occurred since last message, and they are still not solved. Latest error message:\n\n{e}')
                        n_errors['execution']['first_error'] = time.time()
                        n_errors['execution']['n'] = 1
                    else:
                        n_errors['execution']['n'] += 1

                    print(f"Error in execution: {e}")
                    time.sleep(30)

    except Exception as e:
        error_count = n_errors['stream']['n']
        n_errors['stream']['last_message'] = e
        if error_count == 0:
            send_email('Stream error',f'A stream error occurred. Error message:\n{e}')
            n_errors['stream']['first_ever_error'] = time.time()
            n_errors['stream']['first_error'] = time.time()
            n_errors['stream']['n'] = 1
        elif n_errors['stream']['first_error'] < time.time() - 5*60: # Send explanation email every 5 minutes
            send_email('Stream error',f'Multiple {error_count} stream errors have occurred since last message, and they are still not solved. Latest error message:\n\n{e}')
            n_errors['stream']['first_error'] = time.time()
            n_errors['stream']['n'] = 1
        else:
            n_errors['stream']['n'] += 1

        print(f"Error in submission stream: {e}")
        time.sleep(30)