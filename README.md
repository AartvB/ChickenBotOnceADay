# ChickenBotOnceADay
ChickenBotOnceADay is a reddit bot, used to moderate the subreddit r/countwithchickenlady.

This code is shared to give people insight in how this bot works. You are free to use (parts of) this code to create a reddit bot of your own!

## Setup
The code cannot be run in its current state. To run it, you need to:

1. Set up a reddit bot (Create app at https://www.reddit.com/prefs/apps/)
2. Fill in the account details in a file called 'praw.ini'
3. Run the setup_database function
4. Give the bot account mod access in the r/countwithchickenlady subreddit

## Usage
After you finished the setup (see above), you should be able to run this bot by running the ```total_update.py``` file, the ```flair_deleted_post_update.py``` file and the ```update_leaderboards.py``` file.

When the bot has been shut down for a while, before turning the bot on again, you should first run the fill_database_after_failure function.

If someone complains about their user streak (usually via mod mail), you can run the ```check_player_streak``` function. It shows all posts of a user, and their streaks at the time of making the posts. This is very helpful with finding out why someones streak shows unexpected behavior. Usually the problem can be solved by deleting a post from the streak database, using the ```delete_post``` function. If a post must be added to the database, use the ```add_post``` function.

People who started their streak on r/CountOnceADay, but transferred to this subreddit, are allowed to carry over their streak. If someone requests this, use the ```add_COAD_streak``` function.

## Functionalities
The bot does the following things:
- Check if the post is titled correctly. It should be a whole number, exactly 1 higher than the post before it. If it is not titled correctly, it is automatically removed.
- Check if the user does not post more than once per calendar day. It removes any latest post of a user if in the last 2 days, 3 or more posts have been on the same calendar day. We decided to not make it stricter (for example, max. 8 posts in the last 7 calendar days), since that could be confusing to most people. As long as there exists a timezone for which this is not the case, it is okay.
- The posts is not deleted if it is manually approved by a moderator, or if it has been added to the database manually or automatically.
- Update the user streaks. The streak of a user is the number of days the user has posted exactly 1 times per calendar day, until today. If someone hasn't posted for a full day, their post is reset to 0. The bot checks the streak for every possible timezone, and pick the longest streak.
- The user streak is included in the user flair. If the user did not put in a custom flair, the user flair is set to ```Streak: x```, with x the current streak. If the user did put in a custom flair, the user flair is set to ```Custom flair - Streak: x```.
- Update the post that tells user what the correct next number is.
- Update the leaderboards on the wiki, that show interesting statistics of users and posts.
- If a user (or a moderator) removes a post within 10 minutes of it being posted, it is removed from the streak database. If it is removed after 10 minutes, it will not be removed from the streak database, so it will still be used when calculating the streak, and still counts for the 'post once per day' rule. This is done, because otherwise someone might lose their streak if their posts gets deleted after a few hours.
- It stores information on all posts ever posted on this subreddit.
- There are additional functionalities of this bot, which are handled by the code on [this](https://github.com/AartvB/ChickenDiscord) github repository.

## License
MIT License

Copyright (c) 2025 AartvB

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.