# ChickenBotOnceADay

ChickenBotOnceADay is a reddit bot, used to moderate the subreddit r/countwithchickenlady.

This code is shared to give people insight in how this bot works. You are free to use (parts of) this code to create a reddit bot of your own!

## Usage

The code cannot be run in its current state. To run it, you need to:

1. Set up a reddit bot (Create app at https://www.reddit.com/prefs/apps/)
2. Fill in the account details in a file called 'praw.ini'
3. Run the setup_database function
4. Give the bot account mod access in the r/countwithchickenlady subreddit
5. Create a file called .env with information of an email account that can be used to send emails from (a gmail account with an app password), and information of a receiver account. Errors or post deletions are reported using this email adress to the receiver. Syntax:

```python
ACCOUNT=sending_account@gmail.com
PASSWORD=app_password
RECEIVER=receiving_account
```

If you have done this, you should be able to run this bot by running the 'total_update.py' file and the 'flair_deleted_post_update.py' file.

When the bot has been shut down for a while, before turning the bot on again, you should first run the fill_database_after_failure function.

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
