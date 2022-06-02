# TweetCrawler

Crawl geo-tagged streaming tweets.

## Setup

1. Create a virtual environment with Python 3.
    - You may use alternative path and name.

    ```bash
    python3 -m venv /data/TweetCrawler/venv
    ```

2. Install and upgrade some basic Python packages.

    ```bash
    /data/TweetCrawler/venv/bin/python3 -m pip install --upgrade setuptools pip wheel
    ```

3. Install required packages (Tweepy and Google Drive).

    I can't remember what the minimum packages needed, but here are all the packages I'm using.

    ```bash
    /data/TweetCrawler/venv/bin/python3 -m pip install cachetools certifi charset-normalizer elevate google-api-core google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib googleapis-common-protos httplib2 idna oauthlib pkg_resources protobuf pyasn1 pyasn1-modules pyparsing requests requests-oauthlib rsa six tweepy uritemplate urllib3
    ```

4. Put the 2 Python scrpits under `/data/TweetCrawler/Scripts/`.
    - You may use alternative path and name.

5. Put the 2 configuration text files under `/data/TweetCrawler/Configs/`.
    - You may use alternative path and name.

6. Set permissions. We want the everything in the folder, including the scripts, log files, and crawled tweets to be accessible to everyone, except for the configurations.

    - Make the folders accessible:

        ```bash
        sudo chmod 0777 /data/TweetCrawler /data/TweetCrawler/Logs /data/TweetCrawler/Scripts /data/TweetCrawler/Tweets /data/TweetCrawler/venv
        ```

    - Make the scripts executable:

        ```bash
        sudo chmod 0755 /data/TweetCrawler/Scripts/TweetCrawler.py /data/TweetCrawler/Scripts/UploaderAndSweeper.py
        ```

    - Make the configs only accessible by you (and sudoers):

        ```bash
        sudo chmod 0700 /data/TweetCrawler/Configs
        sudo chmod 0600 /data/TweetCrawler/Configs/*
        ```

## Twitter Authentication

Please see [https://docs.tweepy.org/en/latest/authentication.html](https://docs.tweepy.org/en/latest/authentication.html).

I'm using OAuth 1.0a and OAuth 2.0. Type of App `Automated App or bot`. App permissions `Read`.

Edit `crawler_settings.txt`, set the values (right of =) of the following options to what you get from Twitter.

- `twitter_consumer_key`
- `twitter_consumer_secret`
- `twitter_access_key`
- `twitter_access_secret`

## TweetCrawler Settings

- `working_dir`: Where to store the crawled tweets (absolute path to an existing directory).
- `num_threads`: Number of threads for Tweepy. I only use 1.
- `log_file`: Absolute path to the log file for the crawler. The directory must exist.
- `twitter_*`: See the above section.
- `email_recipients`: The crawler repors errors and start/stop info to these emails. Valid format:

  - abc@def.ghi
  - Name <abc@def.ghi>

    Multiple recipients should be separated by `;` or `;`.
- `email_name`: Email title.
- `email_address`: Sender's email.
- `email_smtp`: SMTP server address (hostname or IP).
- `email_port`: SMTP port.
- `email_ssl`: If your SMTP server uses SLL, set this to `true`, otherwise, set to `false`.

If `email_address` or `email_smtp` or `email_recipients` is empty, the crawler does not send any email.

## Run the Crawler

```bash
/data/TweetCrawler/venv/bin/python3 /data/TweetCrawler/Scripts/TweetCrawler.py /data/TweetCrawler/Configs/crawler_settings.txt
```

The `python3` interpreter must be the one you have in the venv, which has `Tweepy` installed.

## Google Drive Authentication

I followed mostly from [https://developers.google.com/drive/api/quickstart/python](https://developers.google.com/drive/api/quickstart/python)

1. Go to [https://console.cloud.google.com/getting-started](https://console.cloud.google.com/getting-started).
2. Create a new project.
3. Go to Credentials &rarr; Create credentials &rarr; OAuth client ID.
4. Application type: `Desktop app`, give a name.
5. Download the client secret json file.
6. In `uploader_settings.txt`, set the value of `google_drive_client_secrets_json` to the absolute path of the downloaded json file.

## Uploader Settings

- `working_dir`: Where to store the crawled tweets (absolute path to an existing directory).
- `log_file`: Absolute path to the log file for the uploader. The directory must exist.
- `google_drive_client_secrets_json`: See above section.
- `google_drive_settings_yaml`: Deprecated.
- `google_drive_token_pickle`: File for stored Google Drive credentials. It will be created automatically from code, and reused in future executions.
- `google_drive_folder_id`: The ID of the Google Drive folder.
- `keep_files_for_days`: Keep N days of crawled tweets. Set to 0 to keep forever.
- `deduplicate`: If multithreading is used in the crawler, there might be duplicate tweets in the file. Set this option to `true` to deduplicate (which does merge sort, and can be slow). If you use single thread, set this to `false`.
- `email_*`: Same as crawler.

## Run the Uploader

The uploader will zip all possible tweets for one day (24 files from 00 to 23), and upload the zip file to Google Drive.

There is no need to run the uploader more than once per day.

## Crontab

- To start the crawler automatically after a reboot:

    ```text
    @reboot tmux new-session -d -s "TweetCrawler" "/data/TweetCrawler/venv/bin/python3 /data/TweetCrawler/Scripts/TweetCrawler.py /data/TweetCrawler/Configs/crawler_settings.txt"
    ```

- For now, I let the crawler restart once per week.

    To stop the crawler at 1:00 am every Monday:

    ```text
    0 1 * * 1 tmux kill-session -t "TweetCrawler"
    ```

    To start the crawler after 1 minute at 1:01 am:

    ```text
    1 1 * * 1 tmux new-session -d -s "TweetCrawler" "/data/TweetCrawler/venv/bin/python3 /data/TweetCrawler/Scripts/TweetCrawler.py /data/TweetCrawler/Configs/crawler_settings.txt"
    ```

- To run the uploader at 4 am every day:

    ```text
    0 4 * * * /data/TweetCrawler/venv/bin/python3 /data/TweetCrawler/Scripts/UploaderAndSweeper.py /data/TweetCrawler/Configs/uploader_settings.txt
    ```

## Notes

Sometimes the crawler may be blocked for different reasons. It can be blocked by Twitter, some network issue may prevent the crawler from running, etc. There may be no files generated in a few hours, missing necessary files to zip.

For example, in one day, you have all the files but missing *-04 (no tweets crawled between UTC time [4am, 5am)), in order for the uploader to run, you can use `touch` command to create an empty file with that name. As long as the uploader can find this file, it can make the zip and upload it.
