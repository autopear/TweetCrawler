#!/usr/bin/env python3

import json
import os
import smtplib
import socket
import sys
import time
import traceback
import zipfile
from datetime import datetime, timezone
from email.mime.text import MIMEText
from http.client import IncompleteRead as http_incompleteRead
from io import StringIO
from subprocess import call
from threading import Lock, Thread
from typing import Callable, TextIO
from urllib.request import urlopen

import tweepy  # Requires Tweepy 4.0.0+
from urllib3.exceptions import IncompleteRead as urllib3_incompleteRead

if len(sys.argv) != 2:
    print("Usage: {0} SETTINGS_FILE".format(os.path.basename(__file__)))
    sys.exit(0)

__setting_path = os.path.abspath(sys.argv[1])
if not os.path.isfile(__setting_path):
    print("Cannot find {0}".format(__setting_path), file = sys.stderr)
    sys.exit(-1)

KEY_WORKING_DIR = "working_dir"
KEY_NUM_THREADS = "num_threads"
KEY_LOG_File = "log_file"
KEY_TWITTER_BEAR_TOKEN = "twitter_bear_token"
KEY_EMAIL_ADDRESS = "email_address"
KEY_EMAIL_NAME = "email_name"
KEY_EMAIL_PASSWORD = "email_password"
KEY_EMAIL_SMTP = "email_smtp"
KEY_EMAIL_PORT = "email_port"
KEY_EMAIL_SSL = "email_ssl"
KEY_EMAIL_RECIPIENTS = "email_recipients"


def read_setting(line: str):
    if not line or line.startswith("#"):
        return None, None
    try:
        idx = line.index("=")
        if idx > -1:
            key = line[:idx].strip()
            val = line[idx + 1:].strip()
            return key, val
        else:
            return None, None
    except ValueError:
        return None, None


__working_dir = None
__num_threads = 1
__log_path = None
__log_file = None
__log_lock = None
__twitter_bear_token = None
__email_address = None
__email_name = None
__email_password = None
__email_smtp = None
__email_port = -1
__email_ssl = True
__email_recipients = None

try:
    with open(__setting_path, "r") as inf:
        for line in inf:
            key, val = read_setting(line.rstrip("\n").rstrip("\r"))
            if key == KEY_WORKING_DIR:
                __working_dir = os.path.abspath(val)
            elif key == KEY_NUM_THREADS:
                try:
                    __num_threads = int(val)
                except ValueError:
                    print(f"Incorrect number of threads: {val}",
                          file = sys.stderr)
                if __num_threads < 1:
                    print(f"Incorrect number of threads: {val}",
                          file = sys.stderr)
                    __num_threads = 1
            elif key == KEY_LOG_File:
                try:
                    __log_path = os.path.abspath(val)
                    __log_file = open(__log_path, "a")
                    __log_lock = Lock()
                except BaseException as fe:
                    print(f"Failed to write to {__log_path}", file = sys.stderr)
                    __log_path = None
                    __log_file = None
                    __log_lock = None
            elif key == KEY_TWITTER_BEAR_TOKEN:
                __twitter_bear_token = val
            elif key == KEY_EMAIL_ADDRESS:
                __email_address = val
            elif key == KEY_EMAIL_NAME:
                __email_name = val
            elif key == KEY_EMAIL_PASSWORD:
                __email_password = val
            elif key == KEY_EMAIL_SMTP:
                __email_smtp = val
            elif key == KEY_EMAIL_PORT:
                try:
                    __email_port = int(val)
                except ValueError:
                    print(f"Incorrect SMTP port: {val}", file = sys.stderr)
                if __num_threads < 1:
                    print(f"Incorrect SMTP port: {val}", file = sys.stderr)
                    __num_threads = -1
            elif key == KEY_EMAIL_SSL:
                __email_ssl = (val.lower() != "false")
            elif key == KEY_EMAIL_RECIPIENTS:
                __email_recipients = [v.strip() for v in val.split(";")]
            else:
                continue
    inf.close()
except BaseException as e:
    print(f"Cannot read {__setting_path}: {e}", file = sys.stderr)
    sys.exit(-1)

if __working_dir is None or len(__working_dir) == 0:
    print(f"{KEY_WORKING_DIR} is not set", file = sys.stderr)
    sys.exit(-1)

if not os.path.isdir(__working_dir):
    try:
        os.mkdir(__working_dir)
    except BaseException as e:
        print(f"Failed to create directory {__working_dir}: {e}",
              file = sys.stderr)
        sys.exit(-1)

if __twitter_bear_token is None or len(__twitter_bear_token) == 0:
    print(f"{KEY_TWITTER_BEAR_TOKEN} is not set", file = sys.stderr)
    sys.exit(-1)

# Email
if __email_address is None or len(__email_address) == 0 \
        or __email_smtp is None or len(__email_smtp) == 0 \
        or __email_port < 1 \
        or __email_recipients is None or len(__email_recipients) == 0:
    __email_address = None
    __email_name = None
    __email_password = None
    __email_smtp = None
    __email_port = -1
    __email_recipients = None


def write_log(msg: str, error: bool = False):
    global __log_file
    __log_lock.acquire()
    if error:
        sys.stderr.write(msg)
        if not msg.endswith("\n"):
            sys.stderr.write("\n")
        sys.stderr.flush()
    else:
        sys.stdout.write(msg)
        if not msg.endswith("\n"):
            sys.stdout.write("\n")
        sys.stdout.flush()
    if __log_file is not None:
        t = datetime.now().strftime("%y-%m-%d %H:%M:%S.%f")
        if error:
            __log_file.write(f"{t} ERROR {msg}\n")
        else:
            __log_file.write(f"{t} INFO {msg}\n")
        __log_file.flush()
        if __log_file.tell() >= 4194304:
            __log_file.close()
            base, ext = os.path.splitext(__log_path)
            zipid = 1
            zipp = f"{base}-{zipid}{ext}.zip"
            while os.path.exists(zipp):
                zipid += 1
                zipp = f"{base}-{zipid}{ext}.zip"
            with zipfile.ZipFile(zipp, "w") as zf:
                zf.write(__log_path, f"{base}-{zipid}{ext}",
                         zipfile.ZIP_DEFLATED)
            zf.close()
            try:
                os.remove(__log_path)
            except OSError:
                __log_file = open(__log_path, "w")
                __log_file.close()
            __log_file = open(__log_path, "a")
    __log_lock.release()


__open_files = {}
__file_lock = Lock()


def send_email(subject: str, msg: str) -> None:
    if __email_address is None:
        return

    body = MIMEText(msg)
    body["Subject"] = subject
    if __email_name is None or len(__email_name) == 0:
        body["From"] = __email_address
    else:
        body["From"] = f"{__email_name} <{__email_address}>"
    body["To"] = ", ".join(__email_recipients)

    try:
        if __email_ssl:
            email = smtplib.SMTP_SSL(__email_smtp, __email_port)
        else:
            email = smtplib.SMTP(__email_smtp, __email_port)
        if __email_password is not None and len(__email_password) > 0:
            email.login(__email_address, __email_password)
        email.sendmail(__email_address, __email_recipients, body.as_string())
        email.close()
        del email
    except Exception as e:
        print(f"Failed to send email: {e}", file = sys.stderr)


def merge_saved_file(tmp_path: str, saved_path: str) -> None:
    with open(tmp_path, "r") as inf, open(saved_path, "a") as outf:
        for line in inf:
            outf.write(line)
    inf.close()
    outf.close()
    os.remove(tmp_path)


def create_or_get_file(timestamp: datetime) -> (TextIO, Lock):
    global __open_files
    """ Get a file to write for given date and time, create if not exists """
    target_key = int(timestamp.strftime("%y%m%d%H00"))
    created = False

    __file_lock.acquire()
    if target_key in __open_files.keys():
        # Get file and lock
        target_file, target_lock, target_name, target_tmp = __open_files[
            target_key]
    else:
        # Create file and lock
        target_name = timestamp.strftime("tweets-%Y%m%d-%H")
        target_tmp = f"{target_name}.tmp"
        target_file = open(os.path.join(__working_dir, target_tmp), "a")
        target_lock = Lock()
        __open_files[target_key] = (
            target_file, target_lock, target_name, target_tmp)
        created = True
    __file_lock.release()
    if created:
        write_log(f"Created {target_tmp}", False)
    merged = []
    finished = []
    __file_lock.acquire()
    if len(__open_files) > 1:
        # Clean some outdated files and locks
        current_key = int(datetime.now(tz = timezone.utc)
                          .strftime("%y%m%d%H%M"))
        keys = sorted(list(__open_files.keys()))
        for old_key in keys:
            if current_key - old_key >= 205:
                # Current is at least 2 hour and 5 minutes later than the
                # timestamp of the file
                old_file, old_lock, old_name, old_tmp = __open_files[
                    old_key]
                if old_lock.acquire(blocking = False):
                    old_file.flush()
                    old_file.close()
                    del old_file
                    tmp_path = os.path.join(__working_dir, old_tmp)
                    saved_path = os.path.join(__working_dir, old_name)
                    if os.path.isfile(saved_path):
                        merge_saved_file(tmp_path, saved_path)
                        merged.append((old_tmp, old_name))
                    else:
                        os.rename(tmp_path, saved_path)
                    del __open_files[old_key]
                    old_lock.release()
                    del old_lock
                    finished.append(old_name)
    __file_lock.release()
    for old_tmp, old_name in merged:
        write_log(f"Merged {old_tmp} to {old_name}", False)
    for old_name in finished:
        write_log(f"Finished {old_name}", False)
    return target_file, target_lock


def close_all_files() -> None:
    global __open_files
    """ Close all open files in case of errors """
    __file_lock.acquire()
    keys = list(__open_files.keys())
    for c_ts in keys:
        c_file, c_lock, c_fn, c_tmp = __open_files[c_ts]
        with c_lock:
            try:
                c_file.flush()
                c_file.close()
                del c_file
            except:
                pass
        del c_lock
        del __open_files[c_ts]
    __file_lock.release()


def trim_json(jobj) -> bool:
    """ Remove any empty fields in a json object recursively. """
    if jobj is None:
        return True
    if type(jobj) is dict:
        keys = list(jobj.keys())
        for key in keys:
            if trim_json(jobj[key]):
                del jobj[key]
        return len(jobj) == 0
    if type(jobj) is list:
        for i in range(len(jobj) - 1, -1, -1):
            if trim_json(jobj[i]):
                del jobj[i]
        return len(jobj) == 0
    if type(jobj) is str:
        return len(jobj) == 0
    return False


def save_tweet(data: str) -> bool:
    """ Save crawled tweets to file in thread-safe way """
    try:
        tweet = json.loads(data)
    except ValueError:
        return False
    if "data" not in tweet or "text" not in tweet["data"]:
        # Filter none Tweets
        return False
    if "matching_rules" in tweet:
        del tweet["matching_rules"]
    if trim_json(tweet):
        return False
    data = json.dumps(tweet, separators = (",", ":"), sort_keys = True) + "\n"
    timestamp = datetime.strptime(tweet["data"]["created_at"],
                                  "%Y-%m-%dT%H:%M:%S.%fZ")
    file, lock = create_or_get_file(timestamp)
    lock.acquire()
    if file.closed:
        lock.release()
        return False
    try:
        file.write(data)  # Save the crawled tweet
    except BaseException as ex:
        lock.release()
        write_log(f"Error on_data: {ex}", True)
        return False
    lock.release()
    return True


class CrawlerStream(tweepy.StreamingClient):
    """ Custom class for steaming Tweets """

    def __init__(self, bearer_token: str,
                 save_func: Callable, log_func: Callable):
        """ Keyword arguments:
        save_func -- thread-safe function to write tweet to file
        log_func -- thread-safe function to write to log
        """
        super(CrawlerStream, self).__init__(bearer_token,
                                            wait_on_rate_limit = True)
        self.__saveFunc = save_func
        self.__logFunc = log_func
        self.__expansions = [
            "author_id",
            "referenced_tweets.id",
            "in_reply_to_user_id",
            "attachments.media_keys",
            "attachments.poll_ids",
            "geo.place_id",
            "entities.mentions.username",
            "referenced_tweets.id.author_id",
        ]

        # https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/media
        self.__media_fields = [
            "media_key",
            "type",
            "url",
            "duration_ms",
            "height",
            # "non_public_metrics",
            # "organic_metrics",
            "preview_image_url",
            # "promoted_metrics",
            "public_metrics",
            "width",
            "alt_text",
            "variants",
        ]

        # https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/place
        self.__place_fields = [
            "full_name",
            "id",
            "contained_within",
            "country",
            "country_code",
            "geo",
            "name",
            "place_type",
        ]

        # https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/poll
        self.__poll_fields = [
            "id",
            "options",
            "duration_minutes",
            "end_datetime",
            "voting_status",
        ]

        # https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
        self.__tweet_fields = [
            "id",
            "text",
            "attachments",
            "author_id",
            "context_annotations",
            "conversation_id",
            "created_at",
            "entities",
            "geo",
            "in_reply_to_user_id",
            "lang",
            "non_public_metrics",
            "organic_metrics",
            "possibly_sensitive",
            "promoted_metrics",
            "public_metrics",
            "referenced_tweets",
            "reply_settings",
            "source",
            "withheld",
        ]

        # https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/user
        self.__user_fields = [
            "id",
            "name",
            "username",
            "created_at",
            "description",
            "entities",
            "location",
            "pinned_tweet_id",
            "profile_image_url",
            "protected",
            "public_metrics",
            "url",
            "verified",
            "withheld",
        ]

    def on_exception(self, exception):
        time.sleep(5)
        raise Exception(f"Encountered error with exception: {exception}")

    def on_data(self, raw_data: bytes) -> bool:
        try:
            self.__saveFunc(raw_data.decode("utf-8"))
        except (http_incompleteRead, urllib3_incompleteRead):
            time.sleep(5)
        return True  # Continue crawling

    # def on_request_error(self, status_code: int):
    #     time.sleep(5)
    #     raise Exception(f"Encountered error with status code: {status_code}")

    def on_limit(self, track: int):
        time.sleep(5)
        raise Exception(f"Encountered rate limited {track}")

    def start_sample(self):
        self.sample(media_fields = self.__media_fields,
                    place_fields = self.__place_fields,
                    poll_fields = self.__poll_fields,
                    tweet_fields = self.__tweet_fields,
                    user_fields = self.__user_fields,
                    expansions = self.__expansions)


class Crawler(Thread):
    """ Run listener in thread """

    def __init__(self, bearer_token: str, save_func: Callable,
                 log_func: Callable):
        """ Keyword arguments:
        bearer_token -- bearer token information
        save_func -- thread-safe function to write tweet to file
        log_func -- thread-safe function to write to log
        """
        Thread.__init__(self)
        self.__bearer_token = bearer_token
        self.__saveFunc = save_func
        self.__logFunc = log_func

    def run(self):
        cs = CrawlerStream(self.__bearer_token,
                           self.__saveFunc,
                           self.__logFunc)
        # Filter geo enabled Tweets
        cs.start_sample()


def get_time() -> bool:
    try:
        utcdata = urlopen(
            "http://worldtimeapi.org/api/timezone/America/Los_Angeles").read(
        ).strip().decode(
            "utf-8")
    except Exception as e:
        print(e)
        return False

    time_obj = json.loads(utcdata)

    utcdt = datetime.strptime(time_obj["utc_datetime"],
                              "%Y-%m-%dT%H:%M:%S.%f+00:00")
    sysdt = utcdt.replace(tzinfo = timezone.utc).astimezone(tz = None)
    sysstr = sysdt.strftime("%m/%d/%Y %H:%M:%S")

    try:
        call(f"sudo hwclock --set --date \"{sysstr}\"", shell = True)
        call("sudo hwclock -s", shell = True)
    except Exception as e:
        print(e)
        return False
    print(f"Time synced at {sysstr}")
    return True


if __name__ == "__main__":
    silent_start = False
    host = socket.gethostname()
    while True:
        if not silent_start:
            now_str = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            content = f"Started at {now_str} on {host}"
            send_email(f"[TweetCrawler]: {content}", content)
        try:
            if __num_threads == 1:
                cs = CrawlerStream(__twitter_bear_token, save_tweet, write_log)
                cs.start_sample()
            else:
                for _ in range(__num_threads):
                    Crawler(__twitter_bear_token, save_tweet, write_log).start()
        except (KeyboardInterrupt, SystemExit):
            close_all_files()
            if __log_file is not None:
                __log_file.close()
            raise
        except (http_incompleteRead, urllib3_incompleteRead):
            close_all_files()
            sio = StringIO()
            traceback.print_exc(file = sio)
            msg = sio.getvalue()
            sio.close()
            write_log(msg, True)
            time.sleep(10)
            silent_start = True
        except Exception as ex:
            close_all_files()
            sio = StringIO()
            traceback.print_exc(file = sio)
            msg = sio.getvalue()
            sio.close()
            write_log(msg, True)
            if "ValueError: invalid literal for int() with base 16: b''" in \
                    msg and \
                    "http.client.IncompleteRead: IncompleteRead(0 bytes " \
                    "read)" in msg:
                time.sleep(10)
                silent_start = True
            else:
                now_str = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
                send_email(f"[TweetCrawler]: Stopped at {now_str} on {host}",
                           msg)
                if "Encountered error with status code: 401" in str(ex):
                    get_time()
                    time.sleep(30)
                else:
                    time.sleep(240)
                silent_start = False
