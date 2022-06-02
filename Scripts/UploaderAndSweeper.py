#!/usr/bin/env python3

import glob
import json
import os
import pathlib
import pickle
import smtplib
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from apiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

if len(sys.argv) != 2:
    print(f"Usage: {os.path.basename(__file__)} SETTINGS_FILE")
    sys.exit(0)

__setting_path = os.path.abspath(sys.argv[1])
if not os.path.isfile(__setting_path):
    print(f"Cannot find {__setting_path}", file = sys.stderr)
    sys.exit(-1)

KEY_WORKING_DIR = "working_dir"
KEY_LOG_File = "log_file"
KEY_GOOGLE_DRIVE_CLIENT_SECRETS_JSON = "google_drive_client_secrets_json"
# KEY_GOOGLE_DRIVE_SETTINGS_YAML = "google_drive_settings_yaml"
KEY_GOOGLE_DRIVE_TOKEN_PICKLE = "google_drive_token_pickle"
KEY_GOOGLE_DRIVE_FOLDER_ID = "google_drive_folder_id"
KEY_KEEP_FILES_FOR_DAYS = "keep_files_for_days"
KEY_DEDUPLICATE = "deduplicate"
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
__log_path = None
__log_file = sys.stdout
__gdrive_client_secret = None
__gdrive_settings = None
__gdrive_folder_id = None
__keep_days = None
__dedup = False
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
            key, val = read_setting(line.replace("\r", "").replace("\n", ""))
            if key == KEY_WORKING_DIR:
                __working_dir = os.path.abspath(val)
            elif key == KEY_LOG_File:
                if len(val) > 0:
                    __log_path = os.path.abspath(val)
            elif key == KEY_GOOGLE_DRIVE_CLIENT_SECRETS_JSON:
                __gdrive_client_secret = os.path.abspath(val)
                if not os.path.isfile(__gdrive_client_secret):
                    print(
                        f"Invalid setting ("
                        f"{KEY_GOOGLE_DRIVE_CLIENT_SECRETS_JSON}): Cannot "
                        f"find {__gdrive_client_secret}",
                        file = sys.stderr)
                    sys.exit(-1)
            # elif key == KEY_GOOGLE_DRIVE_SETTINGS_YAML:
            #     __gdrive_settings = os.path.abspath(val)
            elif key == KEY_GOOGLE_DRIVE_TOKEN_PICKLE:
                __gdrive_settings = os.path.abspath(val)
            elif key == KEY_GOOGLE_DRIVE_FOLDER_ID:
                if len(val) > 0:
                    __gdrive_folder_id = val
            elif key == KEY_KEEP_FILES_FOR_DAYS:
                try:
                    __keep_days = int(val)
                except ValueError as e:
                    print(f"Invalid setting ({KEY_KEEP_FILES_FOR_DAYS}): {e}",
                          file = sys.stderr)
                    sys.exit(-1)
                if __keep_days < 0:
                    print(
                        f"Invalid setting ({KEY_KEEP_FILES_FOR_DAYS}): Must "
                        f"be at least 0",
                        file = sys.stderr)
                    sys.exit(-1)
            elif key == KEY_DEDUPLICATE:
                __dedup = (val.lower() != "false")
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
                    print(f"Incorrect SMTP port: {val}",
                          file = sys.stderr)
                if __email_port < 1:
                    print(f"Incorrect SMTP port: {val}",
                          file = sys.stderr)
                    __email_port = -1
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
    print(f"Cannot find {__working_dir}", file = sys.stderr)
    sys.exit(-1)

if __gdrive_client_secret is None or len(__gdrive_client_secret) == 0:
    print(f"{KEY_GOOGLE_DRIVE_CLIENT_SECRETS_JSON} is not set",
          file = sys.stderr)
    sys.exit(-1)

if __gdrive_settings is None or len(__gdrive_settings) == 0:
    print(f"{KEY_GOOGLE_DRIVE_TOKEN_PICKLE} is not set",
          file = sys.stderr)
    sys.exit(-1)

dt_today = datetime.today()
weekly_digest_file = ""
if len(__log_path) > 0:
    if dt_today.isoweekday() == 7 and os.path.isfile(__log_path):
        dt_start = dt_today - timedelta(days = 7)
        str_start = dt_start.strftime("%Y%m%d")
        str_today = dt_today.strftime("%Y%m%d")
        log_name = os.path.basename(__log_path)
        if "." in log_name:
            log_ext = pathlib.Path(log_name).suffix
            log_base = log_name[0:-len(log_ext)]
            bak_name = f"{log_base}.{str_start}-{str_today}{log_ext}"
        else:
            bak_name = f"{log_name}.{str_start}-{str_today}"
        bak_path = os.path.join(os.path.dirname(__log_path), bak_name)
        if os.path.isfile(bak_path):
            os.remove(__log_path)
        else:
            os.rename(__log_path, bak_path)
        weekly_digest_file = bak_path
    __log_file = open(__log_path, "a")


def now_to_str():
    return datetime.now().strftime("[%m/%d/%Y %H:%M:%S]")


def cout(msg: str):
    if len(__log_path) > 0:
        print(f"{now_to_str()} {msg}", file = __log_file)
        __log_file.flush()
    print(msg, file = sys.stdout)


def cerr(msg: str):
    if len(__log_path) > 0:
        print(f"{now_to_str()} {msg}", file = __log_file)
        __log_file.flush()
    print(msg, file = sys.stderr)


if __gdrive_folder_id is None or len(__gdrive_folder_id) == 0:
    cout("Files will be uploaded to Google Drive's root")

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

# Google Drive authentication
SCOPES = ["https://www.googleapis.com/auth/drive"]
__creds = None
if os.path.exists(__gdrive_settings):
    with open(__gdrive_settings, "rb") as token:
        __creds = pickle.load(token)
if not __creds or not __creds.valid:
    if __creds and __creds.expired and __creds.refresh_token:
        __creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(__gdrive_client_secret,
                                                         SCOPES)
        __creds = flow.run_console()
    # Save the credentials for the next run
    with open(__gdrive_settings, "wb") as token:
        pickle.dump(__creds, token)

__service = None
try:
    __service = build("drive", "v3", credentials = __creds)
except BaseException as be:
    cerr(f"Failed to initialize Google Drive: {be}")


def send_email(subject: str, msg: str, attachments = None):
    if attachments is None:
        attachments = []
    if __email_address is None:
        return

    body = MIMEMultipart()
    body["Subject"] = subject
    if __email_name is None or len(__email_name) == 0:
        body["From"] = __email_address
    else:
        body["From"] = "{0} <{1}>".format(__email_name, __email_address)
    body["To"] = ", ".join(__email_recipients)

    if len(msg) > 0:
        body.attach(MIMEText(msg))
    if len(attachments) > 0:
        for attachment in attachments:
            with open(attachment, "rb") as af:
                part = MIMEApplication(af.read(),
                                       Name = os.path.basename(attachment))
            part[
                "Content-Disposition"] = f"attachment; filename=\"" \
                                         f"{os.path.basename(attachment)}\""
            body.attach(part)

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
        cerr(f"Failed to send email: {e}")


try:
    df = __service.files().get(fileId = __gdrive_folder_id).execute()
    gdrive_dir_name = df["name"]
    cout(f"Name of {__gdrive_folder_id} is \"{gdrive_dir_name}\"")
except BaseException as be:
    cerr(f"Failed to get name of {__gdrive_folder_id}: {be}")
    send_email(f"[TweetCrawler]: Failed to get name of {__gdrive_folder_id}",
               str(be))
    sys.exit(-1)

current = datetime.now(tz = timezone.utc)  # Current UTC date
if __keep_days is None or __keep_days == 0:
    cout("All zip files will be kept")
else:
    last_date = (current - timedelta(days = __keep_days)).strftime("%Y-%m-%d")
    cout(f"Zip files older than {last_date} "
         f"(excluding {last_date}) will be removed")


def upload_to_google_drive(path: str) -> bool:
    """ Upload the zip file to Google Drive """
    zp = os.path.abspath(path)
    zn = os.path.basename(path)

    if not os.path.isfile(f"{zp}.ready"):
        cout(f"{zn} is not ready")
        return False
    if os.path.isfile(f"{zp}.uploaded"):
        cout(f"{zn} is already uploaded")
        return True
    if os.path.isfile(f"{zp}.uploading"):
        cout(f"{zn} is being uploaded")
        return False

    # Set the flag file to be uploading status
    os.rename(f"{zp}.ready", f"{zp}.uploading")
    try:
        if __gdrive_folder_id is None or len(__gdrive_folder_id) == 0:
            file_metadata = {"name": zn}
            cout(f"Uploading {zn} to root")
        else:
            file_metadata = {"name": zn, "parents": [__gdrive_folder_id]}
            cout(f"Uploading {zn} to folder \"{gdrive_dir_name}\"")
        media = MediaFileUpload(zp, mimetype = "application/zip",
                                resumable = True)
        file = __service.files().create(body = file_metadata,
                                        media_body = media,
                                        fields = "id").execute()
    except BaseException as be:
        cerr(f"Failed to upload {zn}: {be}")
        send_email(f"[TweetCrawler]: Failed to upload {zn}", str(be))
        return False
    # Set the flag file to be uploaded status
    os.rename(f"{zp}.uploading", f"{zp}.uploaded")
    cout("Uploaded")
    return True


def filename_to_datetime(filename: str) -> datetime:
    basename = os.path.basename(filename).split(".")[0]
    return datetime.strptime(
        f"{basename}:00:00.000001 +0000",
        "tweets-%Y%m%d-%H:%M:%S.%f %z")


def zipname_to_datetime(filename: str) -> datetime:
    basename = os.path.basename(filename).split(".")[0]
    return datetime.strptime(
        f"{basename}-00:00:00.000001 +0000",
        "tweets-%Y%m%d-%H:%M:%S.%f %z")


def finish_files(save_path: str) -> None:
    """" Search for any unfinished tmp files and rename them """
    for f in glob.glob(os.path.join(save_path,
                                    "tweets-"
                                    # 4 digits year, 2 digits months and day
                                    "20[0-9][0-9][01][0-9][0-3][0-9]-"
                                    # 2 digits minutes
                                    "[0-2][0-9].tmp")):
        file_time = filename_to_datetime(f)
        diff_sec = (datetime.now(tz = timezone.utc) - file_time).total_seconds()
        if diff_sec >= 125 * 60:  # Differ by 2 hours 5 minutes
            try:
                os.rename(f, f[:-4])
                cout(
                    f"Renamed {os.path.basename(f)} to "
                    f"{os.path.basename(f[:-4])}")
            except:
                cerr(
                    f"Failed to rename {os.path.basename(f)} to "
                    f"{os.path.basename(f[:-4])}")


def deduplicate(path: str):
    tweets = {}
    num_lines = 0
    with open(path, "r") as inf:
        for line in inf:
            line = line.rstrip("\n")
            if len(line) == 0:
                continue
            t = json.loads(line)
            tid = int(t["id"])
            tweets[tid] = line
            num_lines += 1
    if num_lines < 2 or len(tweets) == num_lines:
        return
    with open(path, "w") as outf:
        for tid in sorted(tweets.keys()):
            outf.write(tweets[tid] + "\n")
    cout(
        f"Deduplicate {os.path.basename(path)} from {num_lines} to "
        f"{len(tweets)}")


def zip_tweets(save_path: str) -> None:
    """ Zip all text files, group by day """
    days = set()
    for f in glob.glob(os.path.join(save_path,
                                    "tweets-"
                                    # 4 digits year, 2 digits months and day
                                    "20[0-9][0-9][01][0-9][0-3][0-9]-"
                                    # 2 digits minutes
                                    "[0-2][0-9]")):
        fn = os.path.basename(f)
        day_str = fn.split("-")[1]
        days.add(day_str)
    for day_str in sorted(list(days)):
        files = []
        for hour in range(24):
            if hour < 10:
                f = os.path.join(__working_dir, f"tweets-{day_str}-0{hour}")
            else:
                f = os.path.join(__working_dir, f"tweets-{day_str}-{hour}")
            if os.path.isfile(f):
                files.append(f)
            else:
                break
        if len(files) == 24:
            zipp = os.path.join(__working_dir, f"tweets-{day_str}.zip")
            flagp = os.path.join(__working_dir, f"tweets-{day_str}.zip.ready")
            if not os.path.isfile(flagp):
                # Create zip
                zf = zipfile.ZipFile(zipp, "w")
                # Add to zip in order
                for f in files:
                    if __dedup:
                        deduplicate(f)
                    os.chmod(f, 0o644)
                    fn = os.path.basename(f)
                    zf.write(f, fn, zipfile.ZIP_DEFLATED, 9)
                    cout(f"Zipped {fn} (size = {os.path.getsize(f)})")
                zf.close()  # Finish the zip file
                del zf
                os.chmod(zipp, 0o644)

                # Create an empty flag file to indicate the zip file has been
                # created successfully
                outf = open(flagp, "w")
                outf.close()
                del outf
                os.chmod(flagp, 0o644)

                # Remove original files
                for f in files:
                    os.remove(f)
                    cout(f"Removed {os.path.basename(f)}")
                cout(f"Created {day_str}.zip")
        else:
            cout(f"Not completed {day_str}")
        del files
    del days


def worker(save_path: str) -> None:
    """ Check all files """
    # Check if any tmp file is unfinished
    finish_files(save_path)

    # Find files to be zipped
    zip_tweets(save_path)

    files_uploaded = []
    files_cleaned = []

    # Find files to be uploaded
    # Find all zips
    for f in sorted(glob.glob(
            os.path.join(save_path,
                         "tweets-"
                         # 4 digits year, 2 digits months and day
                         "20[0-9][0-9][01][0-9][0-3][0-9].zip"))):
        if os.path.isfile("{0}.ready".format(f)) \
                and not os.path.isfile(f"{f}.uploaded") \
                and not os.path.isfile(f"{f}.uploading"):
            # Upload to Google Drive first, if successful, FILENAME.uploaded
            # should be found
            if upload_to_google_drive(f):
                files_uploaded.append(os.path.basename(f))

        if os.path.isfile(f"{f}.uploading"):
            # The zip file is being uploaded or aborted at some place,
            # try re-upload
            fdate = zipname_to_datetime(f)  # UTC date of the zip file
            if (current - fdate).days >= 2:
                # Only retry after 2 days
                # Restore the flag file
                os.rename(f"{f}.uploading", f"{f}.ready")
                # Re-upload
                if upload_to_google_drive(f):
                    files_uploaded.append(os.path.basename(f))

        if os.path.isfile(f"{f}.uploaded"):
            # The zip file had been uploaded
            fdate = zipname_to_datetime(f)  # UTC date of the zip file
            if __keep_days is not None and __keep_days > 0:
                # Sweeping is enabled
                if (current - fdate).days > __keep_days:
                    # The file is too old
                    os.remove(f)  # Remove the zip file
                    os.remove(f"{f}.uploaded")  # Remove the flag file
                    cout(f"Cleaned {os.path.basename(f)}")
                    files_cleaned.append(os.path.basename(f))

    # Clean some junk files
    for f in sorted(glob.glob(
            os.path.join(save_path,
                         "tweets-"
                         # 4 digits year, 2 digits months and day
                         "20[0-9][0-9][01][0-9][0-3][0-9].zip.ready"))):
        zipf = f[:-6]  # zip file
        if not os.path.isfile(zipf):
            os.remove(f)  # Remove the flag file if the zip file does not exist
            cout(f"Cleaned {os.path.basename(f)}")
            files_cleaned.append(os.path.basename(f))
    for f in sorted(glob.glob(
            os.path.join(save_path,
                         "tweets-"
                         # 4 digits year, 2 digits months and day
                         "20[0-9][0-9][01][0-9][0-3][0-9].zip.uploaded"))):
        zipf = f[:-9]  # zip file
        if not os.path.isfile(zipf):
            os.remove(f)  # Remove the flag file if the zip file does not exist
            cout(f"Cleaned {os.path.basename(f)}")
            files_cleaned.append(os.path.basename(f))
    for f in sorted(glob.glob(
            os.path.join(save_path,
                         "tweets-"
                         # 4 digits year, 2 digits months and day
                         "20[0-9][0-9][01][0-9][0-3][0-9].zip.uploading"))):
        zipf = f[:-10]  # zip file
        if not os.path.isfile(zipf):
            os.remove(f)  # Remove the flag file if the zip file does not exist
            cout(f"Cleaned {os.path.basename(f)}")
            files_cleaned.append(os.path.basename(f))

    # Only report a digest of the whole week on Sunday
    if datetime.today().isoweekday() == 7 and len(
            weekly_digest_file) > 0 and os.path.isfile(weekly_digest_file):
        send_email(f"[TweetCrawler]: Weekly Digest", "", [weekly_digest_file, ])


if __name__ == "__main__":
    worker(__working_dir)
