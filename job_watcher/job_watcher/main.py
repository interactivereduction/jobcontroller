import os

from database.db_updater import DBUpdater
from job_watcher import JobWatcher
from utils import load_kubernetes_config


# Defaults to 6 hours
MAX_TIME_TO_COMPLETE = os.environ.get("MAX_TIME_TO_COMPLETE_JOB", 60*60*6)

DB_IP = os.environ.get("DB_IP", "")
DB_USERNAME = os.environ.get("DB_USERNAME", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_UPDATER = DBUpdater(ip=DB_IP, username=DB_USERNAME, password=DB_PASSWORD)


def main():
    load_kubernetes_config()
    job_watcher = JobWatcher(db_updater=DB_UPDATER, max_time_to_complete=MAX_TIME_TO_COMPLETE)
    job_watcher.watch()


if __name__ == "__main__":
    main()
