from job_watcher import JobWatcher
from utils import load_kubernetes_config


def main():
    load_kubernetes_config()
    job_watcher = JobWatcher()
    job_watcher.watch()


if __name__ == "__main__":
    main()
