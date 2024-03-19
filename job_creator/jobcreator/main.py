"""
Main class, creates jobs by calling to the jobcreator, creates the jobwatcher for each created job, and receives
requests from the topicconsumer.
"""
import asyncio
import os
import uuid
from pathlib import Path
from typing import Dict, Any

from jobcreator.database.db_updater import DBUpdater
from jobcreator.job_creator import JobCreator
from jobcreator.queue_consumer import QueueConsumer
from jobcreator.script_aquisition import acquire_script
from jobcreator.utils import logger, create_ceph_mount_path

# Set up the jobcreator environment
DB_IP = os.environ.get("DB_IP", "")
DB_USERNAME = os.environ.get("DB_USERNAME", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_UPDATER = DBUpdater(ip=DB_IP, username=DB_USERNAME, password=DB_PASSWORD)

# This is used for ensuring that when on staging we will use an empty dir instead of the ceph production mount
DEV_MODE = os.environ.get("DEV_MODE", "False")
# pylint disable=simplifiable-if-statement
if DEV_MODE != "False":
    DEV_MODE = True
else:
    DEV_MODE = False
# pylint enable=simplifiable-if-statement
if DEV_MODE:
    logger.info("Launched in dev mode")
else:
    logger.info("Launched in production mode")

MANTID_SHA = os.environ.get("MANTID_SHA", None)
if MANTID_SHA is None:
    raise OSError("MANTID_SHA not set in the environment, please add it.")
WATCHER_SHA = os.environ.get("WATCHER_SHA", None)
if WATCHER_SHA is None:
    raise OSError("WATCHER_SHA not set in the environment, please add it.")
IR_API_HOST = os.environ.get("IR_API", "ir-api-service.ir.svc.cluster.local:80")
QUEUE_HOST = os.environ.get("QUEUE_HOST", "")
QUEUE_NAME = os.environ.get("INGRESS_QUEUE_NAME", "")
CONSUMER_USERNAME = os.environ.get("QUEUE_USER", "")
CONSUMER_PASSWORD = os.environ.get("QUEUE_PASSWORD", "")
REDUCE_USER_ID = os.environ.get("REDUCE_USER_ID", "")
JOB_NAMESPACE = os.environ.get("JOB_NAMESPACE", "ir")
JOB_CREATOR = JobCreator(dev_mode=DEV_MODE, watcher_sha=WATCHER_SHA)

CEPH_CREDS_SECRET_NAME = os.environ.get("CEPH_CREDS_SECRET_NAME", "ceph-creds")
CEPH_CREDS_SECRET_NAMESPACE = os.environ.get("CEPH_CREDS_SECRET_NAMESPACE", "ir")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "ba68226a-672f-4ba5-97bc-22840318b2ec")
FS_NAME = os.environ.get("FS_NAME", "deneb")

MAX_TIME_TO_COMPLETE = int(os.environ.get("MAX_TIME_TO_COMPLETE", 60 * 60 * 6))


def on_message(message: Dict[str, Any]) -> None:  # pylint: disable=too-many-locals
    """
    Request that the k8s api spawns a job
    :param message: dict, the message is a dictionary containing the needed information for spawning a pod
    :return: None
    """
    asyncio.run(process_message(message))

# pylint disable=too-many-locals
async def process_message(message: Dict[str, Any]) -> None:
    """
    Request that the k8s api spawns a job
    :param message: dict, the message is a dictionary containing the needed information for spawning a pod
    :return: None
    """
    try:
        filename = Path(message["filepath"]).stem
        rb_number = message["experiment_number"]
        instrument_name = message["instrument"]
        experiment_number = message["experiment_number"]
        title = message["experiment_title"]
        users = message["users"]
        run_start = message["run_start"]
        run_end = message["run_end"]
        good_frames = message["good_frames"]
        raw_frames = message["raw_frames"]
        additional_values = message["additional_values"]
        # Add UUID which will avoid collisions for reruns
        job_name = f"run-{filename.lower()}-{str(uuid.uuid4().hex)}"
        db_reduction_id = DB_UPDATER.add_detected_run(
            filename=filename,
            title=title,
            instrument_name=instrument_name,
            users=users,
            experiment_number=experiment_number,
            run_start=run_start,
            run_end=run_end,
            good_frames=good_frames,
            raw_frames=raw_frames,
            reduction_inputs=additional_values,
        )
        script, script_sha = acquire_script(
            ir_api_host=IR_API_HOST,
            reduction_id=db_reduction_id,
            instrument=instrument_name,
        )
        DB_UPDATER.update_script(db_reduction_id, script, script_sha)
        ceph_mount_path = create_ceph_mount_path(instrument_name, rb_number)
        JOB_CREATOR.spawn_job(
            job_name=job_name,
            script=script,
            job_namespace=JOB_NAMESPACE,
            ceph_creds_k8s_secret_name=CEPH_CREDS_SECRET_NAME,
            ceph_creds_k8s_namespace=CEPH_CREDS_SECRET_NAMESPACE,
            cluster_id=CLUSTER_ID,
            fs_name=FS_NAME,
            ceph_mount_path=ceph_mount_path,
            reduction_id=db_reduction_id,
            db_ip=DB_IP,
            db_username=DB_USERNAME,
            db_password=DB_PASSWORD,
            max_time_to_complete_job=MAX_TIME_TO_COMPLETE,
            runner_sha=MANTID_SHA,
        )
    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.exception(exception)
# pylint enable=too-many-locals

def main() -> None:
    """
    This is the function that runs the JobController software suite
    """
    consumer = QueueConsumer(  # pylint: disable=attribute-defined-outside-init
        on_message,
        queue_host=QUEUE_HOST,
        username=CONSUMER_USERNAME,
        password=CONSUMER_PASSWORD,
        queue_name=QUEUE_NAME,
    )
    consumer.start_consuming()


if __name__ == "__main__":
    main()
