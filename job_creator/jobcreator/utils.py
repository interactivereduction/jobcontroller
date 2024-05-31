"""
A general utilities module for code that may or may not be reused throughout this repository
"""
import hashlib
import logging
import os
import sys
from pathlib import Path
from typing import List, Tuple

import requests
from kubernetes import config  # type: ignore[import-untyped]
from kubernetes.config import ConfigException  # type: ignore[import-untyped]

stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("jobcreator")


def create_ceph_path(instrument_name: str, rb_number: str) -> str:
    """
    Create the path that the files should store outputs in on CEPH
    :param instrument_name: The name of the instrument that the file is from
    :param rb_number: The experiment number that the file was generated as part of
    :return: The path that the output should be in
    """
    return os.path.join("/ceph", instrument_name, "RBNumber", f"RB{rb_number}", "autoreduced")


def add_ceph_path_to_output_files(ceph_path: str, output_files: List[str]) -> List[str]:
    """
    Add the ceph path to the beginning of output files
    :param ceph_path: The ceph path to be appended to the front of the output files in the list
    :param output_files: The list of files output from the reduction script, that should be appended to the end of
    the ceph_path
    :return: A list with the new paths
    """
    return [os.path.join(ceph_path, output) for output in output_files]


def load_kubernetes_config() -> None:
    """
    Load the kubernetes config for the kubernetes library, attempt incluster first, then try the KUBECONFIG variable,
    then finally try the default kube config locations
    :return:
    """
    try:
        config.load_incluster_config()
    except ConfigException:
        # Load config that is set as KUBECONFIG in the OS or in the default location
        kubeconfig_path = os.getenv("KUBECONFIG", None)
        if kubeconfig_path:
            config.load_kube_config(config_file=kubeconfig_path)
        else:
            config.load_kube_config()


def ensure_ceph_path_exists(ceph_path_str: str) -> str:
    """
    Takes a path that is intended to be on ceph and ensures that it will be correct for what we should mount and
    apply output to.
    :param ceph_path_str: Is the string path to where we should output to ceph
    :return: The corrected path for output to ceph path
    """
    ceph_path = Path(ceph_path_str)
    if not ceph_path.exists():
        logger.info("Ceph path does not exist: %s", ceph_path_str)
        rb_folder = ceph_path.parent
        if not rb_folder.exists():
            logger.info("RBFolder (%s) does not exist, setting RBNumber folder to unknown", str(rb_folder))
            # Set parent to unknown
            rb_folder = rb_folder.with_name("unknown")
            ceph_path = rb_folder.joinpath(ceph_path.name)
        if not ceph_path.exists():
            logger.info("Attempting to create ceph path: %s", str(ceph_path))
            ceph_path.mkdir(parents=True, exist_ok=True)

    return str(ceph_path)


def create_ceph_mount_path(instrument_name: str, rb_number: str, mount_path: str = "/isis/instrument") -> str:
    """
    Creates the ceph mount for the job to output to
    :param instrument_name: str, name of the instrument
    :param rb_number: str, the rb number of the run
    :param mount_path: str, the path that should be pointed to by default, before RBNumber, and Instrument specific
    directories.
    :return: str, the path that was created for the mount
    """
    ceph_path = create_ceph_path(instrument_name, rb_number)
    ceph_path = ensure_ceph_path_exists(ceph_path)
    # There is an assumption that the ceph_path will have /ceph at the start that needs to be removed
    ceph_path = ceph_path.replace("/ceph", "")
    return os.path.join(mount_path, ceph_path)


def extract_useful_parts_from_image(image_path: str) -> Tuple[str, str, str]:
    """
    Takes the image path and extracts just the user image parts.
    :param image_path: str, the image path to process either ghcr.io/fiaisis/mantid:6.9.1 or
    https://ghcr.io/fiaisis/mantid:6.9.1
    :return: Tuple(str, str, str), organisation name, image name, version tag in that order
    """
    image_path_without_https = image_path.split('://')[-1]
    split_image_path = image_path_without_https.split('/')
    org_name = split_image_path[1]
    image_name, version = split_image_path[2].split(":")
    return org_name, image_name, version  # Use organisation and image name without ghcr.io


def get_sha256_using_image_from_ghcr(user_image: str, version: str = "") -> str:
    """
    Take the user image and request from the github api the sha256 of the image tag
    :param user_image: str, in the format "organisation/image_name" e.g. fiaisis/mantid
    :param version: str, the tag used to refer to a specific image
    :return: str, sha256 of the image e.g. "6e5f2d070bb67742f354948d68f837a740874d230714eaa476d35ab6ad56caec"
    """
    if ":" in version:
        version = version.split(":")[-1]

    # Get token
    token_response = requests.get(f"https://ghcr.io/token?scope=repository:{user_image}:pull")
    token = token_response.json().get("token")

    # Create header
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.docker.distribution.manifest.v2+json"
    }

    # Get response from ghcr for digest
    manifest_response = requests.get(f"https://ghcr.io/v2/{user_image}/manifests/{version}", headers=headers)
    manifest = manifest_response.text
    sha256 = hashlib.sha256(manifest.encode('utf-8')).hexdigest()

    return sha256


def find_sha256_of_image(image: str) -> str:
    """
    Return the sha256 version of the image and return the full image path.
    There is an assumption in this that the image is present on ghcr.io, if not this will fail.
    :param image: str, the image to process e.g. ghcr.io/fiaisis/mantid:6.9.1
    :return: str, Return the exact image sha256 if possible based on the image that was passed, if not possible just return
    the input. e.g. ghcr.io/fiaisis/mantid@sha256:6e5f2d070bb67742f354948d68f837a740874d230714eaa476d35ab6ad56caec
    """
    try:
        # If sha256 is present in image assume it is already correct.
        if "sha256:" in image:
            return image
        org_name, image_name, version = extract_useful_parts_from_image(image)
        user_image = org_name + "/" + image_name
        logger.info("Found user image to use: %s", user_image)
        version_to_use = get_sha256_using_image_from_ghcr(user_image, version)
        logger.info("Found sha256 tag for %s: %s", user_image, version_to_use)
        full_image_name = f"ghcr.io/{org_name}/{image_name}@sha256:{version_to_use}"
        return full_image_name
    except Exception as e:
        logger.warning(str(e))
        return image
