"""
A general utilities module for code that may or may not be reused throughout this repository
"""
import logging
import os
import sys
from typing import List
from kubernetes import client, config  # type: ignore
from kubernetes.config import ConfigException

file_handler = logging.FileHandler(filename="run-detection.log")
stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[file_handler, stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("jobcontroller")


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
    full_paths = []
    for output in output_files:
        full_paths.append(os.path.join(ceph_path, output))
    return full_paths


def load_kubernetes_config():
    try:
        config.load_incluster_config()
    except ConfigException:
        # Load config that is set as KUBECONFIG in the OS or in the default location
        kubeconfig_path = os.getenv("KUBECONFIG", None)
        if kubeconfig_path:
            config.load_kube_config(config_file=kubeconfig_path)
        else:
            config.load_kube_config()
