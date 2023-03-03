# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import os
import unittest
from unittest import mock

from kubernetes import client, config  # type: ignore
from kubernetes.config import ConfigException

from utils import load_kubernetes_config


class UtilTests(unittest.TestCase):
    @mock.patch("jobcontroller.utils.config")
    def test_config_grabbed_from_incluster(self, kubernetes_config):
        load_kubernetes_config()

        kubernetes_config.load_incluster_config.assert_called_once_with()

    @mock.patch("jobcontroller.utils.config")
    def test_not_in_cluster_grab_kubeconfig_from_env_var(self, kubernetes_config):
        def raise_config_exception():
            raise ConfigException()

        kubeconfig_path = mock.MagicMock()
        kubernetes_config.load_incluster_config = mock.MagicMock(side_effect=raise_config_exception)
        os.environ["KUBECONFIG"] = str(kubeconfig_path)

        load_kubernetes_config()

        kubernetes_config.load_incluster_config.assert_called_once_with()
        kubernetes_config.load_kube_config.assert_called_once_with(config_file=str(kubeconfig_path))
        os.environ.pop("KUBECONFIG", None)

    @mock.patch("jobcontroller.utils.config")
    def test_not_in_cluster_and_not_in_env_grab_kubeconfig_from_default_location(self, kubernetes_config):
        os.environ.pop("KUBECONFIG", None)

        def raise_config_exception():
            raise ConfigException()

        kubernetes_config.load_incluster_config = mock.MagicMock(side_effect=raise_config_exception)

        load_kubernetes_config()

        kubernetes_config.load_incluster_config.assert_called_once_with()
        kubernetes_config.load_kube_config.assert_called_once_with()