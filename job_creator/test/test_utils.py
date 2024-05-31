# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, protected-access,
# pylint: disable=too-many-instance-attributes
import os
import unittest
from unittest import mock

from kubernetes.config import ConfigException

from jobcreator.utils import (
    load_kubernetes_config,
    ensure_ceph_path_exists,
    find_sha256_of_image,
    extract_useful_parts_from_image,
    get_sha256_using_image_from_ghcr,
)


class UtilTests(unittest.TestCase):
    @mock.patch("jobcreator.utils.config")
    def test_config_grabbed_from_incluster(self, kubernetes_config):
        load_kubernetes_config()

        kubernetes_config.load_incluster_config.assert_called_once_with()

    @mock.patch("jobcreator.utils.config")
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

    @mock.patch("jobcreator.utils.config")
    def test_not_in_cluster_and_not_in_env_grab_kubeconfig_from_default_location(self, kubernetes_config):
        os.environ.pop("KUBECONFIG", None)

        def raise_config_exception():
            raise ConfigException()

        kubernetes_config.load_incluster_config = mock.MagicMock(side_effect=raise_config_exception)

        load_kubernetes_config()

        kubernetes_config.load_incluster_config.assert_called_once_with()
        kubernetes_config.load_kube_config.assert_called_once_with()

    def test_ensure_ceph_path_exists(self):
        initial_path = "/tmp/ceph/mari/RBNumber/RB99999999/autoreduced/"

        end_path = ensure_ceph_path_exists(initial_path)

        self.assertEqual(end_path, "/tmp/ceph/mari/RBNumber/unknown/autoreduced")
        os.removedirs("/tmp/ceph/mari/RBNumber/unknown/autoreduced")

    def test_extract_useful_parts_from_image_with_https(self):
        image_path = "https://ghcr.io/fiaisis/mantid:6.9.1"

        org_name, image_name, version = extract_useful_parts_from_image(image_path)

        self.assertEqual(org_name, "fiaisis")
        self.assertEqual(image_name, "mantid")
        self.assertEqual(version, "6.9.1")

    def test_extract_useful_parts_from_image_without_https(self):
        image_path = "ghcr.io/fiaisis/mantid:6.9.1"

        org_name, image_name, version = extract_useful_parts_from_image(image_path)

        self.assertEqual(org_name, "fiaisis")
        self.assertEqual(image_name, "mantid")
        self.assertEqual(version, "6.9.1")

    @mock.patch("jobcreator.utils.requests")
    def test_get_sha256_using_image_from_ghcr_with_version_colon(self, requests):
        user_image = "fiaisis/mantid"
        version = ":6.9.1"
        expected_version = "6.9.1"
        response = mock.MagicMock()
        response.text = "requests_response"
        requests.get.return_value = response
        expected_headers = {
            "Authorization": f"Bearer {response.json.return_value.get.return_value}",
            "Accept": "application/vnd.docker.distribution.manifest.v2+json",
        }

        get_sha256_using_image_from_ghcr(user_image, version)

        self.assertEqual(requests.get.call_count, 2)
        self.assertEqual(
            requests.get.call_args_list[0],
            mock.call(f"https://ghcr.io/token?scope=repository:{user_image}:pull", timeout=5),
        )
        self.assertEqual(
            requests.get.call_args_list[1],
            mock.call(
                f"https://ghcr.io/v2/{user_image}/manifests/{expected_version}", timeout=5, headers=expected_headers
            ),
        )

    @mock.patch("jobcreator.utils.requests")
    def test_get_sha256_using_image_from_ghcr_without_version_colon(self, requests):
        user_image = "fiaisis/mantid"
        version = "6.9.1"
        response = mock.MagicMock()
        response.text = "requests_response"
        requests.get.return_value = response
        expected_headers = {
            "Authorization": f"Bearer {response.json.return_value.get.return_value}",
            "Accept": "application/vnd.docker.distribution.manifest.v2+json",
        }

        get_sha256_using_image_from_ghcr(user_image, version)

        self.assertEqual(requests.get.call_count, 2)
        self.assertEqual(
            requests.get.call_args_list[0],
            mock.call(f"https://ghcr.io/token?scope=repository:{user_image}:pull", timeout=5),
        )
        self.assertEqual(
            requests.get.call_args_list[1],
            mock.call(f"https://ghcr.io/v2/{user_image}/manifests/{version}", timeout=5, headers=expected_headers),
        )

    def raise_exception(self):
        raise Exception("Crazy Exception!")  # pylint: disable=broad-exception-raised

    @mock.patch("jobcreator.utils.logger")
    @mock.patch("jobcreator.utils.get_sha256_using_image_from_ghcr")
    @mock.patch("jobcreator.utils.extract_useful_parts_from_image", side_effect=raise_exception)
    def test_find_sha256_of_image_exception_is_raised(self, _, __, logger):
        image = str(mock.MagicMock())

        return_value = find_sha256_of_image(image)

        logger.warning.assert_called_once_with(str(Exception("Crazy Exception!")))
        self.assertEqual(image, return_value)

    def test_find_sha256_of_image_sha256_in_image(self):
        input_value = "ghcr.io/fiaisis/mantid@sha256:6e5f2d070bb67742f354948d68f837a740874d230714eaa476d35ab6ad56caec"

        return_value = find_sha256_of_image(input_value)

        self.assertEqual(return_value, input_value)

    @mock.patch(
        "jobcreator.utils.get_sha256_using_image_from_ghcr",
        return_value="6e5f2d070bb67742f354948d68f837a740874d230714eaa476d35ab6ad56caec",
    )
    @mock.patch("jobcreator.utils.extract_useful_parts_from_image", return_value=("fiaisis", "mantid", "6.9.1"))
    def test_find_sha256_of_image_just_version(self, _, __):
        image_path = "https://ghcr.io/fiaisis/mantid:6.9.1"

        return_value = find_sha256_of_image(image_path)

        self.assertEqual(
            return_value,
            "ghcr.io/fiaisis/mantid@sha256:6e5f2d070bb67742f354948d68f837a740874d230714eaa476d35ab6ad56caec",
        )
