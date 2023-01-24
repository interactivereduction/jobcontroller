"""
Communicate to a kubernetes API to spawn a pod with the metadata passed by kafka message to the RunMaker
"""
from kubernetes import client, config


class K8sAPI:
    def __init__(self):
        config.load_kube_config()

    def spawn_pod(self, meta_data: dict):
        job_name = f'run-{meta_data[""]}'
        pod = client.V1Pod(metadata={'name': job_name},
                           spec={
                               'containers': [
                                   {
                                       'name': job_name,
                                       'image': 'ir-mantid-runner',  # TODO update this to include a sha256
                                       'env': [
                                           {
                                               'name': 'KAFKA_IP',
                                               'value': 'kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local'
                                           }
                                       ],
                                       'volumeMounts': [
                                           {
                                               'name': 'archive-mount',
                                               'mountPath': '/archive'
                                           },
                                           {
                                               'name': 'ceph-mount',
                                               'mountPath': '/ceph'
                                           }
                                       ],
                                   }
                               ],
                               'volumes': [
                                   {
                                       'name': 'archive-mount',
                                       'hostPath': {
                                           'type': 'Directory',
                                           'path': '/archive'
                                       }
                                   },
                                   {
                                       'name': 'ceph-mount',
                                       'hostPath': {
                                           'type': 'Directory',
                                           'path': '/ceph'
                                       }
                                   }
                               ]
                           })
        client.CoreV1Api().create_namespaced_pod(namespace='ir-runs', body=pod)
