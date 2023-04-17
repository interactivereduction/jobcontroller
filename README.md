# Job Controller

![License: GPL-3.0](https://img.shields.io/github/license/InteractiveReduction/jobcontroller)
![Build: passing](https://img.shields.io/github/actions/workflow/status/interactivereduction/jobcontroller/tests.yml?branch=main)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/PyCQA/pylint)

The software responsible for controlling the creation of Jobs, and notifying the rest of the software about job completion.

# Running and Testing

Expects the following environment variables to be set when running:

- "KAFKA_IP": ip to kafka broker
- "IR_IP": ip to the IR-API
- "DB_IP": ip for database
- "DB_USERNAME": Username for database
- "DB_PASSWORD": Password for database
- "REDUCE_USER_ID": The ID for used for when interacting with CEPH
- "KUBECONFIG": (Optional) Path to the kubeconfig file

When a job is created it will have a volume mounted to `/output` that will be the correct folder for ceph to output to.

To run:

- `pip install .`
- `jobcontroller`

To install when developing:

- `pip install .[dev]`

To demo and test. The easiest way to test JobController is running and functioning correctly, it requires a kubernetes cluster to interact with, and a kafka instance with a topic to listen to:

- Follow these instructions to [create the cluster](https://github.com/interactivereduction/k8s#developing-using-a-local-cluster)
- Download [kafka console producer and consumer](https://www.apache.org/dyn/closer.cgi?path=/kafka/3.4.0/kafka_2.13-3.4.0.tgz)
- You need to wait for the kafka cluster to finish being ready, it's recommended to use k9s for this.
- Follow these instructions to [create the local kafka producer](https://github.com/interactivereduction/k8s#creating-a-kafka-producer-for-connecting-to-the-cluster-and-sending-things-to-a-topic)
- Using the producer send one of the messages in the example kafka messages section below.
- The JobController should make a job and the job will make pods that will perform the work for the run

# How to container

- The containers are stored in
  the [container registry for the organisation on github](https://github.com/orgs/interactivereduction/packages).

- Build container:
```bash
docker build . -f ./container/jobcontroller.D -t ghcr.io/interactivereduction/jobcontroller
```

- Run container (replace contents of < > with relevant details):
```bash
docker run -it --rm --mount source=/ceph/<instrument>/RBNumbers/RB<experiment number>,target=/output --name jobcontroller ghcr.io/interactivereduction/jobcontroller
```

- To push containers you will need to setup the correct access for it, you can follow
  this [guide](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry).
- Publish container:
```bash
docker push ghcr.io/interactivereduction/jobcontroller -a
```

# Running Tests:

To run the tests:

```bash
pytest .
```

Example kafka messages:
```
{"filepath": "/test/path/to/MARI0.nxs", "experiment_number": "0", "instrument": "MARI"}
{"filepath": "/test/path/to/MARI123456.nxs", "experiment_number": "1220474", "instrument": "MARI"}
{"run_number": 25581, "instrument": "MARI", "experiment_title": "Whitebeam - vanadium - detector tests - vacuum bad - HT on not on all LAB", "experiment_number": "1820497", "filepath": "/archive/25581/MAR25581.nxs", "run_start": "2019-03-22T10:15:44", "run_end": "2019-03-22T10:18:26", "raw_frames": 8067, "good_frames": 6452, "users": "Wood,Guidi,Benedek,Mansson,Juranyi,Nocerino,Forslund,Matsubara", "additional_values": {"ei": "auto", "sam_mass": 0.0, "sam_rmm": 0.0, "monovan": 0, "remove_bkg": true, "sum_runs": false, "runno": 25581}}
```