# Job Controller

![License: GPL-3.0](https://img.shields.io/github/license/fiaisis/jobcontroller)
![Build: passing](https://img.shields.io/github/actions/workflow/status/fiaisis/jobcontroller/tests.yml?branch=main)
[![codecov](https://codecov.io/github/fiaisis/jobcontroller/branch/main/graph/badge.svg?token=XR6PCJ1VR8)](https://codecov.io/github/fiaisis/jobcontroller)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/PyCQA/pylint)

The software responsible for controlling the creation of Jobs, and notifying the rest of the software about job completion.

# Running and Testing

Expects the following environment variables to be set when running:

- "QUEUE_HOST": ip to the message broker
- "QUEUE_USER": The username for the message broker
- "QUEUE_PASSWORD": The password for the message broker
- "FIA_IP": ip to the FIA-API
- "DB_IP": ip for database
- "DB_USERNAME": Username for database
- "DB_PASSWORD": Password for database
- "REDUCE_USER_ID": The ID for used for when interacting with CEPH
- "RUNNER_SHA": The SHA256 of the runner container on the github container registry, that will be used for completing jobs on the cluster
- "KUBECONFIG": (Optional) Path to the kubeconfig file

When a job is created it will have a volume mounted to `/output` that will be the correct folder for ceph to output to.

To run:

- `pip install .`
- `jobcontroller`

To install when developing:

- `pip install .[dev]`

To demo and test. The easiest way to test JobController is running and functioning correctly, it requires a kubernetes cluster to interact with, and a rabbitmq instance with a queue to listen to:

- Follow these instructions to [create the cluster](https://github.com/fiaisis/k8s#developing-using-a-local-cluster)
- Create the message broker, this is presently [RabbitMQ](https://www.rabbitmq.com/download.html)
- Using the producer send one of the messages in the example messages section below.
- The JobController should make a job and the job will make pods that will perform the work for the run

# How to container

- The containers are stored in
  the [container registry for the organisation on github](https://github.com/orgs/fiaisis/packages).

- Build container:
```bash
docker build . -f ./container/jobcontroller.D -t ghcr.io/fiaisis/jobcontroller
```

- Run container (replace contents of < > with relevant details):
```bash
docker run -it --rm --mount source=/ceph/<instrument>/RBNumbers/RB<experiment number>,target=/output --name jobcontroller ghcr.io/fiaisis/jobcontroller
```

- To push containers you will need to setup the correct access for it, you can follow
  this [guide](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry).
- Publish container:
```bash
docker push ghcr.io/fiaisis/jobcontroller -a
```

# Running Tests:

To run the tests:

```bash
pytest .
```

# Example Message with runner image:
```json
{
   "run_number":30019,
   "instrument":"TOSCA",
   "experiment_title":"A great experiment title",
   "experiment_number":"2310042",
   "runner_image":"ghcr.io/fiaisis/mantid:6.9.0",
   "filepath":"/archive/NDXTOSCA/Instrument/data/cycle_24_1/TSC30018.nxs",
   "run_start":"2024-05-28 13:12:41.000000",
   "run_end":"2024-05-28 13:12:55.000000",
   "raw_frames":27065,
   "good_frames":27043,
   "users":"users names",
   "additional_values":{"input_runs": [30019, 30018], "cycle_string": "cycle_24_1"}
}
```
