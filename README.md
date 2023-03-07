# Job Controller

![License: GPL-3.0](https://img.shields.io/github/license/InteractiveReduction/jobcontroller)
![Build: passing](https://img.shields.io/github/actions/workflow/status/interactivereduction/jobcontroller/tests.yml?branch=main)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/PyCQA/pylint)

The software responsible for controlling the creation of Jobs, and notifying the rest of the software about job completion.

# Running and Testing

Expects the following environment variables to be set when running:

- "KAFKA_IP": ip to kafka broker

When a job is created it will have a volume mounted to `/output` that will be the correct folder for ceph to output to.

To run:

- `pip install .`
- `jobcontroller`

To install when developing:

- `pip install .[dev]`

To demo and test. The easiest way to test JobController is running, requires a kubernetes cluster to interact with, and a kafka instance with a topic to listen to:

#TODO

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

#TODO

Example kafka messages:
```
{"filepath": "/test/path/to/file.txt", "experiment_number": "RB000001", "instrument": "INTER"}
{"filepath": "/test/path/to/anotherone.txt", "experiment_number": "RB000001", "instrument": "INTER"}
{"filepath": "/test/path/to/MARI0.nxs", "experiment_number": "0", "instrument": "MARI"}
```