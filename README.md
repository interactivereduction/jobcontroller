# The software responsible for controlling the creation of Jobs

Expects the following environment variables to be set:

- "KAFKA_IP": ip to kafka broker

When a job is created it will have a volume mounted to `/output` that will be the correct folder for ceph to output to.

Docker:

Build container:
```bash
docker build . -f ./container/jobcontroller.D -t ghcr.io/interactivereduction/jobcontroller
```

Publish container:
```bash
docker push ghcr.io/interactivereduction/jobcontroller -a
```

Example kafka messages:
```
{"filepath": "/test/path/to/file.txt", "experiment_number": "RB000001", "instrument": "INTER"}
{"filepath": "/test/path/to/anotherone.txt", "experiment_number": "RB000001", "instrument": "INTER"}
{"filepath": "/test/path/to/MARI0.nxs", "experiment_number": "0", "instrument": "MARI"}
```