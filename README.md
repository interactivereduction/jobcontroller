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