FROM python:3.10

WORKDIR /jobcontroller

# Install jobcontroller to the container
COPY . /jobcontroller
RUN python -m pip install --no-cache-dir .

CMD ["jobcontroller"]