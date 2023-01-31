FROM python:3.10

# Install jobcontroller to the container
ADD . .
RUN python -m pip install --no-cache-dir .

CMD jobcontroller