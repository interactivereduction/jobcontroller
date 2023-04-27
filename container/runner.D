FROM condaforge/mambaforge:22.9.0-3

# Install mantid (in-container) dependencies
RUN apt-get update && apt-get install libgl1 -y

# Install mantid
COPY ./container/mantid-environment.yml .
RUN mamba env create -f mantid-environment.yml

# Make sure the environment is activated:
RUN conda init bash
RUN echo "conda activate mantid" > ~/.bashrc
ENV PATH /opt/conda/envs/mantid/bin:$PATH

# Create a shell script to run the python command:
RUN echo '#!/bin/bash\npython -c "$@"' > /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the entrypoint:
ENTRYPOINT ["/entrypoint.sh"]