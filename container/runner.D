FROM condaforge/mambaforge:22.9.0-3

# Install mantid
COPY ./container/mantid-environment.yml .
RUN mamba env create -f mantid-environment.yml

# Ensure conda environment is activated on restart
RUN echo "conda activate mantid" >> ~/.bashrc