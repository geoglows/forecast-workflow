#!/usr/bin/env bash

# Install the conda environment if it doesn't exist, otherwise update it to match the file
conda env create -f environment.yml

# Install Docker
./scripts/install_docker.sh

# Pull and create the