#!/usr/bin/env bash

# Install the conda environment if it doesn't exist, otherwise update it to match the file
mamba env create -f ../environment.yaml --name $CONDA_ENV || mamba env update -f ../environment.yaml --name $CONDA_ENV

# set the default environment to the one we just created
# if conda activate $CONDA_ENV is not already in .bashrc, add it
grep -q "conda activate $CONDA_ENV" ~/.bashrc || echo "conda activate $CONDA_ENV" >> ~/.bashrc
