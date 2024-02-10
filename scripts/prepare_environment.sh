#!/usr/bin/env bash

# Install the conda environment if it doesn't exist, otherwise update it to match the file
mamba env create -f environment.yml
