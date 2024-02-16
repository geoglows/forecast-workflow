#!/usr/bin/env bash

# Install the conda environment if it doesn't exist, otherwise update it to match the file
mamba env create -f ../environment.yaml --name $CONDA_ENV || mamba env update -f ../environment.yaml --name $CONDA_ENV

# set the default environment to the one we just created
# if conda activate $CONDA_ENV is not already in .bashrc, add it
grep -q "conda activate $CONDA_ENV" ~/.bashrc || echo "conda activate $CONDA_ENV" >> ~/.bashrc

# make all the directories
mkdir -p $CONFIGS_DIR
mkdir -p $RUNOFFS_DIR
mkdir -p $SCRIPTS_DIR
mkdir -p $FORECASTS_DIR
mkdir -p $INITS_DIR
mkdir -p $RETURN_PERIODS_DIR

# download the model configs
aws s3 cp s3://geoglows-v2/configs/ $CONFIGS_DIR --recursive --no-sign-request

# download the return periods
aws s3 cp s3://geoglows-v2/returnperiods/ $RETURN_PERIODS_DIR --recursive --no-sign-request
