#!/usr/bin/env bash

# create the YMD environment variable and export so the subsequent scripts can also access it
YMD=$(date +%Y%m%d)
export YMD

# Activate the environment
conda activate $CONDA_ENV_NAME

# Download the latest IFS grids from s3 bucket
  # compare the last downloaded date, and the date available on the s3 bucket
  # if the date available on the s3 bucket is newer than the last downloaded date, download the new data
  # if the date available on the s3 bucket is older or the same as the last downloaded date

mkdirs -p $FC_DIR/$YMD/inflows
mkdirs -p $FC_DIR/$YMD/namelists
mkdirs -p $FC_DIR/$YMD/outputs
mkdirs -p $FC_DIR/$YMD/logs

# Calculate inflows
python ../geoglows/prepare_inflows.py

# Prepare namelists
python ../geoglows/prepare_namelists.py

# RAPID routing
docker exec rapid-docker-rapid python3 /mnt/scripts/runrapid.py --fcdir $FC_DIR/$YMD

# Concatenate and summarize the ensemble outputs
./postprocess_rapid_outputs.sh -d $OUTPUT_DIR/$YMD

# Calculate the init files
python ../geoglows/calculate_inits.py

# NetCDF to Zarr (and delete netCDFs)
python ../geoglows/netcdf_ensembles_to_zarr.py

# Generate Esri map style tables
python ../geoglows/generate_map_style_tables.py

# Archive inits, outputs, map tables
./archive_to_aws.sh

# Deactivate the environment
conda deactivate
