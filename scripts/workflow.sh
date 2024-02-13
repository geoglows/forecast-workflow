#!/usr/bin/env bash

# Initialize YMD variable with current date
YMD=$(date +%Y%m%d)

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ymd)
            shift
            if [[ ! "$1" =~ ^[0-9]{8}$ ]]; then
                echo "Error: Invalid date format. Please use YYYYMMDD."
                exit 1
            fi
            YMD=$1
            ;;
        *)
            echo "Error: Unknown option $1"
            exit 1
            ;;
    esac
    shift
done

if [[ -z $YMD ]]; then
    echo "Error: Date not specified. Please use the --ymd flag to specify a date in the format YYYYMMDD."
    exit 1
fi

echo "YMD is set to: $YMD"
export YMD

# Download the latest IFS grids from s3 bucket
  # compare the last downloaded date, and the date available on the s3 bucket
  # if the date available on the s3 bucket is newer than the last downloaded date, download the new data
  # if the date available on the s3 bucket is older or the same as the last downloaded date

mkdir -p $FORECASTS_DIR/$YMD/inflows
mkdir -p $FORECASTS_DIR/$YMD/namelists
mkdir -p $FORECASTS_DIR/$YMD/outputs
mkdir -p $FORECASTS_DIR/$YMD/logs
mkdir -p $FORECASTS_DIR/$YMD/maptables

# Calculate inflows
python ../geoglows/prepare_inflows.py --ymd $YMD

# Prepare namelists
python ../geoglows/prepare_namelists.py --ymd $YMD

# RAPID routing
docker exec rapid python3 /mnt/scripts/runrapid.py --fcdir $FORECASTS_DIR/$YMD

# Concatenate and summarize the ensemble outputs
./postprocess_rapid_outputs.sh -d $FORECASTS_DIR/$YMD/outputs

# Calculate the init files
python ../geoglows/calculate_inits.py

# NetCDF to Zarr (and delete netCDFs)
python ../geoglows/netcdf_ensembles_to_zarr.py

# Generate Esri map style tables
python ../geoglows/generate_map_style_tables.py

# Archive inits, outputs, map tables
./archive_to_aws.sh
