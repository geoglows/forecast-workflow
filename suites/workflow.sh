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
echo "YMD is set to: $YMD"
export YMD

VPUS=$(ls -1 $CONFIGS_DIR | awk -F/ '{print $NF}' | sort -V)

# Download the latest IFS grids from s3 bucket
  # compare the last downloaded date, and the date available on the s3 bucket
  # if the date available on the s3 bucket is newer than the last downloaded date, download the new data
  # if the date available on the s3 bucket is older or the same as the last downloaded date

echo "Creating directory structure"
mkdir -p $FORECASTS_DIR/$YMD/inflows
mkdir -p $FORECASTS_DIR/$YMD/namelists
mkdir -p $FORECASTS_DIR/$YMD/outputs
mkdir -p $FORECASTS_DIR/$YMD/logs
mkdir -p $FORECASTS_DIR/$YMD/maptables

# Calculate inflows
echo "Calculating inflows"
xargs -I {} -P "$(nproc)" sh -c "python $HOME/forecast-workflow/python/prepare_inflows.py --ymd $YMD --vpu {} >> $FORECASTS_DIR/$YMD/logs/{}" <<< $VPUS || exit 1

# Prepare namelists
echo "Preparing namelists"
xargs -I {} -P "$(nproc)" sh -c "python $HOME/forecast-workflow/python/prepare_namelists.py --ymd $YMD --vpu {} >> $FORECASTS_DIR/$YMD/logs/{}" <<< $VPUS || exit 1

# RAPID routing
echo "Running RAPID routing"
NAMELISTS=$(ls -1 $FORECASTS_DIR/$YMD/namelists/* | sort -V)
xargs -I {} -P "$(nproc)" sh -c 'docker exec rapid python3 /mnt/scripts/runrapid.py --namelist "{}" >> "$FORECASTS_DIR/$YMD/logs/$(basename "{}").log"' <<< "$NAMELISTS" || exit 1

# Concatenate and summarize the ensemble outputs
echo "Concatenating and summarizing the ensemble outputs"
xargs -I {} -P "$(nproc)" sh -c "../bash/postprocess_rapid_outputs.sh --outputs $FORECASTS_DIR/$YMD/outputs --vpu {} >> $FORECASTS_DIR/$YMD/logs/{}" <<< $VPUS || exit 1

# Calculate the init files
echo "Calculating the init files"
xargs -I {} -P "$(nproc)" sh -c "python $HOME/forecast-workflow/python/calculate_inits.py --ymd $YMD --vpu {} >> $FORECASTS_DIR/$YMD/logs/{}" <<< $VPUS || exit 1

# Generate Esri map style tables
echo "Generating Esri map style tables"
xargs -I {} -P "$(nproc)" sh -c "python $HOME/forecast-workflow/python/generate_vpu_map_tables.py --ymd $YMD --vpu {} >> $FORECASTS_DIR/$YMD/logs/{}" <<< $VPUS || exit 1
python $HOME/forecast-workflow/python/generate_global_map_tables.py --ymd $YMD || exit 1

# NetCDF to Zarr (and delete netCDFs)
echo "Converting NetCDF to Zarr"
python $HOME/forecast-workflow/python/vpu_netcdfs_to_zarr.py --ymd $YMD || exit 1

# Archive inits, outputs, map tables
echo "Archiving inits, outputs, map tables"
../bash/archive_to_s3.sh

echo "Clearing forecast directory"
../bash/clean_forecast_directory.sh

echo "Forecast workflow completed successfully"
