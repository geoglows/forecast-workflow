#!/usr/bin/env bash

# forecast zarr archival
aws s3 cp "$FORECASTS_DIR/$YMD/outputs/$YMD.zarr" "s3://S3_BUCKET_FORECAST_ARCHIVE/$YMD.zarr" --recursive

# map style table archival
aws s3 cp "$FORECASTS_DIR/$YMD/maptables" s3://S3_BUCKET_ESRI_MAP_TABLES --recursive --exclude "*" --include "mapstyletable*.csv"

# forecast inits archival
aws s3 sync "$INITS_DIR" s3://S3_BUCKET_INIT_ARCHIVE