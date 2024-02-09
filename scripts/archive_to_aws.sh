#!/usr/bin/env bash

# forecast zarr archival
aws s3 cp $OUTPUT_DIR/$YMD/$YMD.zarr s3://$FORECAST_BUCKET_URI/$YMD.zarr --recursive

# map style table archival
aws s3 cp $OUTPUT_DIR/$YMD s3://$MAP_STYLE_TABLE_BUCKET_URI --recursive --exclude "*" --include "mapstyletable*.csv"

# forecast inits archival
aws s3 cp $INITS_DIR/$YMD s3://$FORECAST_BUCKET_URI --recursive --exclude "*" --include "inits*.nc"