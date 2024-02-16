#!/usr/bin/env bash

DAYS_TO_KEEP=3

# delete forecast directories older than a certain number of days
find $FORECAST_DIR -type d -ctime +$DAYS_TO_KEEP -exec rm -rf {} \
  \; -exec echo "Deleted forecast directory: {}" \;