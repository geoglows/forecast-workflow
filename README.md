# GEOGLOWS Forecast Computations

## AWS Infrastructure
S3 Buckets:
- geoglows-forecast-archive
- geoglows-esri-map-tables
- geoglows-init-archive

CloudWatch Logs:
- geoglows-forecast-compute

## Docker Containers
chdavid/rapid:latest

## Required Environment Variables

You should put the following variables in a `.profile` file in the root of the project.
Adjust file paths and names as necessary.

```bash
CONFIGS_DIR=/mnt/configs
INITS_DIR=/mnt/inits
INFLOWS_DIR=/mnt/inflows
NAMELISTS_DIR=/mnt/namelists
OUTPUTS_DIR=/mnt/outputs
SCRIPTS_DIR=/mnt/scripts

S3_BUCKET_FORECAST_ARCHIVE=geoglows-forecast-archive
S3_BUCKET_ESRI_MAP_TABLES=geoglows-esri-map-tables
S3_BUCKET_INIT_ARCHIVE=geoglows-init-archive

CLOUDWATCH_LOG_GROUP=geoglows-forecast-compute
```

## Required Directory Structure

You should designate a working directory, such as /mnt on a mounted volume.
It should contain exactly the following directory structure.

```
/mnt
    /configs
        101/
            k.csv
            x.csv
            comid_lat_lon_z.csv
            rapid_connect.csv
            riv_bas_id.csv
            weight_*.csv
    /inits
        101/
            qfinal_$vpu_$ymd.nc
    /inflows
        $YMD/
            m3_$vpu_$ens.nc
    /namelists
        $YMD/
            namelist_$vpu_$ens
    /outputs
        $YMD/
            Qout_$vpu_$ens.nc
    /scripts
        run_rapid.py