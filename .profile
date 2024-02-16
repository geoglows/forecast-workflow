# File Paths
CONFIGS_DIR="/mnt/configs"
RUNOFFS_DIR="/mnt/runoffs"
SCRIPTS_DIR="/mnt/scripts"
FORECASTS_DIR="/mnt/fc"
INITS_DIR="/mnt/inits"

export CONFIGS_DIR
export RUNOFFS_DIR
export SCRIPTS_DIR
export FORECASTS_DIR
export INITS_DIR

# S3 Buckets
S3_BUCKET_FORECAST_ARCHIVE="geoglows-forecast-archive"
S3_BUCKET_ESRI_MAP_TABLES="geoglows-esri-map-tables"
S3_BUCKET_INIT_ARCHIVE="geoglows-init-archive"

export S3_BUCKET_FORECAST_ARCHIVE
export S3_BUCKET_ESRI_MAP_TABLES
export S3_BUCKET_INIT_ARCHIVE

# CloudWatch
CLOUDWATCH_LOG_GROUP="geoglows-forecast-compute"

export CLOUDWATCH_LOG_GROUP

# Conda Environment
CONDA_ENV="forecasts"

export CONDA_ENV
conda activate $CONDA_ENV