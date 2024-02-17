import argparse
import glob
import logging
import os
import sys

from basininflow import create_inflow_file
from natsort import natsorted

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

CONFIGS_DIR = os.environ['CONFIGS_DIR']
RUNOFFS_DIR = os.environ['RUNOFFS_DIR']
FORECASTS_DIR = os.environ['FORECASTS_DIR']

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--ymd',
        help='Year, month, and day in YYYYMMDD format',
        required=True,
    )
    parser.add_argument(
        '--vpu',
        help='VPU number',
        required=True,
    )
    args = parser.parse_args()
    ymd = args.ymd
    vpu = args.vpu

    # search for runoff files in runoff/YMD
    RUNOFFS_DIR = os.path.join(RUNOFFS_DIR, ymd)
    runoff_files = natsorted(glob.glob(os.path.join(RUNOFFS_DIR, '*.nc')))

    inflow_dir = os.path.join(FORECASTS_DIR, ymd, 'inflows')
    vpu_config_dir = os.path.join(CONFIGS_DIR, vpu)

    # make the inflow/YMD directory
    os.makedirs(os.path.join(FORECASTS_DIR, ymd, 'inflows'), exist_ok=True)

    for runoff_file in runoff_files:
        ensemble_number = os.path.basename(runoff_file).split('.')[0]
        if len(glob.glob(os.path.join(inflow_dir, f'm3_{vpu}*{ensemble_number}.nc'))):
            continue
        create_inflow_file(
            lsm_data=runoff_file,
            input_dir=vpu_config_dir,
            inflow_dir=inflow_dir,
            y_var='lat',
            x_var='lon',
            time_var='time',
            runoff_var='RO',
            file_label=ensemble_number,
            force_positive_runoff=True,
        )
