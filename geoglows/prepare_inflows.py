import argparse
import glob
import logging
import os
import sys
from multiprocessing import Pool

from basininflow import create_inflow_file
from natsort import natsorted

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

CONFIGS_DIR = os.environ['CONFIGS_DIR']
RUNOFFS_DIR = os.environ['RUNOFFS_DIR']
FORECASTS_DIR = os.environ['FORECASTS_DIR']


def make_inflow(vpu_config_dir: str, runoff_file: str, ymd: str):
    ensemble_number = os.path.basename(runoff_file).split('.')[0]
    inflow_dir = os.path.join(FORECASTS_DIR, ymd, 'inflows')
    create_inflow_file(
        lsm_data=runoff_file,
        input_dir=vpu_config_dir,
        inflow_dir=inflow_dir,
        file_label=ensemble_number,
        force_positive_runoff=True,
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--ymd',
        help='Year, month, and day in YYYYMMDD format',
        required=True,
    )
    args = parser.parse_args()
    ymd = args.ymd

    # list all the VPU directories
    vpu_dirs = glob.glob(os.path.join(CONFIGS_DIR, '*'))
    vpu_dirs = natsorted([x for x in vpu_dirs if os.path.isdir(x)])

    # search for runoff files in runoff/YMD
    RUNOFFS_DIR = os.path.join(RUNOFFS_DIR, ymd)
    runoff_files = natsorted(glob.glob(os.path.join(RUNOFFS_DIR, '*.nc')))

    print(f'CONFIGS_DIR: {CONFIGS_DIR}')
    print(f'RUNOFFS_DIR: {RUNOFFS_DIR}')
    print(f'FORECASTS_DIR: {FORECASTS_DIR}')
    print(f'ymd: {ymd}')
    print(f'vpu_dirs: {vpu_dirs}')
    print(f'runoff_files: {runoff_files}')

    if not runoff_files:
        print(f'No runoff files found in {RUNOFFS_DIR}')
        exit(1)

    # make the inflow/YMD directory
    os.makedirs(os.path.join(FORECASTS_DIR, ymd, 'inflows'), exist_ok=True)

    jobs = [(vpu_dir, runoff_file, ymd) for vpu_dir in vpu_dirs for runoff_file in runoff_files]

    with Pool(min(len(jobs), os.cpu_count())) as p:
        p.starmap(make_inflow, jobs)
