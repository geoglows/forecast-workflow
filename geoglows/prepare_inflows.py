from basininflow import create_inflow_file
import os
import glob
import argparse
from natsort import natsorted
from multiprocessing import Pool

config_dir = os.getenv('CONFIG_DIR')
inflow_dir = os.getenv('INFLOW_DIR')
runoff_dir = os.getenv('RUNOFF_DIR')


def make_inflow(vpu_config_dir: str, runoff_file: str, ymd: str):
    ensemble_number = os.path.basename(runoff_file).split('.')[0]
    inflows = os.path.join(inflow_dir, ymd)
    create_inflow_file(
        lsm_data=runoff_file,
        input_dir=vpu_config_dir,
        inflow_dir=inflows,
        file_label=ensemble_number,
        force_positive_runoff=True,
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'ymd',
        help='Year, month, and day in YYYYMMDD format',
        required=True,
    )
    args = parser.parse_args()
    ymd = args.ymd

    # list all the VPU directories
    vpu_dirs = natsorted(glob.glob(os.path.join(config_dir, '*')))

    # search for runoff files in runoff/YMD
    runoff_dir = os.path.join(runoff_dir, ymd)
    runoff_files = natsorted(glob.glob(os.path.join(runoff_dir, '*.nc')))
    if not runoff_files:
        print(f'No runoff files found in {runoff_dir}')
        exit(1)

    # make the inflow/YMD directory
    os.makedirs(os.path.join(inflow_dir, ymd), exist_ok=True)

    jobs = [(vpu_dir, runoff_file) for vpu_dir in vpu_dirs for runoff_file in runoff_files]

    with Pool(min(len(jobs), os.cpu_count())) as p:
        p.starmap(make_inflow, jobs)
