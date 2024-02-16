import argparse
import glob
import logging
import os
import shutil
import sys

import dask
import numpy as np
import xarray as xr
from numcodecs import Blosc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    stream=sys.stdout,
)


def netcdf_forecasts_to_zarr(ymd: str) -> None:
    """
    Converts the netcdf forecast files to zarr.

    Args:
        ymd (str): Year, month, and day in YYYYMMDD format.
    """
    FORECASTS_DIR = os.environ['FORECASTS_DIR']
    outputs_directory = os.path.join(FORECASTS_DIR, ymd, "outputs")

    vpu_nums = sorted(
        set([os.path.basename(x).split("_")[1] for x in glob.glob(os.path.join(outputs_directory, f"Qout_*_52.nc"))])
    )

    qout_1_51_files = sorted([os.path.join(outputs_directory, f"Qout_{vpu}.nc") for vpu in vpu_nums])
    qout_52_files = sorted(glob.glob(os.path.join(outputs_directory, f"Qout_*_52.nc")))
    zarr_file_path = os.path.join(outputs_directory, f"Qout_{ymd}.zarr")
    print(qout_1_51_files)

    if os.path.exists(zarr_file_path):
        logging.info(f"Removing existing zarr file: {zarr_file_path}")
        shutil.rmtree(zarr_file_path)

    with dask.config.set(**{
        'array.slicing.split_large_chunks': False,
        # set the max chunk size to 5MB
        'array.chunk-size': '5MB',
        # use the threads scheduler
        'scheduler': 'threads',
        # set the maximum memory target usage to 90% of total memory
        'distributed.worker.memory.target': 0.80,
        # do not allow spilling to disk
        'distributed.worker.memory.spill': False,
        # specify the amount of resources to allocate to dask workers
        'distributed.worker.resources': {
            'memory': 3e9,  # 1e9=1GB, this is the amount per worker
            'cpu': os.cpu_count(),  # num CPU per worker
        }
    }):
        logging.info("Opening ensembles 1-51 datasets")
        with xr.open_mfdataset(qout_1_51_files, combine="nested", concat_dim="rivid") as ds151:
            logging.info("Assigning the ensemble coordinate variable")
            ds151 = ds151.assign_coords(ensemble=np.arange(1, 52))
            logging.info("Opening ensemble 52 dataset")
            with xr.open_mfdataset(qout_52_files, combine="nested", concat_dim="rivid") as ds52:
                logging.info("Assigning the ensemble coordinate variable")
                ds52 = ds52.assign_coords(ensemble=52)

                logging.info("Concatenating 1-51 and 52 datasets")
                ds = xr.concat([ds151, ds52], dim="ensemble")

                logging.info("Configuring compression")
                compressor = Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)
                encoding = {'Qout': {"compressor": compressor}}
                logging.info("Writing to zarr")
                (
                    ds
                    .drop_vars(["crs", "lat", "lon", "time_bnds", "Qout_err"])
                    .chunk({
                        "time": -1,
                        "rivid": "auto",
                        "ensemble": -1
                    })
                    .to_zarr(
                        zarr_file_path,
                        consolidated=True,
                        encoding=encoding,
                    )
                )
                logging.info("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ymd",
        help="Year, month, and day in YYYYMMDD format",
        required=True,
    )
    args = parser.parse_args()
    netcdf_forecasts_to_zarr(ymd=args.ymd)
