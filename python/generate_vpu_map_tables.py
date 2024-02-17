import argparse
import os

import netCDF4 as nc
import pandas as pd
import xarray as xr

FORECASTS_DIR = os.environ['FORECASTS_DIR']
CONFIGS_DIR = os.environ['CONFIGS_DIR']
RUNOFFS_DIR = os.environ['RUNOFFS_DIR']
INITS_DIR = os.environ['INITS_DIR']
RETURN_PERIODS_DIR = os.environ['RETURN_PERIODS_DIR']


def postprocess_vpu_forecast_directory(ymd: str, vpu: int or str, ):
    style_table_file_name = f'mapstyletable_{vpu}_{ymd}.parquet'
    if os.path.exists(os.path.join(FORECASTS_DIR, ymd, 'mapstyletables', style_table_file_name)):
        return

    nces_output_filename = os.path.join(FORECASTS_DIR, ymd, 'outputs', f'nces_avg_{vpu}.nc')

    # read the date and COMID lists from one of the netcdfs
    with xr.open_dataset(nces_output_filename) as ds:
        comids = ds["rivid"][:].values
        dates = pd.to_datetime(ds["time"][:].values)
        mean_flows = ds["Qout"][:].values.round(1)

    mean_flow_df = pd.DataFrame(mean_flows, columns=comids, index=dates)

    # limit both dataframes to the first 10 days
    mean_flow_df = mean_flow_df[mean_flow_df.index <= mean_flow_df.index[0] + pd.Timedelta(days=10)]

    # creating pandas dataframe with return periods
    rp_path = os.path.join(RETURN_PERIODS_DIR, f"returnperiods_{vpu}.nc")
    with nc.Dataset(rp_path, "r") as rp_ncfile:
        rp_df = pd.DataFrame(
            {
                "return_2": rp_ncfile.variables["rp2"][:],
                "return_5": rp_ncfile.variables["rp5"][:],
                "return_10": rp_ncfile.variables["rp10"][:],
                "return_25": rp_ncfile.variables["rp25"][:],
                "return_50": rp_ncfile.variables["rp50"][:],
                "return_100": rp_ncfile.variables["rp100"][:],
            },
            index=rp_ncfile.variables["rivid"][:],
        )

    mean_thickness_df = pd.DataFrame(columns=comids, index=dates, dtype=int)
    mean_thickness_df[:] = 1
    mean_thickness_df[mean_flow_df >= 20] = 2
    mean_thickness_df[mean_flow_df >= 250] = 3
    mean_thickness_df[mean_flow_df >= 1500] = 4
    mean_thickness_df[mean_flow_df >= 10000] = 5
    mean_thickness_df[mean_flow_df >= 30000] = 6

    mean_ret_per_df = pd.DataFrame(columns=comids, index=dates, dtype=int)
    mean_ret_per_df[:] = 0
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_2"], axis=1)] = 2
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_5"], axis=1)] = 5
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_10"], axis=1)] = 10
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_25"], axis=1)] = 25
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_50"], axis=1)] = 50
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_100"], axis=1)] = 100

    mean_flow_df = mean_flow_df.stack().to_frame().rename(columns={0: "mean"})
    mean_thickness_df = mean_thickness_df.stack().to_frame().rename(columns={0: "thickness"})
    mean_ret_per_df = mean_ret_per_df.stack().to_frame().rename(columns={0: "ret_per"})

    # merge all dataframes
    for df in [mean_thickness_df, mean_ret_per_df]:
        mean_flow_df = mean_flow_df.merge(df, left_index=True, right_index=True)

    maptable_outdir = os.path.join(FORECASTS_DIR, ymd, "maptables")

    mean_flow_df.index.names = ["timestamp", "comid"]
    mean_flow_df = mean_flow_df.reset_index()
    mean_flow_df["mean"] = mean_flow_df["mean"].round(1)
    mean_flow_df.loc[mean_flow_df["mean"] < 0, "mean"] = 0
    mean_flow_df["thickness"] = mean_flow_df["thickness"].astype(int)
    mean_flow_df["ret_per"] = mean_flow_df["ret_per"].astype(int)
    mean_flow_df.to_parquet(os.path.join(maptable_outdir, style_table_file_name))
    return


# runs function on file execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--ymd',
        type=str,
        required=True,
        help="Date in YYYYMMDD format"
    )
    parser.add_argument(
        '--vpu',
        type=str,
        required=True,
        help="VPU number"
    )
    args = parser.parse_args()

    ymd = args.ymd
    vpu = args.vpu

    postprocess_vpu_forecast_directory(ymd=ymd, vpu=vpu)
