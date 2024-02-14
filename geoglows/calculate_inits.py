import datetime
import glob
import xarray as xr
import argparse
import pandas as pd
import os
import netCDF4 as nc

from multiprocessing import Pool

FORECASTS_DIR = os.environ['FORECASTS_DIR']
CONFIGS_DIR = os.environ['CONFIGS_DIR']
RUNOFFS_DIR = os.environ['RUNOFFS_DIR']
INITS_DIR = os.environ['INITS_DIR']


def init_file_from_forecast_averages(ymd: str, vpu: str or int) -> None:
    init_date = pd.to_datetime(ymd) + pd.Timedelta(days=1)
    init_date_string = init_date.strftime('%Y%m%d')
    average_flow_file = os.path.join(FORECASTS_DIR, f'nces_avg_{vpu}.nc')
    init_output_file = os.path.join(INITS_DIR, vpu, f'Qinit_{vpu}_{init_date_string}.nc')

    print(f'Creating initial flow file for {vpu} for initialization date of {init_date_string}')
    print(f'Using average flow file: {average_flow_file}')
    print(f'Output file: {init_output_file}')

    with xr.open_dataset(average_flow_file) as ds:
        # select the timestep 24 hours after the ymd variable
        init_flows = ds.Qout.sel(time=init_date).values
        rivids = ds.rivid.values

    with nc.Dataset(init_output_file, "w", format="NETCDF3_CLASSIC") as inflow_nc:
        # create dimensions
        inflow_nc.createDimension('time', 1)
        inflow_nc.createDimension('rivid', rivids.shape[0])

        qout_var = inflow_nc.createVariable('Qout', 'f8', ('time', 'rivid'))
        qout_var[:] = init_flows
        qout_var.long_name = 'instantaneous river water discharge downstream of each river reach'
        qout_var.units = 'm3 s-1'
        qout_var.coordinates = 'lon lat'
        qout_var.grid_mapping = 'crs'
        qout_var.cell_methods = "time: point"

        # rivid
        rivid_var = inflow_nc.createVariable('rivid', 'i4', ('rivid',))
        rivid_var[:] = rivids
        rivid_var.long_name = 'unique identifier for each river reach'
        rivid_var.units = '1'
        rivid_var.cf_role = 'timeseries_id'

        # time
        time_var = inflow_nc.createVariable('time', 'i4', ('time',))
        time_var[:] = 0
        time_var.long_name = 'time'
        time_var.standard_name = 'time'
        time_var.units = f'seconds since {init_date.strftime("%Y-%m-%d")}'  # Must be seconds
        time_var.axis = 'T'
        time_var.calendar = 'gregorian'

        # longitude
        lon_var = inflow_nc.createVariable('lon', 'f8', ('rivid',))
        lon_var[:] = 0
        lon_var.long_name = 'longitude of a point related to each river reach'
        lon_var.standard_name = 'longitude'
        lon_var.units = 'degrees_east'
        lon_var.axis = 'X'

        # latitude
        lat_var = inflow_nc.createVariable('lat', 'f8', ('rivid',))
        lat_var[:] = 0
        lat_var.long_name = 'latitude of a point related to each river reach'
        lat_var.standard_name = 'latitude'
        lat_var.units = 'degrees_north'
        lat_var.axis = 'Y'

        # crs
        crs_var = inflow_nc.createVariable('crs', 'i4')
        crs_var.grid_mapping_name = 'latitude_longitude'
        crs_var.epsg_code = 'EPSG:4326'  # WGS 84
        crs_var.semi_major_axis = 6378137.0
        crs_var.inverse_flattening = 298.257223563

        # add global attributes
        inflow_nc.Conventions = 'CF-1.6'
        inflow_nc.history = f'date_created: {datetime.datetime.now(datetime.UTC)}'
        inflow_nc.featureType = 'timeSeries'


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--ymd',
        type=str,
        required=True,
        help='Year, month, and day in YYYYMMDD format',
    )
    args = parser.parse_args()
    ymd = args.ymd

    # list all the VPU directories
    vpus = [x for x in glob.glob(os.path.join(CONFIGS_DIR, '*')) if os.path.isdir(x)]
    vpus = [os.path.basename(x) for x in vpus]

    pool_cpus = min([len(vpus), os.cpu_count()])

    with Pool(pool_cpus) as p:
        p.starmap(init_file_from_forecast_averages, [(ymd, os.path.basename(vpu_dir)) for vpu_dir in vpu_dirs])
