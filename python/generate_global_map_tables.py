import argparse
import glob
import logging
import os

import polars as pl
from natsort import natsorted

FORECASTS_DIR = os.environ['FORECASTS_DIR']


def combine_esri_tables(ymd: str) -> None:
    # get path to tables from workspace
    vpu_parquet_tables = natsorted(glob.glob(os.path.join(FORECASTS_DIR, ymd, "maptables", 'map*parquet')))
    global_csv_tables_dir = os.path.join(FORECASTS_DIR, ymd, "maptables")

    # select all outputs/VPUNUMBER/DATE/map_style_table*.parquet files
    logging.info("Concatenating parquet map_style_tables from each VPU")
    global_map_style_df = pl.concat([pl.read_parquet(x) for x in vpu_parquet_tables])

    # replace nans with 0
    logging.info("Preparing concatenated DF")
    global_map_style_df = global_map_style_df.fill_nan(0)

    # for each unique date in the timestamp column, create a new dataframe
    for date in global_map_style_df['timestamp'].unique():
        file_save_path = os.path.join(global_csv_tables_dir, f'mapstyletable_{date.strftime("%Y-%m-%d-%H")}.csv')
        global_map_style_df.filter(pl.col('timestamp') == date).write_csv(file_save_path)

    for parquet_table in vpu_parquet_tables:
        os.remove(parquet_table)
    return


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--ymd', type=str, required=True,)
    args = argparser.parse_args()
    ymd = args.ymd

    combine_esri_tables(ymd)
