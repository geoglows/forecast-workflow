import argparse
import glob
import logging
import os
import sys
from multiprocessing import Pool

import netCDF4
import pandas as pd

FORECASTS_DIR = os.environ['FORECASTS_DIR']
CONFIGS_DIR = os.environ['CONFIGS_DIR']


def rapid_namelist(
        namelist_save_path: str,

        k_file: str,
        x_file: str,
        riv_bas_id_file: str,
        rapid_connect_file: str,
        vlat_file: str,
        qout_file: str,

        time_total: int,
        timestep_calc_routing: int,
        timestep_calc: int,
        timestep_inp_runoff: int,

        # Optional - Flags for RAPID Options
        run_type: int = 1,
        routing_type: int = 1,

        use_qinit_file: bool = False,
        qinit_file: str = '',  # qinit_VPU_DATE.csv

        write_qfinal_file: bool = True,
        qfinal_file: str = '',

        compute_volumes: bool = False,
        v_file: str = '',

        use_dam_model: bool = False,  # todo more options here
        use_influence_model: bool = False,
        use_forcing_file: bool = False,
        use_uncertainty_quantification: bool = False,

        opt_phi: int = 1,

        # Optional - Can be determined from rapid_connect
        reaches_in_rapid_connect: int = None,
        max_upstream_reaches: int = None,

        # Optional - Can be determined from riv_bas_id_file
        reaches_total: int = None,

        # Optional - Optimization Runs Only
        time_total_optimization: int = 0,
        timestep_observations: int = 0,
        timestep_forcing: int = 0,
) -> None:
    """
    Generate a namelist file for a RAPID routing run

    All units are strictly SI: meters, cubic meters, seconds, cubic meters per second, etc.

    Args:
        namelist_save_path (str): Path to save the namelist file
        k_file (str): Path to the k_file (input)
        x_file (str): Path to the x_file (input)
        rapid_connect_file (str): Path to the rapid_connect_file (input)
        qout_file (str): Path to save the Qout_file (routed discharge file)
        vlat_file (str): Path to the Vlat_file (inflow file)

    Returns:
        None
    """
    assert run_type in [1, 2], 'run_type must be 1 or 2'
    assert routing_type in [1, 2, 3, ], 'routing_type must be 1, 2, 3, or 4'
    assert opt_phi in [1, 2], 'opt_phi must be 1, or 2'

    if any([x is None for x in (reaches_in_rapid_connect, max_upstream_reaches)]):
        df = pd.read_csv(rapid_connect_file, header=None)
        reaches_in_rapid_connect = df.shape[0]
        rapid_connect_columns = ['rivid', 'next_down', 'count_upstream']  # plus 1 per possible upstream reach
        max_upstream_reaches = df.columns.shape[0] - len(rapid_connect_columns)

    if reaches_total is None:
        df = pd.read_csv(riv_bas_id_file, header=None)
        reaches_total = df.shape[0]

    namelist_options = {
        'BS_opt_Qfinal': f'.{str(write_qfinal_file).lower()}.',
        'BS_opt_Qinit': f'.{str(use_qinit_file).lower()}.',
        'BS_opt_dam': f'.{str(use_dam_model).lower()}.',
        'BS_opt_for': f'.{str(use_forcing_file).lower()}.',
        'BS_opt_influence': f'.{str(use_influence_model).lower()}.',
        'BS_opt_V': f'.{str(compute_volumes).lower()}.',
        'BS_opt_uq': f'.{str(use_uncertainty_quantification).lower()}.',

        'k_file': f"'{k_file}'",
        'x_file': f"'{x_file}'",
        'rapid_connect_file': f"'{rapid_connect_file}'",
        'riv_bas_id_file': f"'{riv_bas_id_file}'",
        'Qout_file': f"'{qout_file}'",
        'Vlat_file': f"'{vlat_file}'",
        'V_file': f"'{v_file}'",

        'IS_opt_run': run_type,
        'IS_opt_routing': routing_type,
        'IS_opt_phi': opt_phi,
        'IS_max_up': max_upstream_reaches,
        'IS_riv_bas': reaches_in_rapid_connect,
        'IS_riv_tot': reaches_total,

        'IS_dam_tot': 0,
        'IS_dam_use': 0,
        'IS_for_tot': 0,
        'IS_for_use': 0,

        'Qinit_file': f"'{qinit_file}'",
        'Qfinal_file': f"'{qfinal_file}'",

        'ZS_TauR': timestep_inp_runoff,
        'ZS_dtR': timestep_calc_routing,
        'ZS_TauM': time_total,
        'ZS_dtM': timestep_calc,
        'ZS_TauO': time_total_optimization,
        'ZS_dtO': timestep_observations,
        'ZS_dtF': timestep_forcing,
    }

    # generate the namelist file
    namelist_string = '\n'.join([
        '&NL_namelist',
        *[f'{key} = {value}' for key, value in namelist_options.items()],
        '/',
        ''
    ])

    with open(namelist_save_path, 'w') as f:
        f.write(namelist_string)


def create_rapid_namelist(vpu_directory: str,
                          inflow_file: str,
                          namelist_directory: str,
                          outputs_directory: str,
                          file_label: str = None,
                          end_date: str = None,
                          qinit_file: str = None,
                          qfinal_file: str = None, ) -> None:
    vpu_code = os.path.basename(vpu_directory)
    k_file = os.path.join(vpu_directory, f'k.csv')
    x_file = os.path.join(vpu_directory, f'x.csv')
    riv_bas_id_file = os.path.join(vpu_directory, f'riv_bas_id.csv')
    rapid_connect_file = os.path.join(vpu_directory, f'rapid_connect.csv')

    for x in (k_file, x_file, riv_bas_id_file, rapid_connect_file):
        assert os.path.exists(x), f'{x} does not exist'

    write_qfinal_file = bool(qfinal_file)
    use_qinit_file = bool(qinit_file)

    vlat_file = inflow_file
    namelist_file_name = f'namelist_{vpu_code}'
    qout_file_name = inflow_file.replace('m3', 'Qout')
    qinit_file = qinit_file if qinit_file else ''
    qfinal_file = os.path.join(outputs_directory, f'Qfinal_{vpu_code}_{end_date}.nc')

    if file_label:
        namelist_file_name += f'_{file_label}'
        qout_file_name = qout_file_name.replace('.nc', f'_{file_label}.nc')
        qfinal_file = qfinal_file.replace('.nc', f'_{file_label}.nc')

    os.makedirs(outputs_directory, exist_ok=True)
    os.makedirs(namelist_directory, exist_ok=True)

    namelist_path = os.path.join(namelist_directory, namelist_file_name)
    qout_path = os.path.join(outputs_directory, qout_file_name)

    with netCDF4.Dataset(inflow_file) as ds:
        time_step_inflows = ds['time_bnds'][0, 1] - ds['time_bnds'][0, 0]
        time_total_inflow = ds['time_bnds'][-1, 1] - ds['time_bnds'][0, 0]
    time_total = time_total_inflow
    timestep_inp_runoff = time_step_inflows
    timestep_calc = time_step_inflows
    timestep_calc_routing = 900

    rapid_namelist(namelist_save_path=namelist_path,
                   k_file=k_file,
                   x_file=x_file,
                   riv_bas_id_file=riv_bas_id_file,
                   rapid_connect_file=rapid_connect_file,
                   vlat_file=vlat_file,
                   qout_file=qout_path,
                   time_total=time_total,
                   timestep_calc_routing=timestep_calc_routing,
                   timestep_calc=timestep_calc,
                   timestep_inp_runoff=timestep_inp_runoff,
                   write_qfinal_file=write_qfinal_file,
                   qfinal_file=qfinal_file,
                   use_qinit_file=use_qinit_file,
                   qinit_file=qinit_file, )

    return


def call_make_namelist(kwargs_dict: dict):
    create_rapid_namelist(**kwargs_dict)


if __name__ == '__main__':
    """
    Prepare rapid namelist files for a directory of VPU inputs
    """
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--ymd', type=str, required=True)
    args = argparser.parse_args()

    ymd = args.ymd

    inflows_dir = os.path.join(FORECASTS_DIR, ymd, 'inflows')
    namelists_dir = os.path.join(FORECASTS_DIR, ymd, 'namelists')
    outputs_dir = os.path.join(FORECASTS_DIR, ymd, 'outputs')

    os.makedirs(namelists_dir, exist_ok=True)
    os.makedirs(outputs_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout,
    )

    jobs = []

    vpu_dirs = sorted([x for x in glob.glob(os.path.join(CONFIGS_DIR, '*')) if os.path.isdir(x)])
    for vpu_dir in vpu_dirs:
        vpu = os.path.basename(vpu_dir)
        for inflow in glob.glob(os.path.join(inflows_dir, f'm3_{vpu}_*.nc')):
            jobs.append([{
                "vpu_directory": vpu_dir,
                "inflow_file": inflow,
                "namelist_directory": namelists_dir,
                "outputs_directory": outputs_dir,
                "file_label": os.path.splitext(inflow)[0].split('_')[-1],
            }, ])

    with Pool(min(len(jobs), os.cpu_count())) as p:
        p.starmap(call_make_namelist, jobs)
