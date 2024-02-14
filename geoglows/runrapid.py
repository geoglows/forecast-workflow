import argparse
import datetime
import glob
import os
import subprocess
from multiprocessing import Pool


def timestamp():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %X')


def run_rapid_for_namelist_file(namelist_file: str,
                                logdir: str,
                                path_rapid_exec: str = '/home/rapid/src/rapid', ) -> None:
    vpuno = os.path.basename(namelist_file).split("_")[1]
    label = os.path.basename(namelist_file).split("_")[2]
    try:
        with open(os.path.join(logdir, f"{vpuno}_{label}.log"), 'w') as f:
            f.write(f'{timestamp()}: Running RAPID for {namelist_file}')
            subprocess.call(
                [path_rapid_exec, '--namelist', namelist_file, '--ksp_type', 'preonly'],
                stdout=f,
                stderr=f,
            )
            f.write(f'{timestamp()}: Finished RAPID for {namelist_file}')
    except Exception as e:
        print(e)
        f.write(e)
        f.write(f'Failed to run RAPID for {namelist_file}')

    return


if __name__ == '__main__':
    print('runrapid.py')
    parser = argparse.ArgumentParser()
    parser.add_argument('--fcdir', type=str, required=True,
                        help='Path to forecast working directory', )
    parser.add_argument('--rapidexec', type=str, required=False,
                        default='/home/rapid/src/rapid',
                        help='Path to rapid executable', )

    args = parser.parse_args()
    fcdir = args.fcdir
    path_to_rapid_exec = args.rapidexec

    logs_dir = os.path.join(fcdir, 'logs')
    namelists_dirs = os.path.join(fcdir, 'namelists')

    namelist_files = glob.glob(os.path.join(namelists_dirs, '*'))
    sorted_order = (
        605, 714, 109, 302, 609, 122, 402, 303, 304, 502, 214, 406, 105, 111, 804, 106, 409, 503, 404, 706, 103, 412,
        703, 508, 715, 501, 513, 217, 123, 407, 607, 704, 126, 210, 408, 221, 603, 701, 220, 413, 218, 216, 613, 205,
        414, 112, 213, 102, 709, 419, 116, 801, 411, 203, 418, 415, 510, 110, 711, 612, 606, 209, 707, 608, 611, 119,
        113, 211, 422, 201, 301, 120, 505, 117, 121, 708, 610, 506, 101, 401, 509, 421, 206, 511, 410, 713, 115, 215,
        125, 118, 108, 423, 716, 202, 504, 416, 114, 512, 604, 405, 712, 124, 710, 602, 403, 219, 507, 212, 104, 702,
        208, 803, 107, 417, 601, 614, 204, 802, 718, 717, 420, 305, 207, 705, 514,
    )
    namelist_files = sorted(namelist_files, key=lambda x: sorted_order.index(int(os.path.basename(x).split("_")[1])))
    cpu_count = min([os.cpu_count(), len(namelist_files)])

    print(f'Forecasts directory: {fcdir}')
    print(f'Namelist directory: {namelists_dirs}')
    print(f'RAPID executable: {path_to_rapid_exec}')
    print(f'Found {len(namelist_files)} namelist files')
    print(f'Have {os.cpu_count()} cpus')
    print(f'Using {cpu_count} cpus')
    print(f'Logs will be written to {logs_dir}')
    print(f'Running RAPID from {path_to_rapid_exec}')

    with Pool(cpu_count) as p:
        print('Beginning RAPID runs')
        for f in namelist_files:
            p.apply_async(run_rapid_for_namelist_file, args=(f, logs_dir, path_to_rapid_exec,))
        p.close()
        p.join()
        print('Finished RAPID runs')
