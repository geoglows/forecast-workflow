import argparse
import datetime
import glob
import os
import subprocess
from multiprocessing import Pool


def timestamp():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %X')


def run_rapid_for_namelist_directory(namelist_dir: str,
                                     path_rapid_exec: str = '/home/rapid/src/rapid',
                                     logdir: str = '/mnt/logs') -> None:
    watershed_id = os.path.basename(namelist_dir)
    with open(os.path.join(logdir, f"{watershed_id}.log"), 'w') as f:
        for namelist in sorted(glob.glob(os.path.join(namelist_dir, '*namelist*'))):
            try:
                f.write(f'{timestamp()}: Running RAPID for {namelist}')
                subprocess.call(
                    [path_rapid_exec, '--namelist', namelist, '--ksp_type', 'preonly'],
                    stdout=f,
                    stderr=f,
                )
                f.write(f'{timestamp()}: Finished RAPID for {namelist}')
            except Exception as e:
                print(e)
                f.write(e)
                f.write(f'Failed to run RAPID for {namelist}')

    return


if __name__ == '__main__':
    """
    Run RAPID for all namelist files in a specified directory

    Usage:
        python run_rapid.py <path_to_rapid_executable> <namelists_dir>

    Directory structure:
        <namelist_directory>
            rapid_namelist_<watershed_1_id>
            rapid_namelist_<watershed_2_id>
            rapid_namelist_<watershed_3_id>
            ...

    Returns:
        None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--namelistsdir', type=str, required=False,
                        default='/mnt/namelists',
                        help='Path to directory containing subdirectories of namelist files', )
    parser.add_argument('--logdir', type=str, required=False,
                        default='/mnt/logs',
                        help='Path to directory to store logs', )
    parser.add_argument('--rapidexec', type=str, required=False,
                        default='/home/rapid/src/rapid',
                        help='Path to rapid executable', )
    parser.add_argument('--sortdirs', type=bool, action='store_true',
                        default=True,
                        help='Order computations by large to small watershed size', )

    args = parser.parse_args()
    path_to_rapid_exec = args.rapidexec
    namelists_dirs = args.namelistsdir
    logs_dir = args.logdir
    sort_dirs = args.sortdirs

    namelists_dirs = [d for d in glob.glob(os.path.join(namelists_dirs, '*')) if os.path.isdir(d)]
    sorted_order = (
        605, 714, 109, 302, 609, 122, 402, 303, 304, 502, 214, 406, 105, 111, 804, 106, 409, 503, 404, 706, 103, 412,
        703, 508, 715, 501, 513, 217, 123, 407, 607, 704, 126, 210, 408, 221, 603, 701, 220, 413, 218, 216, 613, 205,
        414, 112, 213, 102, 709, 419, 116, 801, 411, 203, 418, 415, 510, 110, 711, 612, 606, 209, 707, 608, 611, 119,
        113, 211, 422, 201, 301, 120, 505, 117, 121, 708, 610, 506, 101, 401, 509, 421, 206, 511, 410, 713, 115, 215,
        125, 118, 108, 423, 716, 202, 504, 416, 114, 512, 604, 405, 712, 124, 710, 602, 403, 219, 507, 212, 104, 702,
        208, 803, 107, 417, 601, 614, 204, 802, 718, 717, 420, 305, 207, 705, 514,
    )
    if sort_dirs:
        namelists_dirs = sorted(namelists_dirs, key=lambda x: sorted_order.index(int(os.path.basename(x))))

    cpu_count = min([os.cpu_count(), len(namelists_dirs)])
    print(f'Found {len(namelists_dirs)} input directories')
    print(f'Have {os.cpu_count()} cpus')
    print(f'Using {cpu_count} cpus')

    with Pool(cpu_count) as p:
        # p.starmap(run_rapid_for_namelist_directory, [(d, path_to_rapid_exec, logs_dir) for d in namelists_dirs])
        for d in namelists_dirs:
            p.apply_async(run_rapid_for_namelist_directory, args=(d, path_to_rapid_exec, logs_dir,))
        p.close()
        p.join()
