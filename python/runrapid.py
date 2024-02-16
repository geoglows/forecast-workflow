import argparse
import datetime
import os
import subprocess


def timestamp():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %X')


def run_rapid_for_namelist_file(namelist_file: str,
                                logdir: str,
                                path_rapid_exec: str = '/home/rapid/src/rapid', ) -> None:
    try:
        print(f'Running RAPID for {namelist_file}')
        vpuno = os.path.basename(namelist_file).split("_")[1]
        label = os.path.basename(namelist_file).split("_")[2]
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
    parser.add_argument('--namelist', type=str, required=False,
                        help='Path to namelist file', )
    parser.add_argument('--rapidexec', type=str, required=False,
                        default='/home/rapid/src/rapid',
                        help='Path to rapid executable', )

    args = parser.parse_args()
    fcdir = args.fcdir
    namelist = args.namelist
    path_to_rapid_exec = args.rapidexec
    logs_dir = os.path.join(fcdir, 'logs')
    run_rapid_for_namelist_file(namelist, logs_dir, path_to_rapid_exec)
