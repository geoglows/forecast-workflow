import argparse
import datetime
import subprocess
import sys


def timestamp():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %X')


def run_rapid_for_namelist_file(namelist_file: str,
                                path_rapid_exec: str = '/home/rapid/src/rapid', ) -> None:
    print(f'Running RAPID for {namelist_file}')
    try:
        print(f'{timestamp()}: Running RAPID for {namelist_file}')
        subprocess.call(
            [path_rapid_exec, '--namelist', namelist_file, '--ksp_type', 'preonly'],
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        print(f'{timestamp()}: Finished RAPID for {namelist_file}')
    except Exception as e:
        print(e)
        print(f'Failed to run RAPID for {namelist_file}')

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--namelist', type=str, required=False,
                        help='Path to namelist file', )
    parser.add_argument('--rapidexec', type=str, required=False,
                        default='/home/rapid/src/rapid',
                        help='Path to rapid executable', )

    args = parser.parse_args()
    namelist = args.namelist
    path_to_rapid_exec = args.rapidexec
    run_rapid_for_namelist_file(namelist, path_to_rapid_exec)
