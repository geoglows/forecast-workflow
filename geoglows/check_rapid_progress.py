import glob
import os
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--inflowsdir', type=str, required=False,
                        default='/mnt/inflows',
                        help='Path to directory containing subdirectories of inflow files', )
    parser.add_argument('--outflowsdir', type=str, required=False,
                        default='/mnt/outputs',
                        help='Path to directory containing subdirectories of outflow files', )

    args = parser.parse_args()
    inflows_dir = args.inflowsdir
    outflows_dir = args.outflowsdir

    outputdirs = sorted([d for d in glob.glob(os.path.join(outflows_dir, '*')) if os.path.isdir(d)])

    num_output_dirs = len(outputdirs)
    outputs_complete = 0

    for outputdir in outputdirs:
        # identify the watershed being checked
        watershed_id = os.path.basename(outputdir)

        # look for files in the output directory
        qout_file = glob.glob(os.path.join(outputdir, 'Qout*.nc'))
        qout_size = sum([os.path.getsize(x) for x in qout_file])

        inflow_files = glob.glob(os.path.join(inflows_dir, watershed_id, '*.nc'))
        inflow_size = sum([os.path.getsize(x) for x in inflow_files])
        # calculate percent completed
        pct_complete = (qout_size / inflow_size) * 100

        if pct_complete >= 100:
            outputs_complete += 1
            print(f'{watershed_id}: COMPLETE {round(pct_complete, 1)}%')
        else:
            print(f'{watershed_id}: {round(pct_complete, 1)}%')

    print(f'{outputs_complete} / {num_output_dirs} complete: {round((outputs_complete / num_output_dirs) * 100, 1)}%')

