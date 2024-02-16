#!/usr/bin/env bash

# Function to check if files exist
check_files_exist() {
  local files=("$@")
  # if all files exist return 0, else return 1
  for file in "${files[@]}"; do
    if [[ ! -f "$file" ]]; then
      echo "File $file does not exist"
      return 1
    fi
  done
  return 0
}

# use getopts to parse the -d options
while getopts "d:v:" flag; do
  case "${flag}" in
    d) rapid_output_directory=${OPTARG} ;;
    v) vpu=${OPTARG} ;;
    *) echo "Invalid option" ;;
  esac
done

echo "Looking for rapid outputs in directory: $rapid_output_directory"
cd "$rapid_output_directory" || exit

# set output file names
avg_output_file="nces_avg_${vpu}.nc"
concat_output_file="Qout_${vpu}.nc"

# calculate the ensemble mean
echo "Calculating ensemble mean for VPU number $vpu"
nces $(ls -1 Qout_${vpu}_*.nc | grep -v Qout_${vpu}*_52.nc | sort -V) -O --op_typ=avg -o "$avg_output_file" ||
  if [[ $? -ne 0 ]]; then
    echo "Failed to calculate ensemble mean for VPU number $vpu"
  fi

# concatenate along a new dimension which is called record by default
echo "Concatenating Ensembles 1-51 for VPU number $vpu"
ncecat $(ls -1 Qout_${vpu}_*.nc | grep -v Qout_${vpu}*_52.nc | sort -V) -O "$concat_output_file" ||
  if [[ $? -ne 0 ]]; then
    echo "Failed to concatenate ensembles 1-51 for VPU number $vpu"
  fi

# rename record to ensemble inplace
echo "Renaming record to ensemble for VPU number $vpu"
ncrename -d record,ensemble "$concat_output_file" ||
  if [[ $? -ne 0 ]]; then
    echo "Failed to rename record to ensemble for VPU number $vpu"
  fi

# remove the files Qout_${vpu}_*.nc
echo "Removing individual ensemble files for VPU number $vpu"
files_to_delete=($(ls -1 Qout_${vpu}_*.nc | grep -v Qout_${vpu}*_52.nc | sort -V))
if check_files_exist "${files_to_delete[@]}"; then
  for file in "${files_to_delete[@]}"; do
    rm "$file" || echo "Failed to remove $file"
  done
else
  echo "Skipping deletion for VPU number $vpu due to missing files."
fi
