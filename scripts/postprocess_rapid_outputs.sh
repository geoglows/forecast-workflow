#!/bin/bash

# Function to check if files exist
check_files_exist() {
  local files=("$@")
  for file in "${files[@]}"; do
    if [[ ! -e "$file" ]]; then
      echo "Error: File $file not found."
      return 1
    fi
  done
  return 0
}

# use getopts to parse the -d options
while getopts "d:" flag; do
  case "${flag}" in
    d) rapid_output_directory=${OPTARG} ;;
    *) echo "Invalid option" ;;
  esac
done

echo "Looking for rapid outputs in directory: $rapid_output_directory"
cd "$rapid_output_directory" || exit

# Initialize an array to store VPU numbers
declare -a vpu_numbers=()
# Read the output of the command line by line and store VPU numbers in the array
while IFS= read -r vpu_number; do
  vpu_numbers+=("$vpu_number")
done < <(ls -1 Qout_*.nc | awk -F_ '{print $2}' | sort -u)

echo "Found ${#vpu_numbers[@]} VPUs"

for vpu_number in "${vpu_numbers[@]}"; do
  # set output file names
  avg_output_file="nces_avg_${vpu_number}.nc"
  concat_output_file="Qout_${vpu_number}.nc"

  # check if the files exist
  if check_files_exist "$avg_output_file" "$concat_output_file"; then
    echo "Skipping VPU number $vpu_number due to missing files."
    continue
  fi

  # calculate the ensemble mean
  echo "Calculating ensemble mean for VPU number $vpu_number"
  nces $(ls -1 Qout_${vpu_number}_*.nc | grep -v Qout_${vpu_number}_52.nc | sort -V) -O --op_typ=avg -o $avg_output_file ||
    if [[ $? -ne 0 ]]; then
      echo "Failed to calculate ensemble mean for VPU number $vpu_number"
      continue
    fi

  # concatenate along a new dimension which is called record by default
  echo "Concatenating Ensembles 1-51 for VPU number $vpu_number"
  ncecat $(ls -1 Qout_${vpu_number}_*.nc | grep -v Qout_${vpu_number}_52.nc | sort -V) -O $output_file ||
    if [[ $? -ne 0 ]]; then
      echo "Failed to concatenate ensembles 1-51 for VPU number $vpu_number"
      continue
    fi

  # rename record to ensemble inplace
  echo "Renaming record to ensemble for VPU number $vpu_number"
  ncrename -d record,ensemble $output_file ||
    if [[ $? -ne 0 ]]; then
      echo "Failed to rename record to ensemble for VPU number $vpu_number"
      continue
    fi

  # remove the files Qout_${vpu_number}_*.nc
  echo "Removing individual ensemble files for VPU number $vpu_number"
  files_to_delete=($(ls -1 Qout_${vpu_number}_*.nc | grep -v Qout_${vpu_number}_52.nc | sort -V))
  if check_files_exist "${files_to_delete[@]}"; then
    for file in "${files_to_delete[@]}"; do
      rm "$file" || echo "Failed to remove $file"
    done
  else
    echo "Skipping deletion for VPU number $vpu_number due to missing files."
  fi
done
