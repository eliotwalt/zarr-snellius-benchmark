#!/bin/bash
zarr_version=$1
year_start=$2
year_end=$3


# Check if all arguments are provided
if [ -z "$zarr_version" ] || [ -z "$year_start" ] || [ -z "$year_end" ]; then
    echo "Usage: $0 <zarr_version> <year_start> <year_end>"
    exit 1
fi

slurm/download.sh \
    ${zarr_version} \
    --dataset gs://weatherbench2/datasets/era5_weekly/1959-2023_01_10-1h-240x121_equiangular_with_poles_conservative.zarr/ \
    --output data/era5_weekly_aurora_${year_start}-${year_end}-wb13-1h-240x121.zarr \
    --date_start ${year_start}-01-01 \
    --date_end ${year_end}-12-31