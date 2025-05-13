#!/bin/bash

# get dataset variable as $1
zarr_version=$1
dataset=$2

# check if both variables are set
if [ -z "$zarr_version" ] || [ -z "$dataset" ]; then
    echo "Usage: $0 <zarr_version> <dataset>"
    exit 1
fi

slurm/benchmark.sh \
    ${zarr_version} \
    --dataset ${dataset} \