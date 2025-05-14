#!/bin/bash

# Zarr version 2
./slurm/download.sh \
    2 \
    --dataset gs://weatherbench2/datasets/era5/1959-2023_01_10-wb13-6h-1440x721.zarr/ \
    --output /projects/prjs0981/ewalt/zarr-snellius-benchmark/data/era5_6h_aurora_2020-2022-wb13-6h-1440x721.zarr/ \
    --date_start 2020-01-01 \
    --date_end 2022-12-31

# Zarr version 3
./slurm/download.sh \
    3 \
    --dataset gs://weatherbench2/datasets/era5/1959-2023_01_10-wb13-6h-1440x721.zarr/ \
    --output /projects/prjs0981/ewalt/zarr-snellius-benchmark/data/era5_6h_aurora_2020-2022-wb13-6h-1440x721.zarr/ \
    --date_start 2020-01-01 \
    --date_end 2022-12-31