#!/bin/bash

./slurm/benchmark.sh 2 \
    --dataset /projects/prjs0981/ewalt/zarr-snellius-benchmark/data/era5_6h_aurora_2020-2022-wb13-6h-1440x721.zarr-v2 \
    --experiment_dir ./experiments/6h_1440x721
./slurm/benchmark.sh 3 \
    --dataset /projects/prjs0981/ewalt/zarr-snellius-benchmark/data/era5_6h_aurora_2020-2022-wb13-6h-1440x721.zarr-v3 \
    --experiment_dir ./experiments/6h_1440x721

# Benchmarks gcs with less samples because it is so slow
./slurm/benchmark.sh 2 \
    --dataset gs://weatherbench2/datasets/era5/1959-2023_01_10-wb13-6h-1440x721.zarr/ \
    --num_samples 20 \
    --date_start 2020-01-01 \
    --date_end 2022-12-31 \
    --experiment_dir ./experiments/6h_1440x721
./slurm/benchmark.sh 3 \
    --dataset gs://weatherbench2/datasets/era5/1959-2023_01_10-wb13-6h-1440x721.zarr/ \
    --num_samples 20 \
    --date_start 2020-01-01 \
    --date_end 2022-12-31 \
    --experiment_dir ./experiments/6h_1440x721