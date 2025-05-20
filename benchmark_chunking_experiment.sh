#!/bin/bash

# 240x121
# dataset
./slurm/benchmark.sh 2 \
    --dataset /projects/prjs0981/ewalt/zarr-snellius-benchmark/data/chunking_exp/era5_weekly_aurora_2022-2022-wb13-1h-240x121.ds.zarr-v2 \
    --experiment_dir ./experiments/chunking_exp_240x121
# dataarray
./slurm/benchmark.sh 2 \
    --dataset /projects/prjs0981/ewalt/zarr-snellius-benchmark/data/chunking_exp/era5_weekly_aurora_2022-2022-wb13-1h-240x121.da.zarr-v2 \
    --experiment_dir ./experiments/chunking_exp_240x121

# # 1440x721
# # dataset
# ./slurm/benchmark.sh 2 \
#     --dataset /projects/prjs0981/ewalt/zarr-snellius-benchmark/data/era5_6h_aurora_2020-2022-wb13-6h-1440x721.zarr-v2/ \
#     --experiment_dir ./experiments/chunking_exp_1440x721
# # dataarray
# ./slurm/benchmark.sh 2 \
#     --dataset /projects/prjs0981/ewalt/zarr-snellius-benchmark/data/chunking_exp/era5_6h_aurora_2022-2022-wb13-6h-1440x721.da.zarr-v2 \
#     --experiment_dir ./experiments/chunking_exp_1440x721
