Benchmarks gcs with less samples because it is so slow
./slurm/benchmark.sh 2 \
    --dataset gs://weatherbench2/datasets/era5_weekly/1959-2023_01_10-1h-240x121_equiangular_with_poles_conservative.zarr/ \
    --num_samples 20 \
    --date_start 2020-01-01 \
    --date_end 2022-12-31
./slurm/benchmark.sh 3 \
    --dataset gs://weatherbench2/datasets/era5_weekly/1959-2023_01_10-1h-240x121_equiangular_with_poles_conservative.zarr/ \
    --num_samples 20 \
    --date_start 2020-01-01 \
    --date_end 2022-12-31

# Benchmarks local zarr2
./slurm/benchmark.sh 2 \
    --dataset ./data/era5_weekly_aurora_2000-2022-wb13-1h-240x121.zarr-v2
./slurm/benchmark.sh 2 \
    --dataset ./data/era5_weekly_aurora_2010-2022-wb13-1h-240x121.zarr-v2
./slurm/benchmark.sh 2 \
    --dataset ./data/era5_weekly_aurora_2020-2022-wb13-1h-240x121.zarr-v2

# Benchmarks local zarr3
./slurm/benchmark.sh 3 \
    --dataset ./data/era5_weekly_aurora_2020-2022-wb13-1h-240x121.zarr-v3