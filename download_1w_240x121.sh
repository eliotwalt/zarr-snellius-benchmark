# Downloads zarr2
./slurm/download.sh \
    2 \
    --dataset gs://weatherbench2/datasets/era5_weekly/1959-2023_01_10-1h-240x121_equiangular_with_poles_conservative.zarr/ \
    --output data/era5_weekly_aurora_2000-2022-wb13-1h-240x121.zarr \
    --date_start 2000-01-01 \
    --date_end 2022-12-31
./slurm/download.sh \
    2 \
    --dataset gs://weatherbench2/datasets/era5_weekly/1959-2023_01_10-1h-240x121_equiangular_with_poles_conservative.zarr/ \
    --output data/era5_weekly_aurora_2010-2022-wb13-1h-240x121.zarr \
    --date_start 2010-01-01 \
    --date_end 2022-12-31
./slurm/download.sh \
    2 \
    --dataset gs://weatherbench2/datasets/era5_weekly/1959-2023_01_10-1h-240x121_equiangular_with_poles_conservative.zarr/ \
    --output data/era5_weekly_aurora_2020-2022-wb13-1h-240x121.zarr \
    --date_start 2020-01-01 \
    --date_end 2022-12-31

# Downloads zarr3
./slurm/download.sh \
    3 \
    --dataset gs://weatherbench2/datasets/era5_weekly/1959-2023_01_10-1h-240x121_equiangular_with_poles_conservative.zarr/ \
    --output data/era5_weekly_aurora_2020-2022-wb13-1h-240x121.zarr \
    --date_start 2020-01-01 \
    --date_end 2022-12-31
