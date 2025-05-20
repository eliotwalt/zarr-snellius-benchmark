# 240x121
dataset=gs://weatherbench2/datasets/era5_weekly/1959-2023_01_10-1h-240x121_equiangular_with_poles_conservative.zarr/
start_year=2022
end_year=2022

output="/projects/prjs0981/ewalt/zarr-snellius-benchmark/data/chunking_exp/era5_weekly_aurora_$start_year-$end_year-wb13-1h-240x121"

# Download dataset 
./slurm/download.sh \
    2 \
    --dataset $dataset \
    --output $output.ds.zarr \
    --date_start $start_year-01-01 \
    --date_end $end_year-12-31 \
    --chunks_write_strategy optimal_dataset

# Download dataarray
./slurm/download.sh \
    2 \
    --dataset $dataset \
    --output $output.da.zarr \
    --date_start $start_year-01-01 \
    --date_end $end_year-12-31 \
    --chunks_write_strategy optimal_dataarray

# 1440x721
dataset=gs://weatherbench2/datasets/era5/1959-2023_01_10-wb13-6h-1440x721.zarr/
start_year=2020
end_year=2022

output="/projects/prjs0981/ewalt/zarr-snellius-benchmark/data/chunking_exp/era5_6h_aurora_$start_year-$end_year-wb13-6h-1440x721"

# Download dataset 
# already avail at /projects/prjs0981/ewalt/zarr-snellius-benchmark/data/era5_6h_aurora_2020-2022-wb13-6h-1440x721.zarr-v2/
# ./slurm/download.sh \
#     2 \
#     --dataset $dataset \
#     --output $output.ds.zarr \
#     --date_start $start_year-01-01 \
#     --date_end $end_year-12-31 \
#     --chunks_write_strategy optimal_dataset

# Download dataarray
./slurm/download.sh \
    2 \
    --dataset $dataset \
    --output $output.da.zarr \
    --date_start $start_year-01-01 \
    --date_end $end_year-12-31 \
    --chunks_write_strategy optimal_dataarray

