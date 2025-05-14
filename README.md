# Benchmarking zarr on Snellius

## Setup 

```bash
# Zarr V2
./make_env_zarr-v2.sh
source env/venv_zarr-v2/bin/activate

# Zarr V3
./make_env_zarr-v3.sh
source env/venv_zarr-v3/bin/activate
```

## Download data
- python script: `src/download.py <OPTIONS>` -> many `argparse` option available for experiments. 
- slurm entrypoint: `slurm/download.sh <ZARR_VERSION> <OPTIONS>`, with `ZARR_VERSION` either `2` or `3`.
- example: `./download_era5_weekly_aurora_240x121_year_range.sh <ZARR_VERSION> <START_YEAR> <END_YEAR>` -> downloads low resolution weekly era5 with aurora variables for specified years (including).

**WARNING**: Quite basic. No rechunking. No fancy library. Zarr v3 does not work yet. 

## Benchmark 
- python script: `src/benchmark.py <OPTIONS>` -> many `argparse` option available for experiments. 
- slurm entrypoint: `slurm/benchmark.sh <ZARR_VERSION> <OPTIONS>`, with `ZARR_VERSION` either `2` or `3`.
- example: `benchmark.sh <ZARR_VERSION> <DATASET>` -> benchmarks a given dataset with default dask setup.

This write some logs in `experiments/`. Check out the notebooks `analysis_240x121-v2-num_years.ipynb` and `analysis_240x121-v2-v3.ipynb`.
