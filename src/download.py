
import xarray as xr
import argparse
import logging
import matplotlib.pyplot as plt
import os
import dask
from torch.utils.data import DataLoader
import zarr

from dataset import BaseDataset
from utils import write_zarr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Suppress logs from Google libraries
logging.getLogger('google').setLevel(logging.ERROR)
logging.getLogger('google.auth').setLevel(logging.ERROR)
logging.getLogger('google.cloud').setLevel(logging.ERROR)

def get_args():
    logger.info("Parsing arguments")
    parser = argparse.ArgumentParser()
    
    # xarray dataset open
    parser.add_argument(
        "--dataset", 
        type=str, 
        required=True, # "gs://weatherbench2/datasets/era5/1959-2023_01_10-wb13-6h-1440x721.zarr/" 
        help="Path to the xarray dataset (local or remote)"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output path for the dataset"
    )
    parser.add_argument(
        "--chunks",
        type=str,
        default="auto",
        help="Chunking strategy for opening the xarray dataset (default: auto)"
    )
    parser.add_argument(
        "--variables", 
        type=str, 
        nargs="+", 
        # default=[
        #     "2t", "msl", "10u", "10v",
        #     "t", "q", "u", "v", "z",
        # ], 
        default=[
            "2m_temperature", "mean_sea_level_pressure", "10m_u_component_of_wind", "10m_v_component_of_wind",
            "temperature", "specific_humidity", "u_component_of_wind", "v_component_of_wind", "geopotential",
        ],
        help="List of variables to include in the dataset"
    )
    parser.add_argument(
        "--levels", 
        type=int, 
        nargs="+", 
        default=[50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000], 
        help="List of levels to include in the dataset"
    )
    parser.add_argument(
        "--date_range",
        type=str,
        nargs=2,
        default=["2020-01-01", "2022-12-31"],
        help="Date range to include in the dataset (start, end)"
    )
    
    # dask arguments
    parser.add_argument(
        "--dask_scheduler",
        type=str,
        default=None,
        choices=[None, "threads", "processes", "single-threaded", "synchronous"],
        help="Dask scheduler to use"
    )
    parser.add_argument(
        "--dask_workers",
        type=int,
        default=1,
        help="Number of workers to use for dask"
    )
    parser.add_argument(
        "--dask_threads_per_worker",
        type=int,
        default=1,
        help="Number of threads per worker to use for dask"
    )

    # parse arguments
    args = parser.parse_args()
    assert len(args.date_range) == 2, "Date range must be a tuple of (start, end)"
    
    # parse arguments
    args = parser.parse_args()
    args.date_range = list(args.date_range)
    args.zarr_version = zarr.__version__
    
    varr_version = zarr.__version__
    if varr_version.startswith("2."):
        logger.info("Zarr version 2.x detected")
        zarr_format = 2
    elif varr_version.startswith("3."):
        logger.info("Zarr version 3.x detected")
        zarr_format = 3
    else:
        raise ValueError(f"Unknown Zarr version: {varr_version}")
    args.zarr_format = zarr_format
        
    if args.output.endswith("/"):
        args.output = args.output[:-1]
    args.output = f"{args.output}-v{zarr_format}"
    
    return args

def main():
    args = get_args()
    
    # dask setup
    if args.dask_scheduler is not None:
        dask.config.set(
            scheduler=args.dask_scheduler, 
            num_workers=args.dask_workers, 
            threads_per_worker=args.dask_threads_per_worker
        )
        logger.info(f"Configured dask (scheduler: {args.dask_scheduler}, workers: {args.dask_workers}, threads per worker: {args.dask_threads_per_worker})")
    
    # load dataset
    logger.info(f"Loading dataset from {args.dataset}")
    ds = xr.open_zarr(args.dataset, chunks=args.chunks)
    ds = ds.sel(time=slice(args.date_range[0], args.date_range[1]))
    ds = ds[args.variables].sel(level=args.levels)
    logger.info(f"Loaded dataset with shape: {ds.dims}")
    
    # write to zarr
    write_zarr(
        ds, 
        path=args.output,
        exist_ok=True,
        new_chunks={"time": 1, "latitude": -1, "longitude": -1, "level": -1}
    )
    
if __name__ == "__main__":
    main()
    logger.info("Finished downloading dataset")