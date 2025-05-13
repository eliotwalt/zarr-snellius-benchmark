
import xarray as xr
import argparse
import time
import logging
import matplotlib.pyplot as plt
import dask
import yaml
import os
import numpy as np
from datetime import datetime
import zarr

from dataset import BaseDataset
from torch.utils.data import DataLoader

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
    
    # xarray dataset
    parser.add_argument(
        "--dataset", 
        type=str, 
        required=True, # "gs://weatherbench2/datasets/era5/1959-2023_01_10-wb13-6h-1440x721.zarr/" 
        help="Path to the xarray dataset (local or remote)"
    )
    parser.add_argument(
        "--chunks",
        type=str,
        default="auto",
        help="Chunking strategy for the xarray dataset (default: auto)"
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
        type=tuple[str, str],
        default=("2020-01-01", "2022-12-31"),
        help="Date range to include in the dataset (start, end)"
    )
    parser.add_argument(
        "--num_input_timesteps", 
        type=int, 
        default=2, 
        help="Number of input timesteps to include in a sample"
    )
    parser.add_argument(
        "--num_output_timesteps", 
        type=int, 
        default=1, 
        help="Number of output timesteps to include in a sample"
    )
    
    # dataloader kwargs
    parser.add_argument(
        "--batch_size", 
        type=int, 
        default=5, 
        help="Batch size for the dataloader"
    )
    parser.add_argument(
        "--pt_workers", 
        type=int, 
        default=0, 
        help="Number of workers for the dataloader"
    )
    parser.add_argument(
        "--pin_memory", 
        action="store_true", 
        help="Pin memory for the dataloader"
    )
    parser.add_argument(
        "--shuffle", 
        action="store_true", 
        help="Shuffle the dataset"
    )
    parser.add_argument(
        "--persistent_workers",
        action="store_true",
        help="Use persistent workers for the dataloader"
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
    
    # benchmark arguments
    parser.add_argument(
        "--num_samples",
        type=int,
        default=20,
        help="Number of samples to benchmark"
    )
    parser.add_argument(
        "--log_frequency",
        type=int,
        default=10,
        help="Frequency of logging info"
    )
    
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
    
    return args

def main():
    
    args = get_args()
    experiment_dir = os.path.join(
        "experiments", 
        datetime.now().strftime("%Y%m%dT%H%M%S")
    )
    os.makedirs(experiment_dir, exist_ok=True)
    logger.info(f"Experiment directory: {experiment_dir}")
    with open(os.path.join(experiment_dir, "args.yaml"), "w") as f:
        yaml.dump(vars(args), f)
    
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
    try:
        ds = xr.open_zarr(args.dataset, chunks=args.chunks, zarr_format=args.zarr_format)
    except Exception as e:
        logger.error(f"Failed to load dataset with zarr format {args.zarr_format}: {e}")
        ds = xr.open_zarr(args.dataset, chunks=args.chunks)
    ds = ds.sel(time=slice(args.date_range[0], args.date_range[1]))
    ds = ds[args.variables].sel(level=args.levels)
    logger.info(f"Loaded dataset with shape: {ds.dims}")
    
    # create dataset
    dataset = BaseDataset(
        ds=ds,
        num_input_timesteps=args.num_input_timesteps,
        num_output_timesteps=args.num_output_timesteps,
        variables=args.variables,
        levels=args.levels,
    )
    logger.info(f"Created dataset with {len(dataset)} samples")
    
    # create dataloader
    if args.dataset.startswith("gs://") and args.pt_workers > 0: dl_kwargs = {"multiprocessing_context": "forkserver"}
    else: dl_kwargs = {}
    dataloader = DataLoader(
        dataset,
        batch_size=args.batch_size,
        num_workers=args.pt_workers,
        pin_memory=args.pin_memory,
        shuffle=args.shuffle,
        persistent_workers=args.persistent_workers,
        **dl_kwargs,
    )
    logger.info(f"Created dataloader with {len(dataloader)} batches")
    
    # benchmark dataloader
    times = []
    t0 = time.time()
    for i, (x, y) in enumerate(dataloader):
        times.append(time.time() - t0)
        t0 = time.time()
        
        if i==0 or i % args.log_frequency == 0:
            logger.info(f"Sample {i+1}/{args.num_samples} - x:{x.shape}, y:{y.shape} - Time: {times[-1]:.4f}s")
            
        if i == args.num_samples:
            break
        
    mean, std = np.mean(times), np.std(times)
    logger.info(f"Mean time: {mean:.4f}s, Std: {std:.4f}s")
    times_summary = {
        "times": times,
        "mean": float(mean),
        "std": float(std),
    }
    with open(os.path.join(experiment_dir, "times_summary.yaml"), "w") as f:
        yaml.dump(times_summary, f)
    
    # hsitogram of times
    plt.hist(times, bins=50)
    plt.xlabel("Time (s)")
    plt.ylabel("Frequency")
    plt.title(f"batch loading time distribution (mean: {mean:.4f}s, std: {std:.4f}s)")
    plt.show()
    plt.savefig(os.path.join(experiment_dir, "times_hist.png"))
    
    plt.close()
    
if __name__ == "__main__":
    main()