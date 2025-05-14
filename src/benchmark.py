
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
from codename import codename
from torch.utils.data import DataLoader

from dataset import BaseDataset
from args import Arguments
from utils import load_dataset

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
    
    parser.add_argument(Arguments.DATASET.flag, **Arguments.DATASET.kwargs)
    parser.add_argument(Arguments.EXPERIMENT_DIR.flag, **Arguments.EXPERIMENT_DIR.kwargs)
    parser.add_argument(Arguments.CHUNKS_OPEN_STRATEGY.flag, **Arguments.CHUNKS_OPEN_STRATEGY.kwargs)
    parser.add_argument(Arguments.VARIABLES.flag, **Arguments.VARIABLES.kwargs)
    parser.add_argument(Arguments.LEVELS.flag, **Arguments.LEVELS.kwargs)
    parser.add_argument(Arguments.DATE_START_BENCHMARK.flag, **Arguments.DATE_START_BENCHMARK.kwargs)
    parser.add_argument(Arguments.DATE_END_BENCHMARK.flag, **Arguments.DATE_END_BENCHMARK.kwargs)
    parser.add_argument(Arguments.NUM_INPUT_TIMESTEPS.flag, **Arguments.NUM_INPUT_TIMESTEPS.kwargs)
    parser.add_argument(Arguments.NUM_OUTPUT_TIMESTEPS.flag, **Arguments.NUM_OUTPUT_TIMESTEPS.kwargs)
    parser.add_argument(Arguments.BATCH_SIZE.flag, **Arguments.BATCH_SIZE.kwargs)
    parser.add_argument(Arguments.PT_WORKERS.flag, **Arguments.PT_WORKERS.kwargs)
    parser.add_argument(Arguments.DATALOADER_KWARGS.flag, **Arguments.DATALOADER_KWARGS.kwargs)
    parser.add_argument(Arguments.DASK_SCHEDULER.flag, **Arguments.DASK_SCHEDULER.kwargs)
    parser.add_argument(Arguments.DASK_WORKERS.flag, **Arguments.DASK_WORKERS.kwargs)
    parser.add_argument(Arguments.DASK_THREADS_PER_WORKER.flag, **Arguments.DASK_THREADS_PER_WORKER.kwargs)
    parser.add_argument(Arguments.LOG_FREQUENCY.flag, **Arguments.LOG_FREQUENCY.kwargs)
    parser.add_argument(Arguments.NUM_SAMPLES.flag, **Arguments.NUM_SAMPLES.kwargs)
    
    # parse arguments
    args = parser.parse_args()
    
    args.experiment_dir = os.path.join(
        args.experiment_dir, 
        codename(separator="_") + "-" + datetime.now().strftime("%Y%m%dT%H%M%S")
    )
    os.makedirs(args.experiment_dir, exist_ok=True)
    
    try: args.chunks_open_strategy = eval(args.chunks_open_strategy)
    except Exception as e: pass
    
    args.date_range = [args.date_start, args.date_end]
    
    args.dataloader_kwargs = eval(args.dataloader_kwargs)
    if args.dataset.startswith("gs://") and args.pt_workers > 0:
        args.dataloader_kwargs["multiprocess_context"] = "forkserver"
    
    args.zarr_version = zarr.__version__
    if args.zarr_version.startswith("2."):
        logger.info("Zarr version 2.x detected")
        args.zarr_format = 2
    elif args.zarr_version.startswith("3."):
        logger.info("Zarr version 3.x detected")
        args.zarr_format = 3
    else:
        raise ValueError(f"Unknown Zarr version: {args.varr_version}")
    
    return args

def main():
    
    args = get_args()
    logger.info(f"Experiment directory: {args.experiment_dir}")
    with open(os.path.join(args.experiment_dir, "args.yaml"), "w") as f:
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
    logger.info(f"Loading dataset from {args.dataset} for dates {args.date_range[0]} to {args.date_range[1]}")
    ds = load_dataset(args)
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
    dataloader = DataLoader(
        dataset,
        batch_size=args.batch_size,
        num_workers=args.pt_workers,
        **args.dataloader_kwargs,
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
    with open(os.path.join(args.experiment_dir, "times_summary.yaml"), "w") as f:
        yaml.dump(times_summary, f)
    
    # hsitogram of times
    plt.hist(times, bins=50)
    plt.xlabel("Time (s)")
    plt.ylabel("Frequency")
    plt.title(f"batch loading time distribution (mean: {mean:.4f}s, std: {std:.4f}s)")
    plt.show()
    plt.savefig(os.path.join(args.experiment_dir, "times_hist.png"))
    
    plt.close()
    
if __name__ == "__main__":
    main()