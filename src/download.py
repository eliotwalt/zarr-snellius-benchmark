
import xarray as xr
import argparse
import logging
import matplotlib.pyplot as plt
import os
import dask
import zarr

from dataset import BaseDataset
from write import write_zarr
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
    parser.add_argument(Arguments.OUTPUT.flag, **Arguments.OUTPUT.kwargs)
    parser.add_argument(Arguments.CHUNKS_OPEN_STRATEGY.flag, **Arguments.CHUNKS_OPEN_STRATEGY.kwargs)
    parser.add_argument(Arguments.CHUNKS_WRITE_STRATEGY.flag, **Arguments.CHUNKS_WRITE_STRATEGY.kwargs)
    parser.add_argument(Arguments.COMPRESS_VARS.flag, **Arguments.COMPRESS_VARS.kwargs)
    parser.add_argument(Arguments.COMPRESS_COORDS.flag, **Arguments.COMPRESS_COORDS.kwargs)
    parser.add_argument(Arguments.VARIABLES.flag, **Arguments.VARIABLES.kwargs)
    parser.add_argument(Arguments.LEVELS.flag, **Arguments.LEVELS.kwargs)
    parser.add_argument(Arguments.DATE_START_DOWNLOAD.flag, **Arguments.DATE_START_DOWNLOAD.kwargs)
    parser.add_argument(Arguments.DATE_END_DOWNLOAD.flag, **Arguments.DATE_END_DOWNLOAD.kwargs)
    parser.add_argument(Arguments.DASK_SCHEDULER.flag, **Arguments.DASK_SCHEDULER.kwargs)
    parser.add_argument(Arguments.DASK_WORKERS.flag, **Arguments.DASK_WORKERS.kwargs)
    parser.add_argument(Arguments.DASK_THREADS_PER_WORKER.flag, **Arguments.DASK_THREADS_PER_WORKER.kwargs)

    # parse arguments
    args = parser.parse_args()
    
    try: args.chunks_open_strategy = eval(args.chunks_open_strategy)
    except Exception as e: pass
    
    try: args.chunks_write_strategy = eval(args.chunks_write_strategy)
    except Exception as e: pass
    logger.info(f"Parsed chunks write strategy: {args.chunks_write_strategy}")
    
    args.date_range = [args.date_start, args.date_end]
    if args.date_range[0] is None or args.date_range[1] is None:
        logger.warning("Date range not specified, this will download the entire dataset")
    
    args.zarr_version = zarr.__version__
    if args.zarr_version.startswith("2."):
        logger.info("Zarr version 2.x detected")
        args.zarr_format = 2
    elif args.zarr_version.startswith("3."):
        logger.info("Zarr version 3.x detected")
        args.zarr_format = 3
    else:
        raise ValueError(f"Unknown Zarr version: {args.varr_version}")
        
    if args.output.endswith("/"):
        args.output = args.output[:-1]
    args.output = f"{args.output}-v{args.zarr_format}"
    
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
    logger.info(f"Loading dataset from {args.dataset} for dates {args.date_range[0]} to {args.date_range[1]}")
    ds = load_dataset(args, force_zarr_format=False)
    logger.info(f"Loaded dataset with shape: {ds.dims}")
    
    # write to zarr
    write_zarr(
        ds, 
        path=args.output,
        exist_ok=True,
        new_chunks=args.chunks_write_strategy,
        compress_vars=args.compress_vars,
        compress_coords=args.compress_coords,
    )
    
if __name__ == "__main__":
    main()
    logger.info("Finished downloading dataset")
