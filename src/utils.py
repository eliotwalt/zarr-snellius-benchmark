import xarray as xr
import logging

logger = logging.getLogger(__name__)

def load_dataset(args, force_zarr_format):
    if force_zarr_format: ds = xr.open_zarr(args.dataset, chunks=args.chunks_open_strategy, zarr_format=args.zarr_format)
    else: ds = xr.open_zarr(args.dataset, chunks=args.chunks_open_strategy)
        
    # deal with the case where ds is actally a dataarray with a 'variable' dimension
    if 'variable' in ds.coords:
        logger.info(f"{args.dataset} is a dataarray. Reading as such...")
        if force_zarr_format: da = xr.open_dataarray(args.dataset, engine='zarr', chunks=args.chunks_open_strategy, zarr_format=args.zarr_format)
        else: da = xr.open_dataarray(args.dataset, engine='zarr', chunks=args.chunks_open_strategy)
        ds = da.to_dataset(dim='variable')
        
    ds = ds.sel(time=slice(args.date_range[0], args.date_range[1]))
    ds = ds[args.variables].sel(level=args.levels)
    logger.info(f"Loaded dataset with sizes: {ds.sizes}")
    
    return ds