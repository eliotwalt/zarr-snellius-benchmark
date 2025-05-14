import xarray as xr

def load_dataset(args, force_zarr_format):
    if force_zarr_format:
        ds = xr.open_zarr(args.dataset, chunks=args.chunks_open_strategy, zarr_format=args.zarr_format)
    else:
        ds = xr.open_zarr(args.dataset, chunks=args.chunks_open_strategy)
    ds = ds.sel(time=slice(args.date_range[0], args.date_range[1]))
    ds = ds[args.variables].sel(level=args.levels)
    return ds