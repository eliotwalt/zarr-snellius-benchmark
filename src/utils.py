import xarray as xr

def load_dataset(args):
    ds = xr.open_zarr(args.dataset, chunks=args.chunks_open_strategy, zarr_format=args.zarr_format)
    ds = ds.sel(time=slice(args.date_range[0], args.date_range[1]))
    ds = ds[args.variables].sel(level=args.levels)
    return ds