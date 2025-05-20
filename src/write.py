import xarray as xr
import os, shutil
import zarr
import logging

# see https://github.com/pydata/xarray/issues/10032
zarr_version = zarr.__version__
zarr_format = int(zarr_version[0])
if zarr_format == 2:
    from numcodecs import Blosc as BloscCompressor
elif zarr_format == 3:
    from zarr.codecs import BloscCodec as BloscCompressor
else:
    raise ValueError(f"Unknown Zarr version: {zarr_version}")

logger = logging.getLogger()

def get_compressor():
    if zarr_format == 2:
        compressor_key = "compressor"
        compressor = BloscCompressor(cname="zstd", clevel=3, shuffle=2)
    elif zarr_format == 3:
        compressor_key = "compressors"
        compressor = BloscCompressor(cname="zstd", clevel=3, shuffle="bitshuffle")
    else:
        raise ValueError(f"Unknown Zarr version: {zarr_version}")
    return compressor_key, compressor

def write_zarr(
    ds: xr.Dataset|xr.DataArray, 
    path: str, 
    exist_ok: bool=False, 
    new_chunks: dict=None, 
    compress_vars: bool=False,
    compress_coords: bool=False
):
    """
    Write a dataset to a zarr file.
    
    Args:
        ds: xarray.Dataset
            The dataset to write.
        path: str
            The path to write the dataset to.
        exist_ok: bool
            Whether to overwrite the file if it already exists
    """
    path = os.path.abspath(path)
    
    if os.path.exists(path):
        if exist_ok:
            shutil.rmtree(path)
        else:
            raise FileExistsError(f"File already exists: {path}")
        
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    
    # get default compressor
    if compress_vars or compress_coords:
        compressor_key, compressor = get_compressor()
        
    # convert to dataarray if new_chunks has 1 more dimension
    if len(new_chunks) == len(ds.sizes)+1:
        logger.info(f"Converting dataset to dataarray with new dimension {list(new_chunks.keys())[0]}")
        dims = list(ds.sizes.keys())
        ds = ds.to_array(dim="variable", name="variable").transpose(*dims, "variable")
        # rename the chunk to 'variable'
        for d in new_chunks:
            if not d in dims:
                value = new_chunks.pop(d)
                new_chunks["variable"] = value
        
    # reformat chunks if needed
    if new_chunks is not None:
        for d, chunk in new_chunks.items():
            if chunk == -1:
                new_chunks[d] = ds.sizes[d]
        logger.info(f"Rechunking to: {new_chunks}")
        ds = ds.chunk(new_chunks)

    # reset encoding
    encoding = {}
    
    # set encoding for each variable
    if isinstance(ds, xr.Dataset):
        for var in ds.data_vars:
            var_encoding = {}
            if new_chunks is not None:
                var_encoding["chunks"] = tuple(new_chunks[dim] for dim in ds[var].sizes)
            if compress_vars:
                var_encoding[compressor_key] = compressor
            encoding[var] = var_encoding
        
    # set encoding for each coordinate
    for coord in ds.coords:
        coord_encoding = {}
        if compress_coords:
            coord_encoding[compressor_key] = compressor
        encoding[coord] = coord_encoding
        
    # set encoding for each attribute
    logger.info(f"Writing dataset to {path}")
    ds.to_zarr(
        path, 
        mode="w", 
        encoding=encoding,
        zarr_format=zarr_format,
    )