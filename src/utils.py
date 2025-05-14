import xarray as xr
import os, shutil
import zarr
import logging

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

def write_zarr(ds: xr.Dataset, path: str, exist_ok: bool=False, new_chunks: dict=None):
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
        
    compressor_key, compressor = get_compressor()
    encoding = {}
    
    if new_chunks is not None:
        new_chunks = {var: chunk if chunk != -1 else ds.sizes[var] for var, chunk in new_chunks.items()}
        
        logger.info(f"Rechunking dataset to {new_chunks}")
        ds = ds.chunk(new_chunks)
        
        for var in ds.data_vars:
            encoding[var] = {
                "chunks": tuple(new_chunks[dim] for dim in ds[var].sizes),
                compressor_key: compressor
            }
        for coord in ds.coords:
            encoding[coord] = {
                compressor_key: compressor
            }
    else:
        for var in ds.data_vars:
            encoding[var] = {
                compressor_key: compressor
            }
        for coord in ds.coords:
            encoding[coord] = {
                compressor_key: compressor
            }
        
    logger.info(f"Writing dataset to {path}")
    ds.to_zarr(
        path, 
        mode="w", 
        encoding=encoding,
        zarr_format=zarr_format,
    )