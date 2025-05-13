import xarray as xr
import os, shutil
import zarr
import numcodecs
import logging

logger = logging.getLogger()

def get_compressor():
    compressor = numcodecs.Blosc(cname="zstd", clevel=3, shuffle=2)
    return compressor

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
        
    compressor = get_compressor() # TODO: does not work when in encoding
    
    if new_chunks is not None:
        new_chunks = {var: chunk if chunk is not -1 else ds.sizes[var] for var, chunk in new_chunks.items()}
        logger.info(f"Rechunking dataset to {new_chunks}")
        ds = ds.chunk(new_chunks)
        encoding = {
            var: {
                "chunks": tuple(new_chunks[dim] for dim in ds[var].sizes),
                # "compressor": compressor
            } for var in ds.data_vars
        }
    # else:
        # encoding = {
        #     var: {
        #         "compressor": compressor
        #     } for var in ds.data_vars
        # }
        
    logger.info(f"Writing dataset to {path}")
    ds.to_zarr(
        path, 
        mode="w", 
        encoding=encoding
    )