import xarray as xr
import torch
import logging

from torch.utils.data import Dataset

logger = logging.getLogger(__name__)

def dataarray_to_tensor(arr: xr.DataArray) -> torch.Tensor:
    """
    Convert an xarray dataset to a PyTorch tensor.
    """
    tensor = torch.from_numpy(arr.values) # T, (C), H, W     
    if not "level" in arr.dims: tensor = tensor.unsqueeze(1) # T, 1, H, W
    
    logger.debug(f"{arr.name} -> {tensor.shape}")
    
    return tensor

def sample_to_tensor(sample: xr.Dataset) -> torch.Tensor:
    """
    Convert an xarray dataset to a PyTorch tensor.
    """
    tensors = []
    for var in sample.data_vars:
        tensor = dataarray_to_tensor(sample[var])
        tensors.append(tensor)
    
    # Concatenate along the channel dimension
    tensor = torch.cat(tensors, dim=1) # T, C, H, W
    logger.debug(f"sample_to_tensor: {tensor.shape}")    

    return tensor

class BaseDataset(Dataset):
    def __init__(
        self, 
        ds: xr.Dataset, 
        num_input_timesteps=2,
        num_output_timesteps=1,
        variables: list[str]=[],
        levels: list[int]=[],
    ):
        assert isinstance(ds, xr.Dataset), "ds must be an xarray Dataset"
        assert isinstance(num_input_timesteps, int), "num_input_timesteps must be an integer"
        assert isinstance(num_output_timesteps, int), "num_output_timesteps must be an integer"
        assert num_input_timesteps > 0, "num_input_timesteps must be greater than 0"
        assert num_output_timesteps > 0, "num_output_timesteps must be greater than 0"
        assert isinstance(variables, list), "variables must be a list"
        assert isinstance(levels, list), "levels must be a list"
        assert all(var in ds.data_vars for var in variables), "all variables must be in the dataset"
        assert all(level in ds.level for level in levels), "all levels must be in the dataset"

        self.ds = ds[variables].sel(level=levels)
        self.num_input_timesteps = num_input_timesteps
        self.num_output_timesteps = num_output_timesteps
        self.variables = variables
        self.levels = levels
        
        self.sample_timesteps = num_input_timesteps + num_output_timesteps
        self.total_timesteps = len(self.ds['time'])

    def __len__(self):
        return self.total_timesteps - self.sample_timesteps

    def __getitem__(self, i):
        sample = self.ds.isel(time=slice(i, i+self.sample_timesteps))
        sample = sample_to_tensor(sample)
        
        x = sample[:self.num_input_timesteps]
        y = sample[self.num_input_timesteps:]
        
        return x, y