import dataclasses

def _parse_chunks_write_strategy(choice):
    """Parse the chunks write strategy argument."""
    if choice == "optimal_dataset":
        return {"time": 1, "latitude": -1, "longitude": -1, "level": -1}
    elif choice == "optimal_dataarray":
        return {"time": 1, "latitude": -1, "longitude": -1, "level": -1, "variable": -1}
    else:
        raise ValueError(f"Invalid chunks write strategy: {choice}")

@dataclasses.dataclass
class ArgumentItem(object):
    """Class to represent an argument item."""
    flag: str
    kwargs: dict

@dataclasses.dataclass
class Arguments(object):
    DATASET = ArgumentItem(**{
        "flag": "--dataset",
        "kwargs": {
            "type": str,
            "required": True,
            "help": "Dataset to use for the benchmark (can be local or remote)"
        }
    })
    OUTPUT = ArgumentItem(**{
        "flag": "--output",
        "kwargs": {
            "type": str,
            "required": True,
            "help": "Output path for the dataset"
        }
    })
    EXPERIMENT_DIR = ArgumentItem(**{
        "flag": "--experiment_dir",
        "kwargs": {
            "type": str,
            "default": "./experiments",
            "help": "Directory to store the experiment results"
        }
    })
    CHUNKS_OPEN_STRATEGY = ArgumentItem(**{
        "flag": "--chunks_open_strategy",
        "kwargs": {
            "type": str,
            "default": "auto",
            "help": "Chunking strategy for opening the xarray dataset (default: auto)"
        }
    })
    CHUNKS_WRITE_STRATEGY = ArgumentItem(**{
        "flag": "--chunks_write_strategy",
        "kwargs": {
            "type": _parse_chunks_write_strategy,         
            "default": "optimal_dataset",
            "help": "Chunking strategy for writing the xarray dataset. Must be one of ['optimal_dataset', 'optimal_dataarray'] (default: optimal_dataset)"
        },
    })
    COMPRESS_VARS = ArgumentItem(**{
        "flag": "--compress_vars",
        "kwargs": {
            "action": "store_true",
            "default": False,
            "help": "Compress variables in the dataset"
        }
    })
    COMPRESS_COORDS = ArgumentItem(**{
        "flag": "--compress_coords",
        "kwargs": {
            "action": "store_true",
            "default": False,
            "help": "Compress coordinates in the dataset"
        }
    })
    VARIABLES = ArgumentItem(**{
        "flag": "--variables",
        "kwargs": {
            "type": str,
            "nargs": "+",
            "default": [
                "2m_temperature", "mean_sea_level_pressure", "10m_u_component_of_wind", "10m_v_component_of_wind",
                "temperature", "specific_humidity", "u_component_of_wind", "v_component_of_wind", "geopotential",
            ],
            "help": "List of variables to include in the dataset"
        }
    })
    LEVELS = ArgumentItem(**{
        "flag": "--levels",
        "kwargs": {
            "type": int,
            "nargs": "+",
            "default": [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000],
            "help": "List of levels to include in the dataset"
        }
    })
    DATE_START_DOWNLOAD = ArgumentItem(**{
        "flag": "--date_start",
        "kwargs": {
            "type": str,
            "default": None,
            "help": "Start date for the dataset (YYYY-MM-DD)"
        }
    })
    DATE_END_DOWNLOAD = ArgumentItem(**{
        "flag": "--date_end",
        "kwargs": {
            "type": str,
            "default": None,
            "help": "End date for the dataset (YYYY-MM-DD)"
        }
    })
    DATE_START_BENCHMARK = ArgumentItem(**{
        "flag": "--date_start",
        "kwargs": {
            "type": str,
            "default": "2020-01-01",
            "help": "Start date for the dataset (YYYY-MM-DD)"
        }
    })
    DATE_END_BENCHMARK = ArgumentItem(**{
        "flag": "--date_end",
        "kwargs": {
            "type": str,
            "default": "2022-12-31",
            "help": "End date for the dataset (YYYY-MM-DD)"
        }
    })
    NUM_INPUT_TIMESTEPS = ArgumentItem(**{
        "flag": "--num_input_timesteps",
        "kwargs": {
            "type": int,
            "default": 2,
            "help": "Number of input timesteps to include in a sample"
        }
    })
    NUM_OUTPUT_TIMESTEPS = ArgumentItem(**{
        "flag": "--num_output_timesteps",
        "kwargs": {
            "type": int,
            "default": 1,
            "help": "Number of output timesteps to include in a sample"
        }
    })
    BATCH_SIZE = ArgumentItem(**{
        "flag": "--batch_size",
        "kwargs": {
            "type": int,
            "default": 1,
            "help": "Batch size for the dataloader"
        }
    })
    PT_WORKERS = ArgumentItem(**{
        "flag": "--pt_workers",
        "kwargs": {
            "type": int,
            "default": 0,
            "help": "Number of workers for the dataloader"
        }
    })
    DATALOADER_KWARGS = ArgumentItem(**{
        "flag": "--dataloader_kwargs",
        "kwargs": {
            "type": str,
            "default": '{"pin_memory": False, "shuffle": True, "persistent_workers": False}',
            "help": "Dataloader kwargs for the dataloader"
        }
    })
    DASK_SCHEDULER = ArgumentItem(**{
        "flag": "--dask_scheduler",
        "kwargs": {
            "type": str,
            "default": None,
            "choices": [None, "threads", "processes", "single-threaded", "synchronous"],
            "help": "Dask scheduler to use"
        }
    })
    DASK_WORKERS = ArgumentItem(**{
        "flag": "--dask_workers",
        "kwargs": {
            "type": int,
            "default": 1,
            "help": "Number of workers to use for dask"
        }
    })
    DASK_THREADS_PER_WORKER = ArgumentItem(**{
        "flag": "--dask_threads_per_worker",
        "kwargs": {
            "type": int,
            "default": 1,
            "help": "Number of threads per worker to use for dask"
        }
    })
    LOG_FREQUENCY = ArgumentItem(**{
        "flag": "--log_frequency",
        "kwargs": {
            "type": int,
            "default": 10,
            "help": "Frequency of logging info"
        }
    })
    NUM_SAMPLES = ArgumentItem(**{
        "flag": "--num_samples",
        "kwargs": {
            "type": int,
            "default": 100,
            "help": "Number of samples to benchmark"
        }
    }) 
    
    
    