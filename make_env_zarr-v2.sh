python3.11 -m venv env/venv_zarr-v2

source env/venv_zarr-v2/bin/activate
pip install "xarray[complete]" torch torchaudio torchvision gcsfs PyYAML