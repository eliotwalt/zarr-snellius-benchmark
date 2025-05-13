python3.11 -m venv env/venv_zarr-v3

source env/venv_zarr-v3/bin/activate
pip install "xarray[complete]" torch torchaudio torchvision gcsfs PyYAML zarr~=3.0.0