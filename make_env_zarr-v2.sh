python3.11 -m venv env/venv_zarr-v2

source env/venv_zarr-v2/bin/activate
which python

pip install "xarray[complete]" torch torchaudio torchvision gcsfs PyYAML ipykernel "zarr<3"
python -c "import torch, gcsfs, xarray, zarr ; print('zarr version:', zarr.__version__)"
