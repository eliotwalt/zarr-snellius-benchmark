#!/bin/bash

# get pyooptions from the command line
zarr_version=$1
shift
pyoptions="$@"
if [ -z "$zarr_version" ]; then
    echo "Usage: $0 <zarr_version> [<python_options>]"
    exit 1
fi
echo "Zarr version: $zarr_version"
echo "Python options: $pyoptions"

sbatch --job-name=download \
       --output=./logs/download/%J.out \
       --error=./logs/download/%J.out \
       --time=02:30:00 \
       --partition=rome \
       --wrap="source env/venv_zarr-v${zarr_version}/bin/activate && python ./src/download.py $pyoptions"