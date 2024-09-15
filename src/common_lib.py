import pandas 
import pyarrow.parquet as pq
import requests
import io
import os
from pathlib import Path
import logging

root_marker = 'README.md'
current_dir = Path.cwd()
while not (current_dir / root_marker).exists() and current_dir != current_dir.parent:
    current_dir = current_dir.parent

os.makedirs(f"{current_dir}/logs/", exist_ok=True)

logging.basicConfig(
    filename=f"{current_dir}/logs/extraction_{pandas.Timestamp.now()}.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)