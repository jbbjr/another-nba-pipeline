"""Extract: Read parquet files into DataFrames."""

import pandas as pd
from pathlib import Path
from config import PARQUET_FILES


def read_parquet_source(path):
    """Read parquet from file or directory."""
    p = Path(path)
    
    if p.is_file():
        # Single parquet file
        return pd.read_parquet(p)
    elif p.is_dir():
        # Directory of parquet files
        parquet_files = sorted(p.glob('*.parquet'))
        if not parquet_files:
            raise ValueError(f"No parquet files found in {path}")
        
        print(f"  Found {len(parquet_files)} files in {path}")
        dfs = [pd.read_parquet(f) for f in parquet_files]
        return pd.concat(dfs, ignore_index=True)
    else:
        raise FileNotFoundError(f"Path not found: {path}")


def read_all():
    """Read all parquet files/directories into memory."""
    data = {}
    
    for name, path in PARQUET_FILES.items():
        print(f"  Reading {name} from {path}...")
        data[name] = read_parquet_source(path)
        print(f"    ✓ Loaded {len(data[name]):,} rows")
    
    return data