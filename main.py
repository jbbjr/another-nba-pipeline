"""Main ETL orchestrator for NBA data."""

from extract import read_all
from transform import transform_all
from load import load_all


def main():
    """Run the complete ETL pipeline."""
    print("=" * 50)
    print("NBA ETL Pipeline")
    print("=" * 50)
    
    # Extract
    print("\n[1/3] Extracting data from parquet files...")
    raw_data = read_all()
    print(f"  ✓ Loaded {len(raw_data)} parquet files")
    
    # Transform
    print("\n[2/3] Transforming data...")
    tables = transform_all(raw_data)
    print(f"  ✓ Generated {len(tables)} tables")
    
    # Load
    print("\n[3/3] Loading to SQLite...")
    load_all(tables)
    
    print("\n" + "=" * 50)
    print("ETL Complete!")
    print("=" * 50)


if __name__ == "__main__":
    main()