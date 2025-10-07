"""Main ETL orchestrator for NBA data."""

from extract import read_all
from transform import transform_all
from load import load_all
from config import LOAD_MODE, RUN_VALIDATION


def main():
    """Run the complete ETL pipeline."""
    print("=" * 50)
    print("NBA ETL Pipeline")
    print(f"Mode: {LOAD_MODE}")
    print("=" * 50)
    
    # Extract
    print("\n[1/3] Extracting data from parquet files...")
    raw_data = read_all()
    
    # Transform
    print("\n[2/3] Transforming data...")
    tables = transform_all(raw_data)
    print(f"  ✓ Generated {len(tables)} tables")
    
    # Load
    print("\n[3/3] Loading to SQLite...")
    load_all(tables)
    
    # Validate (optional)
    if RUN_VALIDATION:
        print("\n" + "=" * 50)
        print("Running Data Quality Validation...")
        print("=" * 50)
        
        try:
            from validate_data import DataValidator
            validator = DataValidator()
            validator.run_all_checks()
            validator.close()
        except Exception as e:
            print(f"⚠️  Validation failed to run: {e}")
            print("You can run validation manually: python validate_data.py")
    
    print("\n" + "=" * 50)
    print("ETL Complete!")
    print("=" * 50)


if __name__ == "__main__":
    main()