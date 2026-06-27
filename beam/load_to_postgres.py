import os
import glob
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.types import JSON
from dotenv import load_dotenv

load_dotenv()

def main():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    silver_layer = os.path.join(project_root, 'silver_layer')
    
    # Find the parquet file
    parquet_files = glob.glob(os.path.join(silver_layer, '*.parquet'))
    if not parquet_files:
        print("No Parquet files found in Silver Layer.")
        return
        
    print(f"Reading {parquet_files[0]}...")
    table = pq.read_table(parquet_files[0])
    df = table.to_pandas()
    
    def sanitize_for_json(x):
        import numpy as np
        if isinstance(x, np.ndarray):
            return [sanitize_for_json(i) for i in x]
        elif isinstance(x, list):
            return [sanitize_for_json(i) for i in x]
        elif isinstance(x, dict):
            return {k: sanitize_for_json(v) for k, v in x.items()}
        elif pd.isna(x):
            return None
        elif isinstance(x, pd.Timestamp) or hasattr(x, 'isoformat'):
            return x.isoformat()
        return x

    for col in ['financials', 'status_history', 'metadata']:
        if col in df.columns:
            df[col] = df[col].apply(sanitize_for_json)

    # Connect to PostgreSQL
    db_user = os.getenv('DB_USER', 'admin')
    db_password = os.getenv('DB_PASSWORD', 'adminpassword')
    db_host = os.getenv('DB_HOST', 'postgres_gold')
    db_name = os.getenv('DB_NAME', 'supermarket_db')
    
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}')
    
    # Load into Postgres with JSON types for nested structures
    print("Loading into PostgreSQL...")
    df.to_sql('sales_enriched', engine, if_exists='replace', index=False, dtype={
        'financials': JSON,
        'status_history': JSON,
        'metadata': JSON
    })
    print("Successfully loaded Parquet data into public.sales_enriched.")

if __name__ == '__main__':
    main()
