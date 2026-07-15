import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from lib.db_utils import get_db_manager

def count_csv_rows(csv_file_name):
    db = get_db_manager()
    csv_path = db.get_data_path(csv_file_name)
    if not os.path.exists(csv_path):
        print(f"Error: File not found at {csv_path}")
        return 0
    
    df = pd.read_csv(csv_path, encoding='utf-8')
    return len(df)

if __name__ == "__main__":
    concerts_csv_rows = count_csv_rows("concerts.csv")
    print(f"concerts.csv has {concerts_csv_rows} rows.")
