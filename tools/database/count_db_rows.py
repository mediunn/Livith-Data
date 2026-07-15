import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from lib.db_utils import get_db_manager

def count_db_table_rows(table_name):
    db = get_db_manager()
    if not db.connect_with_ssh():
        print("Failed to connect to database.")
        return 0
    
    try:
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = db.execute_query(query)
        if result:
            return result[0][0] # Assuming count is the first column
        return 0
    except Exception as e:
        print(f"Error counting rows in {table_name}: {e}")
        return 0
    finally:
        db.disconnect()

if __name__ == "__main__":
    concerts_db_rows = count_db_table_rows("concerts")
    print(f"concerts table in DB has {concerts_db_rows} rows.")
