import sqlite3
import hashlib
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def calculate_row_hash(row):
    """
    Calculates SHA-256 hash of the row data (excluding ID).
    Row is expected to be a tuple where index 0 is ID.
    """
    # Convert all non-ID columns to string and concatenate with a separator
    # We exclude index 0 (ID) because we want to detect changes in attributes
    data_str = "|".join(str(x) for x in row[1:])
    return hashlib.sha256(data_str.encode('utf-8')).hexdigest()

def scd_type2_process():
    """
    Performs the SCD Type 2 Merge operation:
    1. Identifies new records.
    2. Identifies changed records (using hash comparison).
    3. Expires old versions of changed records.
    4. Inserts new versions and new records.
    """
    db_filename = 'scd2.db'
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, db_filename)

    if not os.path.exists(db_path):
        logger.error(f"Database {db_filename} not found. Please run create_database.py first.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    logger.info("--- Starting SCD Type 2 Process ---")

    # 1. Fetch Source Data (sales_records_current)
    cursor.execute("SELECT * FROM sales_records_current")
    source_data = cursor.fetchall()
    
    # 2. Fetch Target Data (sales_records_cdc) - Only Active Records
    cursor.execute("SELECT * FROM sales_records_cdc WHERE is_current = 1")
    target_data = cursor.fetchall()

    # Create a lookup dictionary for target data: {id: row_data}
    target_lookup = {row[0]: row for row in target_data}

    # Constants
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    max_date = '9999-12-31'

    records_to_insert = []
    records_to_expire = []

    for src_row in source_data:
        src_id = src_row[0]
        src_hash = calculate_row_hash(src_row)

        if src_id not in target_lookup:
            # Scenario 4 & 5: New Record (or First Run)
            # Record does not exist in Target, treat as new.
            logger.info(f"New record detected for ID: {src_id}")
            # Construct new record: Source Cols + Hash + Start + End + IsCurrent
            new_record = src_row + (src_hash, current_timestamp, max_date, 1)
            records_to_insert.append(new_record)
        
        else:
            # Record exists, check for changes
            tgt_row = target_lookup[src_id]
            # In sales_records_cdc, row_hash is at index 10 (based on setup_database.sql)
            tgt_hash = tgt_row[10]
            tgt_start_date = tgt_row[11]

            if src_hash != tgt_hash:
                # Scenario 5: Update detected
                logger.info(f"Change detected for ID: {src_id}")
                
                # 1. Expire the current active record (Update row_end_date)
                records_to_expire.append((current_timestamp, src_id, tgt_start_date))

                # 2. Insert the new version of the record
                new_record = src_row + (src_hash, current_timestamp, max_date, 1)
                records_to_insert.append(new_record)
            else:
                # Scenario 3: No changes identified. Do nothing.
                pass

    # Perform Database Operations
    try:
        if records_to_expire:
            cursor.executemany("""
                UPDATE sales_records_cdc
                SET row_end_date = ?, is_current = 0
                WHERE id = ? AND row_start_date = ?
            """, records_to_expire)
            logger.info(f"Expired {len(records_to_expire)} old records.")

        if records_to_insert:
            cursor.executemany("""
                INSERT INTO sales_records_cdc (
                    id, transaction_date, product_name, category, price, quantity, 
                    total_amount, customer_id, region, status, 
                    row_hash, row_start_date, row_end_date, is_current
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, records_to_insert)
            logger.info(f"Inserted {len(records_to_insert)} new records.")

        if not records_to_expire and not records_to_insert:
            logger.info("No changes detected. Target table is up to date.")

        conn.commit()
        logger.info("Process completed successfully.")

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    scd_type2_process()