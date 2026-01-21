"""
SCD Type 2 Processing Module

This module implements the core logic for Slowly Changing Dimensions Type 2 (SCD Type 2).
It detects changes in source data and maintains historical versions in the target table.

Processing Scenarios:
    Scenario 1 - New Record: Record exists in source but not in target
                             Action: Insert as new active record
    
    Scenario 2 - Changed Record: Record exists in both, but hash differs
                                  Action: Expire old version, insert new version
    
    Scenario 3 - Unchanged Record: Record exists in both with identical hash
                                    Action: No operation needed

Author: Data Engineering Team
Version: 2.0
Date: January 2026
"""

import sqlite3
import hashlib
import os
import logging
from datetime import datetime
from typing import List, Tuple, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DB_FILENAME: str = 'scd2.db'
MAX_DATE: str = '9999-12-31'  # End date for active records
HASH_ALGORITHM: str = 'sha256'

# Column indices for CDC table (sales_records_cdc)
# Used to avoid magic numbers when accessing tuple elements
class CDCColumnIndex:
    """Column indices for the CDC table structure."""
    ID: int = 0
    TRANSACTION_DATE: int = 1
    PRODUCT_NAME: int = 2
    CATEGORY: int = 3
    PRICE: int = 4
    QUANTITY: int = 5
    TOTAL_AMOUNT: int = 6
    CUSTOMER_ID: int = 7
    REGION: int = 8
    STATUS: int = 9
    ROW_HASH: int = 10
    ROW_START_DATE: int = 11
    ROW_END_DATE: int = 12
    IS_CURRENT: int = 13


def calculate_row_hash(row: Tuple) -> str:
    """
    Calculates SHA-256 hash of the row data to detect changes.
    
    The hash is calculated on all columns except the ID (index 0), as we want to
    detect changes in attribute values, not the identifier itself.
    
    Args:
        row: A tuple containing record data where index 0 is the ID
        
    Returns:
        A hexadecimal string representing the SHA-256 hash of the row data
        
    Example:
        >>> row = (1, '2024-01-15', 'Laptop', 'Electronics', 1299.99, 1, 1299.99, 1001, 'North', 'Active')
        >>> hash_value = calculate_row_hash(row)
        >>> len(hash_value)
        64
    """
    # Concatenate all non-ID columns with a separator
    # Using pipe separator to ensure consistent hashing
    data_str = "|".join(str(x) for x in row[1:])
    return hashlib.sha256(data_str.encode('utf-8')).hexdigest()


def get_current_timestamp() -> str:
    """
    Returns the current timestamp in a consistent format.
    
    Returns:
        Current timestamp as a string in 'YYYY-MM-DD HH:MM:SS' format
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def fetch_source_data(cursor: sqlite3.Cursor) -> List[Tuple]:
    """
    Fetches all records from the source table.
    
    Args:
        cursor: Database cursor object
        
    Returns:
        List of tuples containing source records
    """
    cursor.execute("SELECT * FROM sales_records_current")
    return cursor.fetchall()


def fetch_target_data(cursor: sqlite3.Cursor) -> Dict[int, Tuple]:
    """
    Fetches active records from the CDC target table.
    
    Args:
        cursor: Database cursor object
        
    Returns:
        Dictionary mapping record IDs to their data tuples
    """
    cursor.execute("SELECT * FROM sales_records_cdc WHERE is_current = 1")
    target_data = cursor.fetchall()
    return {row[CDCColumnIndex.ID]: row for row in target_data}


def process_records(
    source_data: List[Tuple],
    target_lookup: Dict[int, Tuple],
    current_timestamp: str
) -> Tuple[List[Tuple], List[Tuple]]:
    """
    Processes source records and determines what needs to be inserted or expired.
    
    Args:
        source_data: List of tuples from source table
        target_lookup: Dictionary of active target records (ID -> row data)
        current_timestamp: Current timestamp for audit columns
        
    Returns:
        A tuple containing:
        - records_to_insert: List of new records/versions to insert
        - records_to_expire: List of old records to expire
    """
    records_to_insert: List[Tuple] = []
    records_to_expire: List[Tuple] = []

    for src_row in source_data:
        src_id = src_row[CDCColumnIndex.ID]
        src_hash = calculate_row_hash(src_row)

        if src_id not in target_lookup:
            # Scenario 1: New Record
            # Record does not exist in Target, treat as new insertion
            logger.info(f"New record detected for ID: {src_id}")
            
            # Construct new record: Source Cols + Hash + Start Date + End Date + IsCurrent
            new_record = src_row + (src_hash, current_timestamp, MAX_DATE, 1)
            records_to_insert.append(new_record)
        
        else:
            # Record exists, check for changes using hash comparison
            tgt_row = target_lookup[src_id]
            tgt_hash = tgt_row[CDCColumnIndex.ROW_HASH]
            tgt_start_date = tgt_row[CDCColumnIndex.ROW_START_DATE]

            if src_hash != tgt_hash:
                # Scenario 2: Change Detected
                logger.info(f"Change detected for ID: {src_id}")
                
                # 1. Expire the current active record
                records_to_expire.append((current_timestamp, src_id, tgt_start_date))

                # 2. Insert the new version of the record
                new_record = src_row + (src_hash, current_timestamp, MAX_DATE, 1)
                records_to_insert.append(new_record)
            else:
                # Scenario 3: No Changes Identified
                # Do nothing - record is already current and unchanged
                pass

    return records_to_insert, records_to_expire


def apply_changes(
    cursor: sqlite3.Cursor,
    conn: sqlite3.Connection,
    records_to_expire: List[Tuple],
    records_to_insert: List[Tuple]
) -> bool:
    """
    Applies the identified changes to the database.
    
    Args:
        cursor: Database cursor object
        conn: Database connection object
        records_to_expire: List of records to mark as inactive
        records_to_insert: List of new records/versions to insert
        
    Returns:
        True if successful, False if an error occurred
    """
    try:
        # Expire old records by updating end date and is_current flag
        if records_to_expire:
            cursor.executemany("""
                UPDATE sales_records_cdc
                SET row_end_date = ?, is_current = 0
                WHERE id = ? AND row_start_date = ?
            """, records_to_expire)
            logger.info(f"Expired {len(records_to_expire)} old record(s).")

        # Insert new records and new versions
        if records_to_insert:
            cursor.executemany("""
                INSERT INTO sales_records_cdc (
                    id, transaction_date, product_name, category, price, quantity, 
                    total_amount, customer_id, region, status, 
                    row_hash, row_start_date, row_end_date, is_current
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, records_to_insert)
            logger.info(f"Inserted {len(records_to_insert)} new record(s).")

        if not records_to_expire and not records_to_insert:
            logger.info("No changes detected. Target table is up to date.")

        conn.commit()
        return True

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        return False


def scd_type2_process() -> None:
    """
    Executes the complete SCD Type 2 merge operation.
    
    This function orchestrates the entire SCD Type 2 process:
    1. Connects to the database
    2. Fetches source and target data
    3. Identifies new, changed, and unchanged records
    4. Expires old versions of changed records
    5. Inserts new versions and new records
    6. Commits changes or rolls back on error
    
    The process is idempotent - running it multiple times with unchanged
    source data will not create duplicate records.
    
    Returns:
        None
        
    Raises:
        sqlite3.Error: If database operations fail
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, DB_FILENAME)

    if not os.path.exists(db_path):
        logger.error(
            f"Database {DB_FILENAME} not found in {script_dir}. "
            "Please run create_database.py first."
        )
        return

    conn: Optional[sqlite3.Connection] = None
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        logger.info("--- Starting SCD Type 2 Process ---")

        # Fetch data from source and target tables
        source_data = fetch_source_data(cursor)
        target_lookup = fetch_target_data(cursor)

        # Get current timestamp for audit columns
        current_timestamp = get_current_timestamp()

        # Process records and identify changes
        records_to_insert, records_to_expire = process_records(
            source_data, target_lookup, current_timestamp
        )

        # Apply changes to database
        success = apply_changes(cursor, conn, records_to_expire, records_to_insert)

        if success:
            logger.info("Process completed successfully.")
        else:
            logger.error("Process completed with errors.")

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    scd_type2_process()
