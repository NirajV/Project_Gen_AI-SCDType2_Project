"""
Test Suite for SCD Type 2 Implementation

This module contains comprehensive tests for the SCD Type 2 process, including:
- Initial data loading
- Change detection and historical tracking
- New record insertion
- Idempotency verification

Author: Data Engineering Team
Version: 2.0
Date: January 2026
"""

import pytest
import sqlite3
import os
import time
from typing import Generator
from create_database import create_database, DB_FILENAME
from scd_type2_process import scd_type2_process


def get_db_connection() -> sqlite3.Connection:
    """
    Helper function to get a database connection.
    
    Returns:
        sqlite3.Connection: Active database connection
    """
    return sqlite3.connect(DB_FILENAME)


@pytest.fixture(scope="function")
def setup_database_fixture() -> Generator[None, None, None]:
    """
    Pytest fixture to reset the database before each test.
    
    This ensures a clean state for every test function by:
    1. Creating a fresh database
    2. Executing the SQL schema script
    3. Loading initial sample data
    
    Yields:
        None
        
    Note:
        The database file is left after tests for manual inspection if needed.
    """
    # Setup: Re-create the database using the existing script
    create_database()
    yield
    # Teardown: Database is left for inspection
    # Could add: os.remove(DB_FILENAME) if cleanup is desired


def test_initial_load(setup_database_fixture: None) -> None:
    """
    Test Case 1: Initial Load Verification
    
    Verifies that the first run of the SCD process correctly loads all records
    from the source table to the CDC table with proper initialization:
    - All records are inserted
    - All records are marked as active (is_current = 1)
    - No historical records exist yet
    
    Args:
        setup_database_fixture: Pytest fixture that resets the database
        
    Assertions:
        - Active record count matches source record count (7)
        - Historical record count is 0
    """
    # Run the SCD process
    scd_type2_process()

    conn = get_db_connection()
    cursor = conn.cursor()

    # Verify all records are inserted and active
    cursor.execute("SELECT COUNT(*) FROM sales_records_cdc WHERE is_current = 1")
    active_count = cursor.fetchone()[0]
    
    # Based on setup_database.sql, there are 7 initial records
    assert active_count == 7, f"Expected 7 active records, found {active_count}"

    # Verify no historical records yet
    cursor.execute("SELECT COUNT(*) FROM sales_records_cdc WHERE is_current = 0")
    history_count = cursor.fetchone()[0]
    assert history_count == 0, "Expected 0 historical records on initial load"

    conn.close()


def test_no_changes(setup_database_fixture: None) -> None:
    """
    Test Case 2: Idempotency Verification
    
    Verifies that running the SCD process multiple times with no source changes
    does not create duplicate records or alter the CDC table unnecessarily.
    This confirms the process is idempotent.
    
    Args:
        setup_database_fixture: Pytest fixture that resets the database
        
    Assertions:
        - Record count remains unchanged after second run
        - No new versions are created
    """
    # First run
    scd_type2_process()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sales_records_cdc")
    count_run_1 = cursor.fetchone()[0]
    conn.close()

    # Second run (no changes in source)
    scd_type2_process()

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sales_records_cdc")
    count_run_2 = cursor.fetchone()[0]
    conn.close()

    assert count_run_1 == count_run_2, (
        f"Row count should remain same when no changes occur. "
        f"Run 1: {count_run_1}, Run 2: {count_run_2}"
    )


def test_update_and_insert(setup_database_fixture: None) -> None:
    """
    Test Case 3: Update and Insert Operations
    
    Verifies the core SCD Type 2 functionality for handling:
    1. Updates to existing records (creates new version, expires old version)
    2. Insertion of completely new records
    
    Test Procedure:
    - Initial load of 7 records
    - Update record ID 1 (change price)
    - Insert new record ID 99
    - Verify historical tracking and versioning
    
    Args:
        setup_database_fixture: Pytest fixture that resets the database
        
    Assertions:
        - Updated record has 2 versions (old + new)
        - Old version is properly expired (is_current=0, valid end_date)
        - New version is active (is_current=1, end_date='9999-12-31')
        - New record is inserted and active
    """
    # 1. Initial Load
    scd_type2_process()
    
    # Wait 1 second to ensure different timestamps for the next run
    time.sleep(1)

    conn = get_db_connection()
    cursor = conn.cursor()

    # 2. Simulate Changes in Source
    # Update ID 1: Change Price from 1299.99 to 1400.00 (and update total_amount accordingly)
    cursor.execute("""
        UPDATE sales_records_current 
        SET price = 1400.00, total_amount = 1400.00 
        WHERE id = 1
    """)
    
    # Insert New Record ID 99
    cursor.execute("""
        INSERT INTO sales_records_current 
        (id, transaction_date, product_name, category, price, quantity, 
         total_amount, customer_id, region, status)
        VALUES (99, '2024-02-01', 'New Gadget', 'Electronics', 199.99, 1, 
                199.99, 2001, 'East', 'Active')
    """)
    conn.commit()
    conn.close()

    # 3. Run SCD Process to capture changes
    scd_type2_process()

    # 4. Verify Results
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check Update Logic for ID 1
    cursor.execute("""
        SELECT price, is_current, row_end_date 
        FROM sales_records_cdc 
        WHERE id = 1 
        ORDER BY row_start_date
    """)
    versions = cursor.fetchall()

    assert len(versions) == 2, (
        f"ID 1 should have 2 versions (history + current), found {len(versions)}"
    )
    
    # Old Version Validation
    old_price, old_is_current, old_end_date = versions[0]
    assert old_price == 1299.99, (
        f"Old version price mismatch: expected 1299.99, got {old_price}"
    )
    assert old_is_current == 0, (
        f"Old version should be inactive (is_current=0), got {old_is_current}"
    )
    assert old_end_date != '9999-12-31', (
        f"Old version should have a valid end date, not {old_end_date}"
    )

    # New Version Validation
    new_price, new_is_current, new_end_date = versions[1]
    assert new_price == 1400.00, (
        f"New version price mismatch: expected 1400.00, got {new_price}"
    )
    assert new_is_current == 1, (
        f"New version should be active (is_current=1), got {new_is_current}"
    )
    assert new_end_date == '9999-12-31', (
        f"New version should have max end date '9999-12-31', got {new_end_date}"
    )

    # Check Insert Logic for ID 99
    cursor.execute("""
        SELECT COUNT(*) 
        FROM sales_records_cdc 
        WHERE id = 99 AND is_current = 1
    """)
    new_record_count = cursor.fetchone()[0]
    assert new_record_count == 1, (
        f"New record ID 99 should be inserted and active, found {new_record_count}"
    )

    conn.close()


if __name__ == "__main__":
    # Allow running tests directly with: python test_scd_type2.py
    pytest.main([__file__, "-v"])
