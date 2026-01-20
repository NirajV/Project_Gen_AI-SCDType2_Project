import pytest
import sqlite3
import os
from create_database import create_database
from scd_type2_process import scd_type2_process

# Define database path relative to this script
DB_NAME = 'scd2.db'

@pytest.fixture(scope="function")
def setup_database_fixture():
    """
    Fixture to reset the database before each test.
    This ensures a clean state for every test function.
    """
    # Re-create the database using the existing script
    create_database()
    yield
    # Teardown: We can leave the DB for inspection or remove it.
    # For now, we leave it.

def get_db_connection():
    """Helper to get a database connection."""
    return sqlite3.connect(DB_NAME)

def test_initial_load(setup_database_fixture):
    """
    Test 1: Verify that the first run loads all records from source to CDC.
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

def test_no_changes(setup_database_fixture):
    """
    Test 2: Verify that running the process again with no source changes 
    does not alter the CDC table.
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

    assert count_run_1 == count_run_2, "Row count should remain same when no changes occur"

def test_update_and_insert(setup_database_fixture):
    """
    Test 3: Verify SCD Type 2 logic for Updates and Inserts.
    - Update an existing record (should expire old, insert new).
    - Insert a completely new record.
    """
    # 1. Initial Load
    scd_type2_process()

    conn = get_db_connection()
    cursor = conn.cursor()

    # 2. Simulate Changes in Source
    # Update ID 1: Change Price from 1299.99 to 1400.00
    cursor.execute("UPDATE sales_records_current SET price = 1400.00 WHERE id = 1")
    
    # Insert New Record ID 99
    cursor.execute("""
        INSERT INTO sales_records_current 
        (id, transaction_date, product_name, category, price, quantity, total_amount, customer_id, region, status)
        VALUES (99, '2024-02-01', 'New Gadget', 'Electronics', 199.99, 1, 199.99, 2001, 'East', 'Active')
    """)
    conn.commit()
    conn.close()

    # 3. Run SCD Process to capture changes
    scd_type2_process()

    # 4. Verify Results
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check Update Logic for ID 1
    cursor.execute("SELECT price, is_current, row_end_date FROM sales_records_cdc WHERE id = 1 ORDER BY row_start_date")
    versions = cursor.fetchall()

    assert len(versions) == 2, "ID 1 should have 2 versions (history + current)"
    
    # Old Version
    assert versions[0][0] == 1299.99, "Old version price mismatch"
    assert versions[0][1] == 0, "Old version should be inactive (is_current=0)"
    assert versions[0][2] != '9999-12-31', "Old version should have a valid end date"

    # New Version
    assert versions[1][0] == 1400.00, "New version price mismatch"
    assert versions[1][1] == 1, "New version should be active (is_current=1)"
    assert versions[1][2] == '9999-12-31', "New version should have max end date"

    # Check Insert Logic for ID 99
    cursor.execute("SELECT count(*) FROM sales_records_cdc WHERE id = 99 AND is_current = 1")
    new_record_count = cursor.fetchone()[0]
    assert new_record_count == 1, "New record ID 99 should be inserted and active"

    conn.close()