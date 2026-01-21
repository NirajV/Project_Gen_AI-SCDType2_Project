"""
Database Setup Module for SCD Type 2 Implementation

This module creates and initializes the SQLite database for the SCD Type 2 project.
It executes the SQL schema script and loads initial sample data.
"""

import sqlite3
import os
from typing import Optional

# Constants
DB_FILENAME: str = 'scd2.db'
SQL_FILENAME: str = 'setup_database.sql'


def create_database() -> None:
    """
    Creates a SQLite database and executes the setup_database.sql script.
    
    This function:
    1. Removes any existing database file for a clean setup
    2. Creates a new SQLite database
    3. Executes the SQL schema script to create tables and load sample data
    4. Handles errors gracefully with informative messages
    
    Returns:
        None
        
    Raises:
        sqlite3.Error: If database operations fail
        OSError: If file operations fail
    """
    db_filename = DB_FILENAME
    sql_filename = SQL_FILENAME
    
    # Get the absolute path of the current script to locate the SQL file correctly
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(script_dir, sql_filename)
    db_path = os.path.join(script_dir, db_filename)

    if not os.path.exists(sql_path):
        print(f"Error: '{sql_filename}' not found in {script_dir}")
        return

    # Remove existing database to ensure a clean setup
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            print(f"Removed existing database: {db_filename}")
        except OSError as e:
            print(f"Error removing existing database: {e}")
            return

    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print(f"Connected to database: {db_filename}")

        # Read and execute the SQL script
        with open(sql_path, 'r') as f:
            sql_script = f.read()
            
        cursor.executescript(sql_script)
        print(f"Successfully executed {sql_filename}")

        # Commit and close
        conn.commit()
        conn.close()
        print("Database setup complete.")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    create_database()
