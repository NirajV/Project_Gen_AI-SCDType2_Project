"""
Reset Database for Testing

This script safely resets the database for testing by:
1. Properly closing all connections
2. Removing the database file
3. Recreating a fresh database

Run this before executing tests.
"""

import os
import sys
import time
import gc

def reset_database():
    """Reset the database for clean testing."""
    db_file = 'scd2.db'
    
    print("🔄 Resetting database for testing...\n")
    
    # Step 1: Force garbage collection to close any lingering connections
    print("1. Closing any open connections...")
    gc.collect()
    time.sleep(0.5)
    
    # Step 2: Try to remove the database file
    print("2. Removing existing database...")
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            if os.path.exists(db_file):
                os.remove(db_file)
                print(f"   ✅ Database file removed")
                break
        except PermissionError as e:
            if attempt < max_attempts - 1:
                print(f"   ⚠️  Attempt {attempt + 1}/{max_attempts} failed: Database is locked")
                print(f"   ⏳ Waiting 2 seconds...")
                time.sleep(2)
                gc.collect()
            else:
                print(f"\n❌ ERROR: Cannot delete database file!")
                print(f"   The file is locked by another process.")
                print(f"\n💡 Solutions:")
                print(f"   1. Close any database viewers (DB Browser for SQLite, etc.)")
                print(f"   2. Close any Python scripts/terminals accessing the database")
                print(f"   3. Restart your IDE/terminal")
                print(f"   4. Manually delete 'scd2.db' file after closing all programs")
                return False
    
    # Step 3: Recreate the database
    print("3. Creating fresh database...")
    try:
        from create_database import create_database
        create_database()
        print("\n✅ Database reset complete!")
        print("\n📋 Now you can run tests:")
        print("   python -m pytest test_scd_type2.py -v")
        return True
    except Exception as e:
        print(f"\n❌ Error creating database: {e}")
        return False

if __name__ == "__main__":
    success = reset_database()
    sys.exit(0 if success else 1)
