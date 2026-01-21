"""
Sample Script to Add Changes to Source Data

This script demonstrates how to make changes (inserts and updates) to the source table.
Run this to test the SCD Type 2 detection and processing.
"""

import sqlite3

def add_sample_changes():
    """Add sample insert and update to demonstrate SCD Type 2."""
    print("📝 Adding sample changes to source data...\n")
    
    conn = sqlite3.connect('scd2.db')
    cursor = conn.cursor()
    
    try:
        # Insert new record
        print("1. Inserting new record (ID 20)...")
        cursor.execute("""
            INSERT INTO sales_records_current VALUES 
            (20, '2024-02-01', 'New Product', 'Electronics', 299.99, 1, 299.99, 2001, 'South', 'Active')
        """)
        print("   ✅ Inserted: ID 20 - New Product")
        
        # Update existing record
        print("\n2. Updating existing record (ID 1)...")
        cursor.execute("""
            UPDATE sales_records_current 
            SET price = 1499.99, total_amount = 1499.99 
            WHERE id = 1
        """)
        print("   ✅ Updated: ID 1 - Changed price to $1499.99")
        
        conn.commit()
        print("\n" + "="*60)
        print("✅ Changes committed to database!")
        print("="*60)
        print("\n📋 Next steps:")
        print("   1. Run: python verify_records.py")
        print("      (You should see 1 NEW and 1 UPDATED record)")
        print("\n   2. Run: python scd_type2_process.py")
        print("      (This will process the changes)")
        print("\n   3. Run: python verify_records.py")
        print("      (Everything should be synchronized)")
        print("\n" + "="*60)
        
    except sqlite3.IntegrityError as e:
        print(f"\n❌ Error: {e}")
        print("   This ID might already exist. Try deleting it first:")
        print("   python -c \"import sqlite3; conn = sqlite3.connect('scd2.db'); conn.execute('DELETE FROM sales_records_current WHERE id=20'); conn.commit()\"")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    add_sample_changes()
