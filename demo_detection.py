"""
Demo script to test the verification script's detection capabilities

This script demonstrates how the verification script detects new inserts and updates.
It safely handles re-runs by cleaning up test records first.
"""
import sqlite3

def main():
    """Main demo function."""
    conn = sqlite3.connect('scd2.db')
    cursor = conn.cursor()

    print("📝 Setting up demo records to demonstrate detection...\n")

    # Clean up any existing test records first (for re-runability)
    print("🧹 Cleaning up any existing test records...")
    cursor.execute("DELETE FROM sales_records_current WHERE id >= 100")
    
    # Get the current price of ID 1 to show update detection
    cursor.execute("SELECT price, total_amount FROM sales_records_current WHERE id = 1")
    result = cursor.fetchone()
    
    if result:
        current_price = result[0]
        print(f"   Current ID 1 price: ${current_price}")
    
    conn.commit()

    print("\n📥 Adding test records:\n")

    # Add a new record (will be detected as NEW)
    try:
        cursor.execute("""
            INSERT INTO sales_records_current 
            (id, transaction_date, product_name, category, price, quantity, 
             total_amount, customer_id, region, status)
            VALUES (100, '2024-02-15', 'Test Product NEW', 'Electronics', 299.99, 1, 
                    299.99, 3001, 'North', 'Active')
        """)
        print("   ✅ Inserted NEW record: ID 100 - Test Product NEW")
    except sqlite3.IntegrityError:
        print("   ⚠️  Record ID 100 already exists, skipping...")

    # Update an existing record (will be detected as UPDATED)
    new_price = 1899.99
    cursor.execute("""
        UPDATE sales_records_current 
        SET price = ?, total_amount = ? 
        WHERE id = 1
    """, (new_price, new_price))
    print(f"   ✅ Updated existing record: ID 1 - Changed price to ${new_price}")

    conn.commit()
    conn.close()

    print("\n" + "=" * 60)
    print("🔍 Demo setup complete!")
    print("=" * 60)
    print("\n📋 Next steps:")
    print("   1. Run: python verify_records.py")
    print("      (You should see 1 NEW record and 1 UPDATED record)")
    print("\n   2. Run: python scd_type2_process.py")
    print("      (This will process the changes)")
    print("\n   3. Run: python verify_records.py")
    print("      (Everything should be synchronized)")
    print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    main()
