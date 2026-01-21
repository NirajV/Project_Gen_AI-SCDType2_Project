"""
Demo script to test the verification script's detection capabilities
"""
import sqlite3

conn = sqlite3.connect('scd2.db')
cursor = conn.cursor()

print("📝 Adding test records to demonstrate detection...\n")

# Add a new record (will be detected as NEW)
cursor.execute("""
    INSERT INTO sales_records_current 
    (id, transaction_date, product_name, category, price, quantity, 
     total_amount, customer_id, region, status)
    VALUES (100, '2024-02-15', 'Test Product NEW', 'Electronics', 299.99, 1, 
            299.99, 3001, 'North', 'Active')
""")
print("✅ Inserted NEW record: ID 100 - Test Product NEW")

# Update an existing record (will be detected as UPDATED)
cursor.execute("""
    UPDATE sales_records_current 
    SET price = 1599.99, total_amount = 1599.99 
    WHERE id = 1
""")
print("✅ Updated existing record: ID 1 - Changed price to $1599.99")

conn.commit()
conn.close()

print("\n🔍 Now run 'python verify_records.py' to see the detection!\n")
