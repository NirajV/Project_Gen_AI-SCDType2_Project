# SCD Type 2 Implementation Project

## 📋 Overview

This project demonstrates a complete implementation of **Slowly Changing Dimensions (SCD) Type 2** - a data warehousing technique used to track and preserve historical data changes over time.

### What is SCD Type 2?

SCD Type 2 maintains historical data by creating multiple records for a given natural key in dimensional tables. When a record changes:
- The old version is marked as inactive (historical)
- A new version is inserted with current data
- Both versions are preserved with timestamps

This allows you to:
- Track what changed and when
- Query data as it existed at any point in time
- Maintain complete audit trails

### Key Features

- ✅ **Automated Change Detection** - Uses SHA-256 hashing to detect data changes
- ✅ **Enhanced Verification Script** - Automatically detects new inserts and updates
- ✅ **Historical Tracking** - Maintains complete version history with timestamps
- ✅ **Type Safety** - Includes Python type hints for better code reliability
- ✅ **Comprehensive Testing** - Full pytest suite with multiple scenarios
- ✅ **Production Ready** - Robust error handling and logging
- ✅ **SQLite Based** - No external database server required

---

## 🏗️ Architecture

### Database Schema

**Source Table: `sales_records_current`**
- Contains the current/latest state of records
- Represents your operational data source

**Target Table: `sales_records_cdc`** (Change Data Capture)
- Stores all historical versions
- Includes SCD Type 2 audit columns:
  - `row_hash`: SHA-256 hash for change detection
  - `row_start_date`: When this version became active
  - `row_end_date`: When this version was superseded (9999-12-31 for current)
  - `is_current`: Flag indicating active version (1) or historical (0)

### Processing Logic

The SCD Type 2 process handles three scenarios:

**Scenario 1: New Record**
- Record exists in source but not in target
- Action: Insert as new active record

**Scenario 2: Changed Record**
- Record exists in both, but hash differs
- Action: Expire old version, insert new version

**Scenario 3: Unchanged Record**
- Record exists in both with identical hash
- Action: No operation needed

---

## 🚀 Setup

### Prerequisites

- Python 3.7 or higher
- pip (Python package manager)

### 1. Clone the Repository

```bash
git clone https://github.com/NirajV/Project_Gen_AI-SCDType2_Project.git
cd SCDType2_Project
```

### 2. Create Virtual Environment (Recommended)

Creating a virtual environment isolates project dependencies:

**Windows:**
```bash
python -m venv venv
.\venv\Scripts\activate
```

**macOS/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Initialize the Database

Run the database creation script to set up the SQLite database and load sample data:

```bash
python create_database.py
```

**Expected Output:**
```
Connected to database: scd2.db
Successfully executed setup_database.sql
Database setup complete.
```

This creates:
- `scd2.db` - SQLite database file
- `sales_records_current` table with 7 sample records
- `sales_records_cdc` table (empty, ready for processing)

---

## ⚡ Quick Start - Execution Sequence

Follow this exact sequence for first-time setup and usage:

### Initial Setup (One-Time)

```bash
# Step 1: Install dependencies
pip install -r requirements.txt

# Step 2: Create database with sample data
python create_database.py

# Step 3: Run initial SCD process (loads data to CDC table)
python scd_type2_process.py

# Step 4: Verify everything is synchronized
python verify_records.py
```

**Expected Result:** You should see "🟢 All records are synchronized!"

### Daily Operations Workflow

```bash
# Step 1: Check current status
python verify_records.py

# Step 2: Make changes to source data (insert/update records)
# ... use SQL or Python scripts ...

# Step 3: Verify pending changes (shows what needs processing)
python verify_records.py

# Step 4: Process the changes
python scd_type2_process.py

# Step 5: Confirm synchronization
python verify_records.py
```

### Testing Workflow

```bash
# Step 1: Reset database for clean testing
python reset_for_tests.py

# Step 2: Run tests (Windows)
python -m pytest test_scd_type2.py -v

# Linux/Mac can use:
pytest test_scd_type2.py -v
```

---

## 💻 Usage

### Complete Workflow with Verification

The recommended workflow includes using the verification script to monitor changes:

#### Step 1: Check Initial Status

```bash
python verify_records.py
```

This shows current synchronization status between source and CDC tables.

#### Step 2: Make Changes to Source Data

**Option A: Use the helper script (Recommended):**
```bash
python add_sample_changes.py
```

This script will:
- Insert a new record (ID 20)
- Update an existing record (ID 1)
- Show you the next steps

**Option B: Write your own Python script:**
Create a file (e.g., `my_changes.py`) with:
```python
import sqlite3

conn = sqlite3.connect('scd2.db')
cursor = conn.cursor()

# Insert new record
cursor.execute("""
    INSERT INTO sales_records_current VALUES 
    (20, '2024-02-01', 'New Product', 'Electronics', 299.99, 1, 299.99, 2001, 'South', 'Active')
""")

# Update existing record
cursor.execute("""
    UPDATE sales_records_current 
    SET price = 1499.99, total_amount = 1499.99 
    WHERE id = 1
""")

conn.commit()
conn.close()
print("Changes applied!")
```

Then run: `python my_changes.py`

**Option C: Use SQLite command line:**
```bash
sqlite3 scd2.db "INSERT INTO sales_records_current VALUES (20, '2024-02-01', 'New Product', 'Electronics', 299.99, 1, 299.99, 2001, 'South', 'Active');"
sqlite3 scd2.db "UPDATE sales_records_current SET price = 1499.99, total_amount = 1499.99 WHERE id = 1;"
```

#### Step 3: Verify Pending Changes

```bash
python verify_records.py
```

**Output Example:**
```
🆕 NEW RECORDS (Not in CDC): 1
   ID 20: New Product (Category: Electronics, Price: $299.99)

🔄 UPDATED RECORDS (Hash Mismatch): 1
   ID 1: Laptop Pro
      • price: 1299.99 → 1499.99

💡 Action Required: Run 'python scd_type2_process.py'
```

#### Step 4: Process the Changes

```bash
python scd_type2_process.py
```

**Output:**
```
2026-01-21 09:00:00 - INFO - --- Starting SCD Type 2 Process ---
2026-01-21 09:00:00 - INFO - Change detected for ID: 1
2026-01-21 09:00:00 - INFO - New record detected for ID: 20
2026-01-21 09:00:00 - INFO - Expired 1 old record(s).
2026-01-21 09:00:00 - INFO - Inserted 2 new record(s).
2026-01-21 09:00:00 - INFO - Process completed successfully.
```

#### Step 5: Confirm Synchronization

```bash
python verify_records.py
```

**Output:**
```
🟢 All records are synchronized!
   No action required - CDC table is up to date
```

---

## 🔍 Enhanced Verification Script

The `verify_records.py` script provides comprehensive change detection and reporting:

### Features

✅ **Automatic Detection**
- Detects ALL new records (not hardcoded IDs)
- Detects ALL updates via hash comparison
- No manual configuration needed

✅ **Field-Level Comparison**
- Shows exactly what changed
- Old value → New value format
- Makes debugging easy

✅ **Dashboard View**
- Summary statistics
- Recent changes with timestamps
- Historical version tracking
- Actionable recommendations

### Usage Examples

**Basic Status Check:**
```bash
python verify_records.py
```

**Sample Output:**
```
============================================================
SCD TYPE 2 VERIFICATION REPORT
============================================================
Generated: 2026-01-21 09:00:00

------------------------------------------------------------
📊 SUMMARY STATISTICS
------------------------------------------------------------
Source Records:           12
Active CDC Records:       12
Historical CDC Records:   4
Total CDC Records:        16

------------------------------------------------------------
🆕 NEW RECORDS (Not in CDC): 0
------------------------------------------------------------
✅ No new records found - all source records exist in CDC

------------------------------------------------------------
🔄 UPDATED RECORDS (Hash Mismatch): 0
------------------------------------------------------------
✅ No updates detected - all active CDC records match source

------------------------------------------------------------
✅ SYNCHRONIZED RECORDS: 12
------------------------------------------------------------
These 12 records are up-to-date in both SOURCE and CDC

------------------------------------------------------------
📜 RECENT CHANGES (Last 5 CDC Operations)
------------------------------------------------------------
   ID 1: Laptop Pro - ACTIVE (3 versions)
      Started: 2026-01-21 08:00:00
   ID 20: New Product - ACTIVE (1 version)
      Started: 2026-01-21 08:30:00
```

### Demo Script

Use `demo_detection.py` to see the verification script in action:

```bash
python demo_detection.py
```

This script:
1. Cleans up any test records
2. Inserts a new record (ID 100)
3. Updates an existing record (ID 1)
4. Provides step-by-step instructions to verify detection

---

## 📊 Querying Historical Data

### Get Current Active Records

```sql
SELECT * FROM sales_records_cdc WHERE is_current = 1;
```

### Get All Versions of a Specific Record

```sql
SELECT id, product_name, price, row_start_date, row_end_date, is_current 
FROM sales_records_cdc 
WHERE id = 1 
ORDER BY row_start_date;
```

**Example Output:**
```
ID | Product    | Price    | Start Date          | End Date            | Is Current
---|------------|----------|---------------------|---------------------|------------
1  | Laptop Pro | 1299.99  | 2024-01-15 10:00:00 | 2024-02-01 14:30:00 | 0
1  | Laptop Pro | 1499.99  | 2024-02-01 14:30:00 | 2024-02-15 09:15:00 | 0
1  | Laptop Pro | 1699.99  | 2024-02-15 09:15:00 | 9999-12-31          | 1
```

### Point-in-Time Query

Get data as it existed on a specific date:

```sql
SELECT * FROM sales_records_cdc 
WHERE '2024-01-20' BETWEEN row_start_date AND row_end_date;
```

### Find Records with Price Changes

```sql
SELECT DISTINCT c1.id, c1.product_name, 
       COUNT(*) as version_count,
       MIN(c1.price) as min_price,
       MAX(c1.price) as max_price
FROM sales_records_cdc c1
GROUP BY c1.id, c1.product_name
HAVING COUNT(*) > 1 AND MIN(c1.price) != MAX(c1.price);
```

---

## 🧪 Testing

### Prepare for Testing

**IMPORTANT:** Always reset the database before running tests to ensure a clean state:

```bash
python reset_for_tests.py
```

This script:
- Closes any open database connections
- Removes the existing database file
- Creates a fresh database with sample data

### Run All Tests

**Windows (Recommended):**
```bash
python -m pytest test_scd_type2.py -v
```

**Linux/Mac:**
```bash
pytest test_scd_type2.py -v
# OR
python -m pytest test_scd_type2.py -v
```

**Expected Output:**
```
test_scd_type2.py::test_initial_load PASSED                [ 33%]
test_scd_type2.py::test_no_changes PASSED                  [ 66%]
test_scd_type2.py::test_update_and_insert PASSED           [100%]

====================================== 3 passed in 1.20s ======================================
```

### Test Coverage

The test suite covers:

1. **Initial Load Test** - Verifies first-time data loading
2. **No Changes Test** - Ensures idempotency
3. **Update & Insert Test** - Validates change tracking and new record insertion

### Running Specific Tests

**Windows:**
```bash
# Run a single test
python -m pytest test_scd_type2.py::test_initial_load -v

# Run with detailed output
python -m pytest test_scd_type2.py -v -s

# Run tests with coverage report
python -m pytest test_scd_type2.py --cov=. --cov-report=html
```

**Linux/Mac:**
```bash
# Run a single test
pytest test_scd_type2.py::test_initial_load -v

# Run with detailed output
pytest test_scd_type2.py -v -s

# Run tests with coverage report
pytest test_scd_type2.py --cov=. --cov-report=html
```

### Complete Testing Workflow

```bash
# Step 1: Reset database for clean state
python reset_for_tests.py

# Step 2: Run tests (Windows)
python -m pytest test_scd_type2.py -v

# Step 3: If tests pass, your setup is correct!
```

---

## 📁 Project Structure

```
SCDType2_Project/
│
├── create_database.py          # Database initialization script
├── scd_type2_process.py        # Main SCD Type 2 processing logic
├── verify_records.py           # ⭐ Enhanced verification & detection script
├── add_sample_changes.py       # Helper script to add sample insert/update
├── demo_detection.py           # Demo script for testing detection
├── reset_for_tests.py          # Database reset utility for testing
├── setup_database.sql          # SQL schema and sample data
├── test_scd_type2.py          # Comprehensive test suite
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── CHANGELOG.md                # Version history and improvements
├── VERIFICATION_GUIDE.md       # Detailed verification script guide
├── .gitignore                  # Git ignore patterns
│
└── scd2.db                     # SQLite database (generated)
```

---

## 🔧 Configuration

### Database Path

By default, the database is created in the project root directory. To change the location, modify the `DB_FILENAME` constant in:
- `create_database.py`
- `scd_type2_process.py`
- `verify_records.py`
- `test_scd_type2.py`

### Logging

Logging is configured in `scd_type2_process.py`. To adjust log levels:

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change to DEBUG, INFO, WARNING, ERROR
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

### Verification Script Options

The verification script can be customized by modifying these functions in `verify_records.py`:
- `get_recent_changes(cursor, limit=5)` - Change the number of recent changes shown
- Add color output using libraries like `colorama` for better readability

---

## 🐛 Troubleshooting

### Issue: "Database scd2.db not found"

**Solution:** Run the database creation script first:
```bash
python create_database.py
```

### Issue: "Module not found" errors

**Solution:** Install dependencies:
```bash
pip install -r requirements.txt
```

### Issue: UNIQUE constraint failed

**Solution:** This occurs when trying to insert a record with an existing ID. Either:
1. Use the `demo_detection.py` script which handles this automatically
2. Delete the existing record first: `DELETE FROM sales_records_current WHERE id = X`
3. Use a different ID for your new record

### Issue: Database locked errors

**Solution:** Ensure no other processes are accessing `scd2.db`. Close any database viewers or other scripts.

### Issue: Test failures

**Solution:** 
1. Delete `scd2.db` to clean state
2. Run `python create_database.py`
3. Run tests again: `pytest test_scd_type2.py -v`

### Issue: Verification script shows changes but scd_type2_process doesn't detect them

**Solution:** Ensure you committed your database changes with `conn.commit()` after making updates to the source table.

---

## 🎯 Use Cases

This implementation is ideal for:

- **Customer Master Data** - Track changes in customer information (addresses, contact info, preferences)
- **Product Catalogs** - Maintain price history and product attribute changes
- **Employee Records** - Track salary changes, department transfers, role changes
- **Financial Data** - Audit trail for regulatory compliance
- **Sales Records** - Historical analysis of transaction data
- **Configuration Management** - Track system configuration changes over time

---

## 🔄 Complete Workflow Example

### Day 1: Initial Load
```bash
# Setup database
python create_database.py

# Initial load
python scd_type2_process.py

# Verify
python verify_records.py
```
**Result:** All 7 source records loaded to CDC as active.

### Day 2: Customer Changes Address
```python
# Update customer address
cursor.execute("UPDATE sales_records_current SET region = 'West' WHERE id = 3")
conn.commit()
```

```bash
python verify_records.py
# Shows: 1 UPDATED record detected

python scd_type2_process.py
# Creates new version, expires old

python verify_records.py
# Shows: All synchronized, ID 3 has 2 versions
```

### Day 3: New Customer Added
```python
# Insert new customer
cursor.execute("""INSERT INTO sales_records_current VALUES 
    (15, '2024-02-03', 'Smart Speaker', 'Electronics', 149.99, 1, 149.99, 2005, 'East', 'Active')
""")
conn.commit()
```

```bash
python verify_records.py
# Shows: 1 NEW record detected

python scd_type2_process.py
# Inserts new active record

python verify_records.py
# Shows: All synchronized
```

### Day 4: No Changes
```bash
python verify_records.py
# Shows: All synchronized

python scd_type2_process.py
# No changes detected, no action taken
```

### Day 5: Analysis
```sql
-- See ID 3's history
SELECT id, region, row_start_date, row_end_date, is_current
FROM sales_records_cdc
WHERE id = 3
ORDER BY row_start_date;

-- Output shows:
-- Version 1: region='East', ended on Day 2
-- Version 2: region='West', still active
```

---

## 📚 Key Concepts

### Hash-Based Change Detection

The system uses SHA-256 hashing to detect changes:
- **Efficient**: Single hash comparison vs. multiple column comparisons
- **Comprehensive**: Detects ANY attribute change
- **Reliable**: Cryptographically secure
- **Implementation**: `calculate_row_hash()` function in both `scd_type2_process.py` and `verify_records.py`

**Example:**
```python
# Row: (1, '2024-01-15', 'Laptop', 'Electronics', 1299.99, ...)
# Hash: SHA256("2024-01-15|Laptop|Electronics|1299.99|...")
```

### Composite Primary Key

The CDC table uses `(id, row_start_date)` as primary key:
- **Multiple Versions**: Allows multiple versions of the same ID
- **Uniqueness**: Each version has a unique start timestamp
- **Performance**: Enables efficient querying with proper indexes

### Temporal Validity

Records include temporal bounds for point-in-time queries:
- **row_start_date**: When this version became effective
- **row_end_date**: When this version expired (9999-12-31 for current)
- **Enables**: "Show me the data as it was on DATE" queries

---

## 🆕 What's New in Version 2.0

### Enhanced Verification Script (`verify_records.py`)
- ✅ Automatic detection of NEW records (no hardcoded IDs)
- ✅ Automatic detection of UPDATED records (hash-based)
- ✅ Field-level change comparison (shows old → new values)
- ✅ Dashboard view with statistics
- ✅ Historical tracking and version counts
- ✅ Actionable recommendations

### Code Quality Improvements
- ✅ Python type hints throughout
- ✅ Constants for magic numbers (CDCColumnIndex class)
- ✅ Comprehensive docstrings
- ✅ Better error handling
- ✅ Modular function design

### Documentation
- ✅ Complete README (this file)
- ✅ Detailed VERIFICATION_GUIDE.md
- ✅ Comprehensive CHANGELOG.md
- ✅ .gitignore for Python projects

### Testing
- ✅ Fixed timing issues in tests
- ✅ Better test assertions
- ✅ Improved test documentation

---

## 📖 Additional Documentation

- **[VERIFICATION_GUIDE.md](VERIFICATION_GUIDE.md)** - Detailed guide for the verification script
- **[CHANGELOG.md](CHANGELOG.md)** - Complete version history and improvements
- **Python Docstrings** - All functions have comprehensive inline documentation

---

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Make your changes with tests
4. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
5. Push to the branch (`git push origin feature/AmazingFeature`)
6. Open a Pull Request

### Development Setup

```bash
# Clone and setup
git clone https://github.com/NirajV/Project_Gen_AI-SCDType2_Project.git
cd SCDType2_Project
python -m venv venv
source venv/bin/activate  # Windows: .\venv\Scripts\activate
pip install -r requirements.txt

# Run tests
pytest test_scd_type2.py -v

# Check code style
flake8 *.py

# Format code
black *.py
```

---

## 📝 License

This project is open source and available for educational and commercial use.

---

## 👤 Author

**Data Engineering Team**
- GitHub: [@NirajV](https://github.com/NirajV)
- Repository: [Project_Gen_AI-SCDType2_Project](https://github.com/NirajV/Project_Gen_AI-SCDType2_Project)

---

## 🔗 Related Resources

- [SCD Type 2 Wikipedia](https://en.wikipedia.org/wiki/Slowly_changing_dimension)
- [Data Warehousing Concepts](https://en.wikipedia.org/wiki/Data_warehouse)
- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [Python Type Hints (PEP 484)](https://www.python.org/dev/peps/pep-0484/)
- [SHA-256 Hash Function](https://en.wikipedia.org/wiki/SHA-2)

---

## 📅 Version History

- **v2.0** (January 2026) - Major update
  - Enhanced verification script with automatic detection
  - Added type hints throughout
  - Refactored code with constants
  - Comprehensive documentation
  - Fixed data quality issues
  - Production-ready code

- **v1.0** (January 2024) - Initial implementation
  - Basic SCD Type 2 functionality
  - SQLite integration
  - Initial test suite

---

## ❓ FAQ

**Q: How do I know if changes need to be processed?**  
A: Run `python verify_records.py` - it will tell you exactly what needs processing.

**Q: Can I run the SCD process multiple times safely?**  
A: Yes! The process is idempotent - running it multiple times with no changes won't create duplicates.

**Q: How do I see the history of a specific record?**  
A: Query: `SELECT * FROM sales_records_cdc WHERE id = X ORDER BY row_start_date`

**Q: What if I need to delete a record?**  
A: SCD Type 2 typically doesn't handle deletions. Consider using a soft delete (status field = 'Inactive').

**Q: Can I use this with other databases?**  
A: Yes, the logic is database-agnostic. You'll need to update the connection code for PostgreSQL, MySQL, etc.

**Q: How do I automate the SCD process?**  
A: Use scheduling tools (cron on Linux, Task Scheduler on Windows) to run the scripts periodically.

---

**Questions or Issues?** Please open an issue on the [GitHub repository](https://github.com/NirajV/Project_Gen_AI-SCDType2_Project/issues).

---

**⭐ If you find this project helpful, please consider giving it a star on GitHub!**
