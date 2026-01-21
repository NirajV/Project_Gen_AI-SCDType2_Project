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

## 💻 Usage

### Running the SCD Type 2 Process

Execute the main processing script:

```bash
python scd_type2_process.py
```

**First Run Output:**
```
2026-01-21 06:00:00 - INFO - --- Starting SCD Type 2 Process ---
2026-01-21 06:00:00 - INFO - New record detected for ID: 1
2026-01-21 06:00:00 - INFO - New record detected for ID: 2
...
2026-01-21 06:00:00 - INFO - Inserted 7 new records.
2026-01-21 06:00:00 - INFO - Process completed successfully.
```

**Subsequent Runs (No Changes):**
```
2026-01-21 06:00:00 - INFO - --- Starting SCD Type 2 Process ---
2026-01-21 06:00:00 - INFO - No changes detected. Target table is up to date.
2026-01-21 06:00:00 - INFO - Process completed successfully.
```

### Simulating Changes

To test the SCD Type 2 logic, modify source data and rerun:

```python
import sqlite3

conn = sqlite3.connect('scd2.db')
cursor = conn.cursor()

# Update a record - change price
cursor.execute("UPDATE sales_records_current SET price = 1499.99 WHERE id = 1")

# Insert a new record
cursor.execute("""
    INSERT INTO sales_records_current VALUES 
    (8, '2024-02-01', 'New Product', 'Electronics', 299.99, 1, 299.99, 1006, 'South', 'Active')
""")

conn.commit()
conn.close()

# Now run the process
# python scd_type2_process.py
```

### Querying Historical Data

**Get current active records:**
```sql
SELECT * FROM sales_records_cdc WHERE is_current = 1;
```

**Get all versions of a specific record:**
```sql
SELECT id, product_name, price, row_start_date, row_end_date, is_current 
FROM sales_records_cdc 
WHERE id = 1 
ORDER BY row_start_date;
```

**Get data as it existed on a specific date:**
```sql
SELECT * FROM sales_records_cdc 
WHERE '2024-01-20' BETWEEN row_start_date AND row_end_date;
```

---

## 🧪 Testing

### Run All Tests

```bash
pytest test_scd_type2.py -v
```

**Expected Output:**
```
test_scd_type2.py::test_initial_load PASSED
test_scd_type2.py::test_no_changes PASSED
test_scd_type2.py::test_update_and_insert PASSED
```

### Test Coverage

The test suite covers:

1. **Initial Load Test** - Verifies first-time data loading
2. **No Changes Test** - Ensures idempotency
3. **Update & Insert Test** - Validates change tracking and new record insertion

### Running Specific Tests

```bash
# Run a single test
pytest test_scd_type2.py::test_initial_load -v

# Run with detailed output
pytest test_scd_type2.py -v -s
```

---

## 📁 Project Structure

```
SCDType2_Project/
│
├── create_database.py          # Database initialization script
├── scd_type2_process.py        # Main SCD Type 2 processing logic
├── setup_database.sql          # SQL schema and sample data
├── test_scd_type2.py          # Comprehensive test suite
├── requirements.txt            # Python dependencies
├── README.md                   # This file
│
└── scd2.db                     # SQLite database (generated)
```

---

## 🔧 Configuration

### Database Path

By default, the database is created in the project root directory. To change the location, modify the `db_filename` variable in:
- `create_database.py`
- `scd_type2_process.py`
- `test_scd_type2.py`

### Logging

Logging is configured in `scd_type2_process.py`. To adjust log levels:

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change to DEBUG, INFO, WARNING, ERROR
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

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

### Issue: Database locked errors

**Solution:** Ensure no other processes are accessing `scd2.db`. Close any database viewers or other scripts.

### Issue: Test failures

**Solution:** 
1. Delete `scd2.db` to clean state
2. Run `python create_database.py`
3. Run tests again: `pytest test_scd_type2.py -v`

---

## 🎯 Use Cases

This implementation is ideal for:

- **Customer Master Data** - Track changes in customer information
- **Product Catalogs** - Maintain price history and product attribute changes
- **Employee Records** - Track salary changes, department transfers
- **Financial Data** - Audit trail for regulatory compliance
- **Sales Records** - Historical analysis of transaction data

---

## 🔄 Workflow Example

1. **Initial Load**: All source records loaded to CDC table as active
2. **Day 2**: Customer changes address → Old record expires, new record created
3. **Day 3**: New customer added → New active record inserted
4. **Day 4**: No changes → Process completes with no modifications
5. **Analysis**: Query historical data to see customer address on any date

---

## 📚 Key Concepts

### Hash-Based Change Detection

The system uses SHA-256 hashing to detect changes:
- Efficient comparison (single hash vs. multiple columns)
- Detects any attribute change
- Cryptographically secure

### Composite Primary Key

The CDC table uses `(id, row_start_date)` as primary key:
- Allows multiple versions of same ID
- Ensures uniqueness of each version
- Efficient querying with proper indexes

### Temporal Validity

Records include temporal bounds:
- `row_start_date`: Version effective date
- `row_end_date`: Version expiration date
- Enables point-in-time queries

---

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

---

## 📝 License

This project is open source and available for educational and commercial use.

---

## 👤 Author

**Data Engineering Team**
- GitHub: [@NirajV](https://github.com/NirajV)

---

## 🔗 Related Resources

- [SCD Type 2 Wikipedia](https://en.wikipedia.org/wiki/Slowly_changing_dimension)
- [Data Warehousing Concepts](https://en.wikipedia.org/wiki/Data_warehouse)
- [SQLite Documentation](https://www.sqlite.org/docs.html)

---

## 📅 Version History

- **v2.0** (January 2026) - Refactored with type hints, improved documentation, enhanced testing
- **v1.0** (January 2024) - Initial implementation

---

**Questions or Issues?** Please open an issue on the GitHub repository.
