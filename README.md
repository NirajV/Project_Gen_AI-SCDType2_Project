# Project_Gen_AI-SCDType2_Project

## Overview
This project demonstrates the implementation of **Slowly Changing Dimensions (SCD) Type 2**.
SCD Type 2 is a data warehousing technique used to track historical data by creating multiple records for a given natural key in the dimensional tables, preserving the history of changes.

## Database Setup
The database schema is defined in `setup_database.sql` and initialized using `create_database.py`.

### Tables Created
1. **`sales_records_current`** (Source Table):
   - Represents the current state of sales data.
   - Contains sample data inserted during setup.

2. **`sales_records_cdc`** (CDC/Dimension Table):
   - Stores historical changes using SCD Type 2 logic.
   - Includes audit columns: `row_hash`, `row_start_date`, `row_end_date`, and `is_current`.

### Usage
Run the Python script to initialize the SQLite database (`scd2.db`):
```bash
python create_database.py
```

Reference: setup_database.sql, create_database.py
