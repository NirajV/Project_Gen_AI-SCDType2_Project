# Project_Gen_AI-SCDType2_Project

## Overview
This project demonstrates the implementation of **Slowly Changing Dimensions (SCD) Type 2**.
SCD Type 2 is a data warehousing technique used to track historical data by creating multiple records for a given natural key in the dimensional tables, preserving the history of changes.

## Setup

### 1. Create a Virtual Environment (Recommended)
It is recommended to create and activate a virtual environment to keep project dependencies isolated.

```bash
# Create a virtual environment named 'venv'
python -m venv venv

# Activate the environment
# On Windows:
.\venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### 2. Initialize the Database
Run the `create_database.py` script. This will create the `scd2.db` SQLite database and execute `setup_database.sql` to create the tables and insert initial sample data.
```bash
python create_database.py
```

Reference: setup_database.sql, create_database.py
