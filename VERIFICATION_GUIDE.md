# Enhanced Verification Script Guide

## Overview

The **verify_records.py** script automatically detects and reports:
- ✅ New records in source table not yet in CDC
- ✅ Updated records with hash mismatches
- ✅ Synchronized records
- ✅ Historical tracking and versioning
- ✅ Actionable recommendations

---

## Features

### 1. **Automatic Detection**
No hardcoded IDs! The script dynamically compares source and CDC tables to find:
- Records in source but not in CDC (NEW)
- Records with data changes (UPDATED via hash comparison)

### 2. **Field-Level Change Tracking**
For updated records, shows exactly what changed:
```
ID 1: Laptop Pro
   • price: 1700.0 → 1599.99
   • total_amount: 1400.0 → 1599.99
```

### 3. **Historical Analysis**
- Shows records with multiple versions
- Displays recent CDC operations
- Tracks version counts per record

### 4. **Actionable Insights**
Tells you exactly what to do:
- ⚠️ If changes detected: "RUN: python scd_type2_process.py"
- ✅ If synchronized: "No action required"

---

## Usage

### Basic Usage

Simply run the script anytime to check status:

```bash
python verify_records.py
```

### Recommended Workflow

**Step 1:** Make changes to source table
```python
import sqlite3
conn = sqlite3.connect('scd2.db')
cursor = conn.cursor()

# Insert new record
cursor.execute("""
    INSERT INTO sales_records_current 
    VALUES (100, '2024-02-15', 'New Product', 'Electronics', 299.99, 1, 299.99, 3001, 'North', 'Active')
""")

# Update existing record
cursor.execute("UPDATE sales_records_current SET price = 1599.99 WHERE id = 1")

conn.commit()
conn.close()
```

**Step 2:** Run verification to see what needs processing
```bash
python verify_records.py
```

**Output:**
```
🔴 2 record(s) need processing:
   • 1 new record(s) to insert
   • 1 record(s) to update with new version

➡️  RUN: python scd_type2_process.py
```

**Step 3:** Process the changes
```bash
python scd_type2_process.py
```

**Step 4:** Verify synchronization
```bash
python verify_records.py
```

**Output:**
```
🟢 All records are synchronized!
   No action required - CDC table is up to date
```

---

## Sample Output

### When Changes Are Detected

```
============================================================
SCD TYPE 2 VERIFICATION REPORT
============================================================
Generated: 2026-01-21 08:26:46

------------------------------------------------------------
📊 SUMMARY STATISTICS
------------------------------------------------------------
Source Records:           12
Active CDC Records:       11
Historical CDC Records:   3
Total CDC Records:        14

------------------------------------------------------------
🆕 NEW RECORDS (Not in CDC): 1
------------------------------------------------------------

⚠️  These records exist in SOURCE but NOT in CDC:
   ID 100: Test Product NEW (Category: Electronics, Price: $299.99)

💡 Action Required: Run 'python scd_type2_process.py' to insert these records

------------------------------------------------------------
🔄 UPDATED RECORDS (Hash Mismatch): 1
------------------------------------------------------------

⚠️  These records have been modified in SOURCE:

   ID 1: Laptop Pro
      • price: 1700.0 → 1599.99
      • total_amount: 1400.0 → 1599.99

💡 Action Required: Run 'python scd_type2_process.py' to create new versions

------------------------------------------------------------
⚡ ACTION SUMMARY
------------------------------------------------------------

🔴 2 record(s) need processing:
   • 1 new record(s) to insert
   • 1 record(s) to update with new version

➡️  RUN: python scd_type2_process.py
```

### When Everything Is Synchronized

```
============================================================
SCD TYPE 2 VERIFICATION REPORT
============================================================
Generated: 2026-01-21 08:27:04

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
⚡ ACTION SUMMARY
------------------------------------------------------------

🟢 All records are synchronized!
   No action required - CDC table is up to date
```

---

## Technical Details

### How It Works

1. **Hash Calculation**: Uses the same SHA-256 hashing logic as `scd_type2_process.py`
2. **Smart Comparison**: Only compares active CDC records (`is_current = 1`)
3. **Field-Level Diff**: Compares each field to show exactly what changed
4. **No Database Changes**: Read-only operations - safe to run anytime

### What It Detects

✅ **New Records**
- Records with IDs in source but not in active CDC
- Shows product name, category, and price

✅ **Updated Records**  
- Records where hash(source) ≠ hash(CDC)
- Shows field-by-field changes with old → new values

✅ **Historical Tracking**
- Records with multiple versions
- Version counts per ID

❌ **What It Doesn't Detect**
- Deleted records (records in CDC but not in source)
  - SCD Type 2 typically doesn't handle deletions
  - Use soft deletes (status field) if needed

---

## Automation Ideas

### Schedule Regular Checks

**Windows (Task Scheduler):**
```batch
@echo off
cd C:\path\to\SCDType2_Project
python verify_records.py > verification_log.txt
```

**Linux/Mac (cron):**
```bash
0 * * * * cd /path/to/SCDType2_Project && python verify_records.py >> verification_log.txt
```

### Pre-Commit Hook

Add to `.git/hooks/pre-commit`:
```bash
#!/bin/bash
cd /path/to/SCDType2_Project
python verify_records.py
```

### Integration with CI/CD

```yaml
# Example GitHub Actions
- name: Verify SCD Sync
  run: |
    python verify_records.py
    if grep -q "need processing" <(python verify_records.py); then
      echo "Warning: SCD sync needed"
    fi
```

---

## Troubleshooting

### Issue: "Database file 'scd2.db' not found!"
**Solution:** Run `python create_database.py` first to create the database

### Issue: Script shows no changes but you know you made updates
**Solution:** Check if you committed your changes with `conn.commit()`

### Issue: Want to see deleted records
**Solution:** This is not part of standard SCD Type 2. Consider adding a status field and using soft deletes instead

---

## Comparison with Original Script

| Feature | Original | Enhanced |
|---------|----------|----------|
| Detect new records | ❌ Hardcoded IDs only | ✅ Automatic detection |
| Detect updates | ❌ No | ✅ Hash-based detection |
| Show changes | ❌ No | ✅ Field-level comparison |
| Historical analysis | ❌ No | ✅ Version tracking |
| Actionable output | ❌ No | ✅ Clear recommendations |
| Reusable | ❌ Needs code changes | ✅ Works with any data |

---

## Related Files

- **scd_type2_process.py** - Main SCD Type 2 processing logic
- **create_database.py** - Database initialization
- **test_scd_type2.py** - Automated test suite
- **demo_detection.py** - Demo script showing detection capabilities

---

## Support

For issues or questions:
1. Check the main [README.md](README.md)
2. Review the [CHANGELOG.md](CHANGELOG.md)
3. Open an issue on GitHub

---

**Last Updated:** January 2026  
**Version:** 2.0
