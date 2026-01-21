"""
Enhanced SCD Type 2 Verification Script

This script automatically detects:
- New records in source not yet in CDC
- Updated records with hash mismatches
- Recent changes and historical tracking
- Comprehensive status dashboard
"""

import sqlite3
import hashlib
from datetime import datetime
from typing import List, Tuple, Dict


def calculate_row_hash(row: Tuple) -> str:
    """Calculate hash for a row (excluding ID at index 0)."""
    data_str = "|".join(str(x) for x in row[1:])
    return hashlib.sha256(data_str.encode('utf-8')).hexdigest()


def get_source_records(cursor: sqlite3.Cursor) -> Dict[int, Tuple]:
    """Get all source records with their hashes."""
    cursor.execute("SELECT * FROM sales_records_current")
    records = {}
    for row in cursor.fetchall():
        record_id = row[0]
        row_hash = calculate_row_hash(row)
        records[record_id] = {
            'data': row,
            'hash': row_hash
        }
    return records


def get_cdc_active_records(cursor: sqlite3.Cursor) -> Dict[int, Tuple]:
    """Get all active CDC records with their hashes."""
    cursor.execute("SELECT * FROM sales_records_cdc WHERE is_current = 1")
    records = {}
    for row in cursor.fetchall():
        record_id = row[0]
        records[record_id] = {
            'data': row,
            'hash': row[10]  # row_hash column
        }
    return records


def detect_new_records(source: Dict, cdc: Dict) -> List[int]:
    """Detect records in source but not in CDC."""
    return sorted([rid for rid in source.keys() if rid not in cdc.keys()])


def detect_updated_records(source: Dict, cdc: Dict) -> List[Tuple]:
    """Detect records with hash mismatches."""
    updated = []
    for rid in source.keys():
        if rid in cdc and source[rid]['hash'] != cdc[rid]['hash']:
            updated.append((rid, source[rid]['data'], cdc[rid]['data']))
    return updated


def get_recent_changes(cursor: sqlite3.Cursor, limit: int = 5) -> List[Tuple]:
    """Get recently modified records."""
    cursor.execute("""
        SELECT id, product_name, row_start_date, is_current,
               (SELECT COUNT(*) FROM sales_records_cdc c2 
                WHERE c2.id = c1.id) as version_count
        FROM sales_records_cdc c1
        ORDER BY row_start_date DESC
        LIMIT ?
    """, (limit,))
    return cursor.fetchall()


def get_historical_records(cursor: sqlite3.Cursor) -> int:
    """Count historical (expired) records."""
    cursor.execute("SELECT COUNT(*) FROM sales_records_cdc WHERE is_current = 0")
    return cursor.fetchone()[0]


def compare_record_fields(old_row: Tuple, new_row: Tuple) -> List[str]:
    """Compare two records and show what changed."""
    fields = ['id', 'transaction_date', 'product_name', 'category', 'price', 
              'quantity', 'total_amount', 'customer_id', 'region', 'status']
    changes = []
    
    for i, field in enumerate(fields):
        if i < len(old_row) and i < len(new_row):
            if old_row[i] != new_row[i]:
                changes.append(f"{field}: {old_row[i]} → {new_row[i]}")
    
    return changes


def print_section(title: str, symbol: str = "="):
    """Print a formatted section header."""
    print(f"\n{symbol * 60}")
    print(f"{title}")
    print(symbol * 60)


def main():
    """Main verification function."""
    try:
        conn = sqlite3.connect('scd2.db')
        cursor = conn.cursor()
        
        print_section("SCD TYPE 2 VERIFICATION REPORT", "=")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Get data
        source_records = get_source_records(cursor)
        cdc_records = get_cdc_active_records(cursor)
        historical_count = get_historical_records(cursor)
        
        # Summary Statistics
        print_section("📊 SUMMARY STATISTICS", "-")
        print(f"Source Records:           {len(source_records)}")
        print(f"Active CDC Records:       {len(cdc_records)}")
        print(f"Historical CDC Records:   {historical_count}")
        print(f"Total CDC Records:        {len(cdc_records) + historical_count}")
        
        # Detect new records
        new_records = detect_new_records(source_records, cdc_records)
        print_section(f"🆕 NEW RECORDS (Not in CDC): {len(new_records)}", "-")
        
        if new_records:
            print("\n⚠️  These records exist in SOURCE but NOT in CDC:")
            for rid in new_records:
                data = source_records[rid]['data']
                print(f"   ID {rid}: {data[2]} (Category: {data[3]}, Price: ${data[4]})")
            print("\n💡 Action Required: Run 'python scd_type2_process.py' to insert these records")
        else:
            print("✅ No new records found - all source records exist in CDC")
        
        # Detect updated records
        updated_records = detect_updated_records(source_records, cdc_records)
        print_section(f"🔄 UPDATED RECORDS (Hash Mismatch): {len(updated_records)}", "-")
        
        if updated_records:
            print("\n⚠️  These records have been modified in SOURCE:")
            for rid, src_data, cdc_data in updated_records:
                print(f"\n   ID {rid}: {src_data[2]}")
                changes = compare_record_fields(cdc_data[:10], src_data[:10])
                for change in changes:
                    print(f"      • {change}")
            print("\n💡 Action Required: Run 'python scd_type2_process.py' to create new versions")
        else:
            print("✅ No updates detected - all active CDC records match source")
        
        # Synchronized records
        synchronized = len(source_records) - len(new_records) - len(updated_records)
        print_section(f"✅ SYNCHRONIZED RECORDS: {synchronized}", "-")
        print(f"These {synchronized} records are up-to-date in both SOURCE and CDC")
        
        # Recent changes
        print_section("📜 RECENT CHANGES (Last 5 CDC Operations)", "-")
        recent = get_recent_changes(cursor, 5)
        
        if recent:
            for rid, product, start_date, is_current, version_count in recent:
                status = "ACTIVE" if is_current else "HISTORICAL"
                versions_text = f"({version_count} version{'s' if version_count > 1 else ''})"
                print(f"   ID {rid}: {product} - {status} {versions_text}")
                print(f"      Started: {start_date}")
        else:
            print("   No records in CDC yet")
        
        # Records with history
        cursor.execute("""
            SELECT id, COUNT(*) as versions
            FROM sales_records_cdc
            GROUP BY id
            HAVING COUNT(*) > 1
            ORDER BY versions DESC
        """)
        multi_version = cursor.fetchall()
        
        if multi_version:
            print_section(f"📚 RECORDS WITH HISTORY: {len(multi_version)}", "-")
            print("These records have multiple versions (showing historical tracking):")
            for rid, version_count in multi_version[:5]:
                print(f"   ID {rid}: {version_count} versions")
        
        # Action Summary
        print_section("⚡ ACTION SUMMARY", "=")
        
        needs_action = len(new_records) + len(updated_records)
        if needs_action > 0:
            print(f"\n🔴 {needs_action} record(s) need processing:")
            if new_records:
                print(f"   • {len(new_records)} new record(s) to insert")
            if updated_records:
                print(f"   • {len(updated_records)} record(s) to update with new version")
            print(f"\n➡️  RUN: python scd_type2_process.py")
        else:
            print("\n🟢 All records are synchronized!")
            print("   No action required - CDC table is up to date")
        
        print("\n" + "=" * 60 + "\n")
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except FileNotFoundError:
        print("❌ Database file 'scd2.db' not found!")
        print("   Run 'python create_database.py' first")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()
