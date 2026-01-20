import sqlite3
import os

# Source: https://github.com/NirajV/Project_Gen_AI-Data_Tranformation/blob/main/create_database.py

def create_database():
    """
    Creates a SQLite database and executes the setup_database.sql script.
    """
    db_filename = 'scd2.db'
    sql_filename = 'setup_database.sql'
    
    # Get the absolute path of the current script to locate the SQL file correctly
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(script_dir, sql_filename)
    db_path = os.path.join(script_dir, db_filename)

    if not os.path.exists(sql_path):
        print(f"Error: '{sql_filename}' not found in {script_dir}")
        return

    # Remove existing database to ensure a clean setup
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            print(f"Removed existing database: {db_filename}")
        except OSError as e:
            print(f"Error removing existing database: {e}")
            return

    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print(f"Connected to database: {db_filename}")

        # Read and execute the SQL script
        with open(sql_path, 'r') as f:
            sql_script = f.read()
            
        cursor.executescript(sql_script)
        print(f"Successfully executed {sql_filename}")

        # Commit and close
        conn.commit()
        conn.close()
        print("Database setup complete.")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    create_database()