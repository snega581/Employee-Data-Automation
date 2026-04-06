import pandas as pd
import sqlite3
from datetime import datetime
import os

# --- Configuration ---
DATABASE_NAME = "employee_automation_system.db"
CSV_FILE = "employee_dataset.csv"


def init_db():
    """Initializes the SQLite database."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS employee_data (
            emp_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            department TEXT,
            salary REAL,
            sync_time DATETIME
        )
    ''')
    conn.commit()
    return conn


def create_sample_csv():
    """Generates data automatically so the project always works."""
    data = {
        'emp_id': [5001, 5002, 5003, 5004, 5005],
        'name': ['Alice Johnson', 'Bob Smith', 'Charlie Davis', 'Diana Prince', 'Invalid Entry'],
        'email': ['ALICE@company.com', 'bob@company.com', 'charlie@company.com', 'DIANA@company.com', 'test@com'],
        'department': ['Engineering', 'Marketing', 'Sales', 'HR', 'None'],
        'salary': [95000, 72000, 68000, 89000, -500]
    }
    pd.DataFrame(data).to_csv(CSV_FILE, index=False)
    print(f"Generated sample file: {CSV_FILE}")


def run_automation():
    """Extract, Transform, and Load (ETL) process."""
    print("Initializing Automation Pipeline...")

    if not os.path.exists(CSV_FILE):
        create_sample_csv()

    # 1. Extraction
    df = pd.read_csv(CSV_FILE)

    # 2. Transformation (Cleaning)
    df = df[df['salary'] > 0]  # Remove negative salaries
    df['email'] = df['email'].str.lower()  # Standardize email
    df = df.drop_duplicates(subset='emp_id')  # Remove duplicates

    # 3. Loading (SQL)
    conn = init_db()
    cursor = conn.cursor()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO employee_data (emp_id, name, email, department, salary, sync_time)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(emp_id) DO UPDATE SET
                salary = excluded.salary,
                sync_time = excluded.sync_time
        ''', (row['emp_id'], row['name'], row['email'], row['department'], row['salary'], current_time))

    conn.commit()
    conn.close()
    print(f"Success! Data synced to {DATABASE_NAME}")


if __name__ == "__main__":
    run_automation()