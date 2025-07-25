import mysql.connector
import csv

# --- Step 1: MySQL DB connection config ---
DB_CONFIG = {
    "host": "localhost",         # e.g., "localhost" or "127.0.0.1"
    "port": 3306,                # default MySQL port
    "user": "pg_user",
    "password": "Dst0ao88lfxHnkHLkQd5Hf",
    "database": "payoutdb"
}

# --- Step 2: Date range ---
START_DATE = '2025-07-01'
END_DATE = '2025-07-24 23:59:59'

# --- Step 3: Output file ---
CSV_FILE = "Payout_transaction_details_2025_07_01_to_2025_07_24.csv"

try:
    # Connect to MySQL
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Prepare query
    query = """
        SELECT * FROM TransactionDetails
        WHERE created BETWEEN %s AND %s
    """
    cursor.execute(query, (START_DATE, END_DATE))

    # Fetch data and headers
    rows = cursor.fetchall()
    headers = [i[0] for i in cursor.description]

    # Write to CSV
    with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"✅ Export complete. CSV saved as: {CSV_FILE}")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
