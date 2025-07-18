import mysql.connector
import pandas as pd
import json
from datetime import datetime

print("Step 1️⃣: Connecting to MySQL database...")

# Connect to MySQL Database
connection = mysql.connector.connect(
    host='localhost',
    port=3306,
    user='pg_user',
    password='Dst0ao88lfxHnkHLkQd5Hf',
    database='pgdb'
)
cursor = connection.cursor(dictionary=True)
print("✅ Step 1 complete: Connected to database.\n")

# Step 2: Fetch ismart_create_order data
print("Step 2️⃣: Fetching data from `ismart_create_order`...")
cursor.execute("""
    SELECT id, amount, callback_json, created, order_id
    FROM ismart_create_order
    WHERE DATE(created) = '2025-07-17' and pg_name LIKE 'JODETX%'
""")
df_ismart = pd.DataFrame(cursor.fetchall())
print(f"✅ Step 2 complete: Fetched {len(df_ismart)} rows from `ismart_create_order`.\n")

# Step 3: Fetch transaction_details data
print("Step 3️⃣: Fetching data from `transaction_details`...")
cursor.execute("""
    SELECT id, created, orderid, amount, status
    FROM transaction_details
    WHERE DATE(created) = '2025-07-17' and pg_type like 'JODETX%'
""")
df_transaction = pd.DataFrame(cursor.fetchall())
print(f"✅ Step 3 complete: Fetched {len(df_transaction)} rows from `transaction_details`.\n")

# Step 4: Normalize column names to avoid KeyErrors
print("Step 4️⃣: Normalizing column names...")

if not df_ismart.empty:
    df_ismart.columns = df_ismart.columns.astype(str).str.strip().str.lower()
else:
    print("⚠️ Warning: `df_ismart` is empty. Skipping column normalization.\n")
    print("✅ Normalized ismart columns:", df_ismart.columns.tolist())

if not df_transaction.empty:
    df_transaction.columns = df_transaction.columns.astype(str).str.strip().str.lower()
else:
    print("⚠️ Warning: `df_transaction` is empty. Skipping column normalization.\n")
print("✅ Step 4 complete: Normalized column names (if data was present).\n")
print("✅ Normalized ismart columns:", df_ismart.columns.tolist())



# Step 5: Extract callback_status from callback_json
print("Step 5️⃣: Extracting `callback_status` from JSON...")

def extract_status(callback_json):
    try:
        data = json.loads(callback_json)
        return data.get("response_message", {}).get("result", {}).get("status")
    except Exception:
        return None

if "callback_json" in df_ismart.columns:
    df_ismart["callback_status"] = df_ismart["callback_json"].apply(extract_status)
    print("✅ Step 5 complete: Callback status extracted.\n")
else:
    df_ismart["callback_status"] = None
    print("⚠️ Warning: `callback_json` column not found. Defaulting `callback_status` to None.\n")

# Step 6: Merge transaction and order data
print("Step 6️⃣: Merging transaction and order data...")
print(f"➡️ Columns in df_transaction: {df_transaction.columns.tolist()}")
print(f"➡️ Columns in df_ismart: {df_ismart.columns.tolist()}")

if 'orderid' in df_transaction.columns and 'order_id' in df_ismart.columns:
    df_merged = pd.merge(
        df_transaction,
        df_ismart,
        how='left',
        left_on='orderid',
        right_on='order_id'
    )
    print(f"✅ Step 6 complete: Merged into {len(df_merged)} rows.\n")
else:
    missing = []
    if 'orderid' not in df_transaction.columns:
        missing.append("`orderid` in df_transaction")
    if 'order_id' not in df_ismart.columns:
        missing.append("`order_id` in df_ismart")
    raise KeyError(f"❌ Merge failed: Missing columns -> {', '.join(missing)}.\n")

# Step 7: Convert amount to rupees
print("Step 7️⃣: Converting amount from paisa to rupees...")
df_merged["amount_rs"] = df_merged["amount_x"] / 100
print("✅ Step 7 complete: Conversion done.\n")

# Step 8: Prepare final DataFrame
print("Step 8️⃣: Preparing final result DataFrame...")
df_result = df_merged[["orderid", "status", "callback_status", "amount_rs", "created_x"]]
df_result.columns = ["TransactionID", "TransactionStatus", "CallbackStatus", "Amount(Rs)", "CreatedDate"]
print("✅ Step 8 complete: Final DataFrame ready.\n")

# Step 9: Identify mismatches
print("Step 9️⃣: Identifying mismatches...")
df_result["StatusMismatch"] = df_result["TransactionStatus"] != df_result["CallbackStatus"]
print("✅ Step 9 complete: Mismatches calculated.\n")

# Step 🔟: Calculate Totals
print("Step 🔟: Calculating mismatch totals...")
total_mismatches = df_result["StatusMismatch"].sum()
total_mismatch_amount = df_result[df_result["StatusMismatch"]]["Amount(Rs)"].sum()
print("✅ Step 10 complete: Totals computed.\n")

# Step 11: Save to CSV
print("Step 1️⃣1️⃣: Saving output to CSV...")
df_result.to_csv("mysql_transaction_comparison.csv", index=False)
print("✅ Step 11 complete: Saved to 'mysql_transaction_comparison.csv'\n")

# Final Report
print("📊 FINAL REPORT")
print(f"🚨 Total Mismatched Transactions: {total_mismatches}")
print(f"💰 Total Mismatched Amount (Rs): {total_mismatch_amount:.2f}")
