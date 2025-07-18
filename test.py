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
    WHERE DATE(created) = '2025-07-17' AND pg_name LIKE 'JODETX%'
""")
df_ismart = pd.DataFrame(cursor.fetchall())
print(f"✅ Step 2 complete: Fetched {len(df_ismart)} rows from `ismart_create_order`.\n")

# Step 3: Fetch transaction_details data
print("Step 3️⃣: Fetching data from `transaction_details`...")
cursor.execute("""
    SELECT id, created, orderid, amount, status
    FROM transaction_details
    WHERE DATE(created) = '2025-07-17' AND pg_type LIKE 'JODETX%' AND status = 'FAILED'
""")
df_transaction = pd.DataFrame(cursor.fetchall())
print(f"✅ Step 3 complete: Fetched {len(df_transaction)} rows from `transaction_details`.\n")

# Step 4: Normalize column names
print("Step 4️⃣: Normalizing column names...")
df_ismart.columns = df_ismart.columns.astype(str).str.strip().str.lower()
df_transaction.columns = df_transaction.columns.astype(str).str.strip().str.lower()
print("✅ Step 4 complete: Column names normalized.\n")

# Step 5: Extract callback_status from callback_json
print("Step 5️⃣: Extracting `callback_status` from JSON...")

def extract_status(callback_json):
    try:
        if not callback_json or not isinstance(callback_json, str):
            return None
        data = json.loads(callback_json)

        # Priority: response_message.result.status
        nested_status = data.get("response_message", {}).get("result", {}).get("status")
        if isinstance(nested_status, str):
            return nested_status

        # Fallback: top-level status
        top_status = data.get("status")
        if isinstance(top_status, str):
            return top_status

    except (json.JSONDecodeError, TypeError):
        return None
    return None

df_ismart["callback_status"] = df_ismart["callback_json"].apply(extract_status)
print("✅ Step 5 complete: Callback status extracted.\n")

# Optional debug print
if not df_ismart["callback_json"].dropna().empty:
    print("🔍 Example JSON parsing result:")
   # print("Raw JSON:", df_ismart["callback_json"].iloc[0])
    print("Parsed Status:", extract_status(df_ismart["callback_json"].iloc[0]))

# Step 6: Merge data
print("Step 6️⃣: Merging transaction and order data...")
df_merged = pd.merge(
    df_transaction,
    df_ismart,
    how='left',
    left_on='orderid',
    right_on='order_id'
)
print(f"✅ Step 6 complete: Merged into {len(df_merged)} rows.\n")
# Safely convert amount to numeric before summing

# Step 7: Convert amount to rupees
print("Step 7️⃣: Converting amount from paisa to rupees...")
df_merged["amount_rs"] = df_merged["amount_x"] / 100
print("✅ Step 7 complete: Conversion done.\n")

# Step 8: Prepare final DataFrame
print("Step 8️⃣: Preparing final result DataFrame...")
df_result = df_merged[["orderid", "status", "callback_status", "amount_rs", "created_x"]]
df_result.columns = ["TransactionID", "TransactionStatus", "CallbackStatus", "Amount(Rs)", "CreatedDate"]
print("✅ Step 8 complete: Final DataFrame ready.\n")

# Step 9: Filter rows where CallbackStatus is not SUCCESS
print("Step 9️⃣: Filtering records where CallbackStatus = 'SUCCESS'...")
df_filtered = df_result[
    (df_result["CallbackStatus"] == "SUCCESS")
].copy()
print(f"✅ Step 9 complete: Found {len(df_filtered)} records.\n")

# Step 🔟: Save filtered data to CSV
print("Step 🔟: Saving filtered transactions to CSV...")
###########Need to uncomment ###########
#df_filtered.to_csv("non_success_callback_transactions.csv", index=False)
print("✅ Step 10 complete: Saved to 'non_success_callback_transactions.csv'.\n")

# Final Report
total_count = len(df_filtered)
total_amount = df_filtered["Amount(Rs)"].sum()

print("📊 FINAL REPORT")


print(f"❌ Total transactions with CallbackStatus NOT SUCCESS: {total_count}")
print(f"💰 Total Amount of those transactions: ₹{total_amount:.2f}\n")

print("🧾 Top 10 Mismatched Transactions Preview:")
print(df_filtered.head(10))
