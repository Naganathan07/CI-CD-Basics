import sqlite3
import pandas as pd
import json

# ----------------------------
# CONFIG: CSV and SQLite paths
# ----------------------------
CSV_FILE = r"C:\Users\admin\Downloads\product_new.csv"
DB_FILE = r"C:\Users\admin\Downloads\db_14_10_BEFORE_UPDATED_IN_SHARAT\db_14_10_BEFORE_UPDATED_IN_SHARAT.sqlite3"
TABLE_NAME = "form_generator_filledformdata"

# ----------------------------
# LOAD CSV
# ----------------------------
df = pd.read_csv(CSV_FILE)

# ----------------------------
# CLEAN AND VALIDATE COLUMNS
# ----------------------------

# Drop rows missing mandatory columns
df = df.dropna(subset=['formId', 'organization_id'])

# Convert numeric columns
df['formId'] = pd.to_numeric(df['formId'], errors='coerce').astype(int)
df['organization_id'] = pd.to_numeric(df['organization_id'], errors='coerce').astype(int)
df['core_filled_data'] = pd.to_numeric(df['core_filled_data'], errors='coerce').fillna(0).astype(int)
df['is_enabled'] = pd.to_numeric(df['is_enabled'], errors='coerce').fillna(0).astype(int)

# Fill missing optional columns
optional_cols = ['status', 'caseId_id', 'processId_id', 'userId_id', 'created_at', 'updated_at']
for col in optional_cols:
    if col not in df.columns:
        df[col] = ''
    else:
        df[col] = df[col].fillna('')

# ----------------------------
# CONNECT TO SQLITE
# ----------------------------
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# ----------------------------
# INSERT ROWS
# ----------------------------
for i, row in df.iterrows():
    data_json = row['data_json']
    
    # Ensure valid JSON
    try:
        parsed_json = json.loads(data_json)
        data_json = json.dumps(parsed_json, ensure_ascii=False)
    except:
        data_json = "[]"  # fallback empty JSON array

    try:
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME} (
                formId, data_json, updated_at, status, caseId_id, organization_id,
                processId_id, userId_id, core_filled_data, is_enabled, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['formId'], data_json, row['updated_at'], row['status'], row['caseId_id'],
            row['organization_id'], row['processId_id'], row['userId_id'],
            row['core_filled_data'], row['is_enabled'], row['created_at']
        ))
    except Exception as e:
        print(f"Error inserting row {i}: {e}")

# ----------------------------
# COMMIT & CLOSE
# ----------------------------
conn.commit()
conn.close()
print("CSV data inserted successfully into SQLite!")
