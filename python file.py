import os
import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection string
DB_URL = "postgresql://neondb_owner:npg_8XWa1xJcIspF@ep-lingering-rice-a12ash0h-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

# Folder containing CSV files
CSV_FOLDER = "C:/Users/vaish/OneDrive/Desktop/aditya/csv-files"

def load_and_clean_csvs(folder_path):
    all_data = []
    
    # Loop through each CSV file in the folder
    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            
            # Clean the data
            df = df.dropna()  # Remove missing values
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")  # Standardize column names
            
            all_data.append(df)

    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    return combined_df

def store_to_postgres(df, table_name="combined_data"):
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        df.to_sql(table_name, con=conn, if_exists="replace", index=False)
        print(f"Data successfully stored in PostgreSQL table: {table_name}")

# Load, clean, and combine CSV files
df_cleaned = load_and_clean_csvs(CSV_FOLDER)

# Store into PostgreSQL
store_to_postgres(df_cleaned)
