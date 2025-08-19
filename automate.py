# Data Cleaning Master - Advanced Version

import pandas as pd
import numpy as np
import time
import openpyxl
import xlrd
import os
import random
from sklearn.preprocessing import MinMaxScaler

def data_cleaning_master(data_path, data_name):

    print("Thank you for giving the details!")
    sec = random.randint(1, 4)
    print(f"Please wait for {sec} seconds! Checking file path")
    time.sleep(sec)
    
    # checking if the path exists
    if not os.path.exists(data_path):
        print("Incorrect path! Try again with correct path")
        return
    else:
        # checking the file type
        try:
            if data_path.endswith('.csv'):
                print('Dataset is csv!')
                try:
                    data = pd.read_csv(data_path, encoding='utf-8', encoding_errors='ignore')
                except:
                    data = pd.read_csv(data_path, encoding='latin1', encoding_errors='ignore')
            
            elif data_path.endswith('.xlsx'):
                print('Dataset is excel file!')
                data = pd.read_excel(data_path)
            else:
                print("Unknown file type")
                return
        except Exception as e:
            print(f"Error while reading file: {e}")
            return

    sec = random.randint(1, 4)
    print(f"Please wait for {sec} seconds! Checking total columns and rows")
    time.sleep(sec)
            
    print(f"Dataset contain total rows: {data.shape[0]} \n Total Columns: {data.shape[1]}")

    # ----------------- New Feature: Clean column names -----------------
    data.columns = (
        data.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^a-zA-Z0-9_]", "", regex=True)
    )
    print("✅ Column names standardized!")

    # ----------------- Duplicates -----------------
    sec = random.randint(1, 4)
    print(f"Please wait for {sec} seconds! Checking total duplicates")
    time.sleep(sec)
    
    duplicates = data.duplicated()
    total_duplicate = duplicates.sum()
    print(f"Datasets has total duplicates records: {total_duplicate}")

    sec = random.randint(1, 4)
    print(f"Please wait for {sec} seconds! Saving duplicate rows")
    time.sleep(sec)

    if total_duplicate > 0:
        duplicate_records = data[duplicates]
        duplicate_records.to_csv(f'{data_name}_duplicates.csv', index=None)

    df = data.drop_duplicates()

    # ----------------- Missing Values -----------------
    sec = random.randint(1, 10)
    print(f"Please wait for {sec} seconds! Checking for missing values")
    time.sleep(sec)

    total_missing_value = df.isnull().sum().sum()
    missing_value_by_colums = df.isnull().sum()

    print(f"Dataset has Total missing values: {total_missing_value}")
    print(f"Missing value count by columns:\n{missing_value_by_colums}")

    # Remove columns with >50% missing values
    threshold = 0.5
    cols_to_drop = df.columns[df.isnull().mean() > threshold]
    if len(cols_to_drop) > 0:
        print(f"⚠️ Dropping columns with >50% missing values: {list(cols_to_drop)}")
        df.drop(columns=cols_to_drop, inplace=True)

    # ----------------- Fill Missing Values -----------------
    sec = random.randint(1, 6)
    print(f"Please wait for {sec} seconds! Cleaning datasets")
    time.sleep(sec)

    for col in df.columns:
        if df[col].dtype in [float, int, np.float64, np.int64]:
            df[col] = df[col].fillna(df[col].mean())
        else:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("nan", np.nan)
            df.dropna(subset=[col], inplace=True)

    # ----------------- Outlier Removal (IQR) -----------------
    for col in df.select_dtypes(include=[np.number]).columns:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower, upper = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
        before = df.shape[0]
        df = df[(df[col] >= lower) & (df[col] <= upper)]
        after = df.shape[0]
        if before != after:
            print(f"✅ Removed {before - after} outliers from column {col}")

    # ----------------- Detect Date Columns -----------------
    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_datetime(df[col], errors='ignore')
            except:
                pass

    # ----------------- Remove Constant Columns -----------------
    constant_cols = [col for col in df.columns if df[col].nunique() == 1]
    if constant_cols:
        print(f"⚠️ Removing constant columns: {constant_cols}")
        df.drop(columns=constant_cols, inplace=True)

    # ----------------- Normalize Categorical -----------------
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.lower().str.strip()

    # ----------------- Optional Scaling -----------------
    scaler = MinMaxScaler()
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
        print("✅ Numeric columns scaled using MinMaxScaler!")

    # ----------------- Save Profiling Report -----------------
    profile = pd.DataFrame({
        "Column": df.columns,
        "DataType": df.dtypes.astype(str),
        "UniqueValues": df.nunique().values,
        "MissingValues": df.isnull().sum().values
    })
    profile.to_csv(f"{data_name}_Data_Profile.csv", index=False)
    print("📊 Data profiling report saved!")

    # ----------------- Final Export -----------------
    sec = random.randint(1, 5)
    print(f"Please wait for {sec} seconds! Exporting datasets")
    time.sleep(sec)

    print(f"🎉 Congrats! Dataset is cleaned! \nNumber of Rows: {df.shape[0]} Number of columns: {df.shape[1]}")

    df.to_csv(f'{data_name}_Clean_data.csv', index=None)
    print("✅ Dataset is saved!")

    return df


if __name__ == "__main__":
    print("Welcome to Data Cleaning Master - Advanced!")
    data_path = input("Please enter dataset path: ")
    data_name = input("Please enter dataset name: ")
    cleaned_data = data_cleaning_master(data_path, data_name)
