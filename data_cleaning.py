import pandas as pd
import re
import unicodedata
import streamlit as st
import pandas as pd
import requests
from typing import Dict, List, Optional, Tuple
import time
from datetime import datetime
from io import BytesIO
def is_email(value):
    """Check if a value is a valid email address."""
    email_pattern = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )
    value = str(value).strip()
    return bool(email_pattern.match(value))


def detect_string_columns(df):
    """Detect columns that contain string data (excluding emails)."""
    string_cols = []
    for col in df.columns:
        series = df[col].dropna()
        # Filter strings
        string_values = series[series.apply(lambda x: isinstance(x, str))]
        if not string_values.empty:
            # Exclude if any looks like email
            if not string_values.map(is_email).any():
                string_cols.append(col)
        # Check if column has any string with alphabetic char
        has_text = series.astype(str).apply(lambda x: any(c.isalpha() for c in x)).any()
        # Exclude columns that contain emails
        contains_email = series.astype(str).map(is_email).any()
        # Exclude numeric-only columns
        is_numeric = pd.api.types.is_numeric_dtype(df[col])
        if has_text and not contains_email and not is_numeric:
            string_cols.append(col)
    return string_cols


def detect_numeric_columns(df):
    """Detect columns that likely contain numeric data (quantities, prices, etc.)"""
    numeric_cols = []
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            numeric_cols.append(col)
        else:
            # Check if column contains numeric-like strings
            sample_values = df[col].dropna().astype(str).head(100)
            numeric_count = 0
            for val in sample_values:
                # Remove common non-numeric characters and check if remainder is numeric
                cleaned = re.sub(r'[,$\s]', '', str(val))
                try:
                    float(cleaned)
                    numeric_count += 1
                except:
                    pass

            if len(sample_values) == 0:  # Prevent division by zero
                continue
            
            if numeric_count / len(sample_values) > 0.7:  # 70% numeric-like values
                numeric_cols.append(col)
    
    return numeric_cols


def detect_categorical_columns(df, exclude_clusters=True):
    """Detect columns suitable for grouping/categorization"""
    categorical_cols = []
    if len(df) == 0:  # Prevent ZeroDivisionError
        return categorical_cols
    for col in df.columns:
        if exclude_clusters and '_cluster' in col:
            continue
        
        # Skip numeric columns
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
            
        # Check unique value ratio
        unique_ratio = df[col].nunique() / len(df)
        
        # Good categorical columns have reasonable number of unique values
        if 0.01 <= unique_ratio <= 0.3:  # Between 1% and 30% unique values
            categorical_cols.append(col)
    
    return categorical_cols


def safe_numeric_conversion(series):
    """Safely convert a series to numeric, handling common formats"""
    def convert_value(val):
        if pd.isna(val):
            return 0
        
        val_str = str(val).strip()
        
        # Remove common non-numeric characters
        cleaned = re.sub(r'[,$\s]', '', val_str)
        
        try:
            return float(cleaned)
        except:
            return 0
    
    return series.apply(convert_value)

def drop_unwanted_columns(df):
    """
    Drops unwanted columns from the dataframe regardless of casing.
    """

    # Define columns to drop (in any case)
    unwanted = [
        'CUSH', 'MODE OF SHIPMENT', 'AG', 'Total_Duty_Paid', 'Supplier_Address',
        'IEC', 'Importer_Name', 'Importer_Address', 'Importer_PIN',
        'Importer_Phone', 'Importer_mail', 'Importer_Contact_Person_1',
        'Importer_Contact_Person_2', 'IEC_Date_of_Establishment'
    ]

    # Lowercase the dataframe's columns
    df_cols_lower = [col.lower() for col in df.columns]

    # Map original column names to lowercase
    col_mapping = dict(zip(df_cols_lower, df.columns))

    # Prepare lowercase version of unwanted columns
    unwanted_lower = [col.lower() for col in unwanted]

    # Get actual column names to drop from original df
    cols_to_drop = [col_mapping[col] for col in unwanted_lower if col in col_mapping]
    unnamed_cols = [col for col in df.columns if col.strip().lower().startswith('unnamed:')]

    # Drop all identified columns
    df_cleaned = df.drop(columns=cols_to_drop + unnamed_cols)


    return df_cleaned

def clean_pin(value):
    """Clean PIN codes by removing prefixes and extracting 6-digit codes."""
    if pd.isna(value):
        return value
    value = str(value)
    # Remove "pin-" prefix, case-insensitive
    value = re.sub(r'pin-', '', value, flags=re.IGNORECASE).strip()
    # Extract first group of 6 digits
    match = re.search(r'\b(\d{6})\b', value)
    return match.group(1) if match else value

def remove_commas_and_periods(value):
    """Remove commas and full stops from a string."""
    if pd.isna(value):
        return value
    return re.sub(r'[.,]', '', str(value))



def standardize_value(val, col_name=""):
    """Standardize a single value for better matching and clustering."""
    if pd.isna(val):
        return val
    
    val_str = str(val)

    if val_str.strip() == "":
        return val_str
    
    if "pin" in col_name.lower():
        return clean_pin(val)

    val_str = unicodedata.normalize('NFKD', val_str).encode('ascii', 'ignore').decode('utf-8')
    val_str = val_str.lower()
    val_str = val_str.strip()
    val_str = re.sub(r'\s+', ' ', val_str)
    val_str = remove_commas_and_periods(val_str)

    return val_str


def standardize_dataframe(df, string_cols):
    """Standardize string columns in a DataFrame."""
    df = df.copy()
    for col in string_cols:
        df[col] = df[col].apply(lambda x: standardize_value(x, col_name=col))
    return df


def convert_df_to_csv_bytes(df):
    """Convert DataFrame to CSV bytes for download."""
    return df.to_csv(index=False).encode('utf-8')
UNIT_CONVERSIONS_TO_KG = {
    "g": 0.001,
    "gram": 0.001,
    "grams": 0.001,
    "mg": 0.000001,
    "ton": 1000,
    "tons": 1000,
    "quintal": 100,
    "lb": 0.453592,
    "lbs": 0.453592,
    "pound": 0.453592,
    "ounces": 0.0283495,
    "oz": 0.0283495,
    "kg": 1,
    "kgs": 1,
    "mts": 1000,
    "metric ton": 1000
}


def is_convertible_unit(unit):
    """Returns True if the unit is convertible to kg."""
    unit = str(unit).lower().strip()
    return unit in UNIT_CONVERSIONS_TO_KG

def extract_numeric_quantity(val):
    """Extract leading numeric value from a string (e.g., '2 pcs' -> 2.0)."""
    if pd.isna(val):
        return None
    match = re.match(r'^\s*(\d+(?:\.\d+)?)', str(val))
    return float(match.group(1)) if match else None

def convert_to_kg(df, quantity_col="Quantity", unit_col="UQC"):
    changed_rows = []
    rows_to_delete = []

    for idx, row in df.iterrows():
        raw_unit = row[unit_col]
        raw_quantity = row[quantity_col]

        unit = standardize_value(raw_unit, unit_col)
        quantity = extract_numeric_quantity(raw_quantity)

        if pd.isna(unit) or unit not in UNIT_CONVERSIONS_TO_KG or quantity is None:
            rows_to_delete.append({
                "Index": idx,
                "Original Unit": raw_unit,
                "Original Quantity": raw_quantity
            })
            continue

        if unit in ["kg", "kgs"]:
            continue

        factor = UNIT_CONVERSIONS_TO_KG[unit]
        new_quantity = quantity * factor

        changed_rows.append({
            "Index": idx,
            "Original Unit": raw_unit,
            "Original Quantity": raw_quantity,
            "Converted Quantity (kg)": new_quantity
        })

        df.at[idx, quantity_col] = new_quantity
        df.at[idx, unit_col] = "kgs"

    df = df.drop(index=[r["Index"] for r in rows_to_delete])

    return df, changed_rows, rows_to_delete


import requests
import pandas as pd

base_url = "https://marketdata.tradermade.com/api/v1/convert"
api_key = "-eRFVM6ugO_vKeHx0_Yu"

def convert_currency(amount, from_currency, to_currency):
    if from_currency == to_currency:
        return 1.0, amount

    try:
        url = f"{base_url}?api_key={api_key}&from={from_currency}&to={to_currency}&amount={amount}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            rate = data.get("quote")
            converted_amount = data.get("total")
            return rate, converted_amount
        return None, None
    except Exception:
        return None, None

def fetch_supported_currencies():
    try:
        url = f"https://marketdata.tradermade.com/api/v1/live_currencies_list?api_key={api_key}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            currencies_data = response.json()
            if "available_currencies" in currencies_data:
                currencies = currencies_data["available_currencies"]
                return list(currencies.keys())
        return None
    except Exception:
        return None

def convert_sheet_to_usd(df, currency_col, value_cols, progress_callback=None, status_callback=None, warning_callback=None, success_callback=None):
    df_result = df.copy()
    rate_cache = {}
    total_rows = len(df)

    for idx, row in df.iterrows():
        currency = str(row[currency_col]).strip().upper()

        
        progress = min((idx + 1) / total_rows, 1.0)
        if progress_callback:
            progress_callback(progress)

        if status_callback:
            status_callback(f"Processing row {idx + 1} of {total_rows}")

        if not currency or currency == 'USD' or currency == 'NAN':
            if currency == 'USD':
                for col in value_cols:
                    try:
                        value = float(row[col])
                        df_result.at[idx, f"{col}_USD"] = value
                    except:
                        df_result.at[idx, f"{col}_USD"] = None
            continue

        if currency in rate_cache:
            rate = rate_cache[currency]
        else:
            rate, _ = convert_currency(1, currency, "USD")
            rate_cache[currency] = rate

        for col in value_cols:
            try:
                value = row[col]
                if pd.isna(value) or value == '':
                    df_result.at[idx, f"{col}_USD"] = None
                    continue
                value = float(value)
                if rate is not None:
                    converted_value = value * rate
                    df_result.at[idx, f"{col}_USD"] = round(converted_value, 4)
                else:
                    df_result.at[idx, f"{col}_USD"] = None
            except (ValueError, TypeError) as e:
                if warning_callback:
                    warning_callback(f"Row {idx + 1}, Column {col}: Invalid value - {str(e)}")
                df_result.at[idx, f"{col}_USD"] = None

    if success_callback:
        success_callback(f"Conversion completed! Processed {total_rows} rows.")
    return df_result

def get_conversion_rate(from_currency, to_currency="USD"):
    rate, _ = convert_currency(1, from_currency, to_currency)
    return rate


def convert_month_column_to_datetime(df):
    """
    Converts various messy date formats in the 'Month' column to datetime (e.g., 2020-04-01).
    Supported formats:
    - Apr--2020, June--2020
    - June-2020, Aug-19, july-19, etc.
    - Jun/20, July 2020, etc.
    Replaces original 'Month' column with standardized datetime objects.
    """

    def parse_date(val):
        val = str(val).strip().lower()
        val = re.sub(r'[^a-z0-9]', '-', val)  # replace all non-alphanum with dashes
        val = re.sub(r'-+', '-', val)         # collapse repeated dashes
        
        # Try full month name first (e.g., June-2020)
        try:
            return datetime.strptime(val, "%B-%Y")
        except:
            pass

        # Try short month name + full year (e.g., Jun-2020)
        try:
            return datetime.strptime(val, "%b-%Y")
        except:
            pass

        # Try short/full month + 2-digit year (e.g., Jun-20 or July-19)
        try:
            return datetime.strptime(val, "%b-%y")
        except:
            pass

        try:
            return datetime.strptime(val, "%B-%y")
        except:
            pass

        return pd.NaT  # fallback

    if "Month" in df.columns:
        df["Month"] = df["Month"].apply(parse_date)
    return df

import re
from rapidfuzz import fuzz

def clean_supplier_name(name):
    """
    Cleans supplier names by removing common suffixes and special characters.
    """
    name = str(name).lower().strip()
    name = re.sub(r'[^a-z0-9\s]', '', name)

    suffixes = [' limited', ' ltd', ' inc', ' pte', ' co', ' gmbh', ' bvba', ' llc', ' incorporated']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[: -len(suffix)]

    name = re.sub(r'\s+', ' ', name)
    return name.strip()


def cluster_supplier_names(df, supplier_column="Supplier_Name", threshold=90):
    """
    Clusters similar supplier names using fuzzy matching and replaces the original column.
    """
    if supplier_column not in df.columns:
        return df

    unique_names = df[supplier_column].dropna().unique()
    clusters = []
    canonical_names = []
    name_to_cluster = {}

    for name in unique_names:
        cleaned = clean_supplier_name(name)
        matched = False
        for i, canon in enumerate(canonical_names):
            if fuzz.token_sort_ratio(cleaned, canon) > threshold:
                clusters[i].append(name)
                name_to_cluster[name] = canon  # Use canonical cluster name
                matched = True
                break
        if not matched:
            clusters.append([name])
            canonical_names.append(cleaned)
            name_to_cluster[name] = cleaned

    df[supplier_column] = df[supplier_column].map(name_to_cluster).fillna(df[supplier_column])
    return df
