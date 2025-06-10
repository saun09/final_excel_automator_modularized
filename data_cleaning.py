import pandas as pd
import re
import unicodedata


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
            
            if numeric_count / len(sample_values) > 0.7:  # 70% numeric-like values
                numeric_cols.append(col)
    
    return numeric_cols


def detect_categorical_columns(df, exclude_clusters=True):
    """Detect columns suitable for grouping/categorization"""
    categorical_cols = []
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