import pandas as pd
import re
from difflib import SequenceMatcher


def extract_core_product_name(text):
    """Extract the core product name by preserving important product codes"""
    if pd.isna(text):
        return ""
    
    original_text = str(text).strip()
    text = original_text.lower()
    
    # First, extract important product codes before removing parentheses
    # Look for patterns like (ar-740), (ar-825h), (pq0015066), etc.
    product_codes = []
    
    # Extract alphanumeric codes with hyphens (like ar-740, ar-825h)
    code_matches = re.findall(r'\(([a-z]{2,3}-?\d+[a-z]*)\)', text)
    product_codes.extend(code_matches)
    
    # Extract other product codes (like pq0015066)
    other_codes = re.findall(r'\(([a-z]{2}\d+)\)', text)
    product_codes.extend(other_codes)
    
    # Remove descriptions in parentheses but keep the main text structure
    text = re.sub(r'\([^)]*\)', '', text)
    
    # Clean up extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Extract the base product name (like "acm", "lipolan f", etc.)
    base_name = ""
    
    # Try to match common patterns
    if re.match(r'^[a-z]+\s*[a-z]*', text):
        # Extract first 1-2 words as base name
        words = text.split()
        if len(words) >= 2:
            base_name = f"{words[0]} {words[1]}"
        else:
            base_name = words[0] if words else ""
    else:
        base_name = text
    
    # Combine base name with the most specific product code
    if product_codes:
        # Prioritize codes with hyphens and letters (more specific)
        specific_codes = [code for code in product_codes if '-' in code and any(c.isalpha() for c in code)]
        if specific_codes:
            return f"{base_name} {specific_codes[0]}"
        else:
            return f"{base_name} {product_codes[0]}"
    
    return base_name


def similarity_score(str1, str2):
    """Calculate similarity between two strings"""
    return SequenceMatcher(None, str1, str2).ratio()


def cluster_product_names(series, similarity_threshold=0.8):
    """Cluster similar product names together with better product code handling"""
    if series.empty:
        return pd.Series([], dtype=str)
    
    # Get unique values and their core names
    unique_values = series.dropna().unique()
    core_names = {val: extract_core_product_name(val) for val in unique_values}
    
    # Create direct mapping - each unique core name becomes a cluster
    clusters = {}
    
    for val, core in core_names.items():
        if core and core.strip():  # Only process non-empty core names
            clusters[val] = core
        else:
            # Fallback for items without clear core names
            clusters[val] = str(val).lower().strip()
    
    # Map the series values to cluster names
    return series.map(lambda x: clusters.get(x, str(x).lower().strip() if pd.notna(x) else x))


def add_cluster_column(df, column_name):
    """Add a cluster column for the specified column"""
    if column_name not in df.columns:
        return df
    
    df_copy = df.copy()
    cluster_col_name = f"{column_name}_cluster"
    df_copy[cluster_col_name] = cluster_product_names(df_copy[column_name])
    
    return df_copy