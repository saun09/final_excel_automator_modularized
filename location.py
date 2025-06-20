import pandas as pd
from rapidfuzz import fuzz
from collections import defaultdict
import re
# Load your Excel file
file_path = "/home/saun/final_excel_automator_modularized/Trade data_Petcoke (1).xlsx"  # 🔁 Replace with your actual file path
df = pd.read_excel(file_path)

# Column to cluster
supplier_column = "Supplier_Name"

# Cleaning function for better comparison
def clean_name(name):
    name = str(name).lower().strip()
    
    # Normalize whitespace and remove special characters
    name = re.sub(r'[^a-z0-9\s]', '', name)
    
    # Remove known suffixes (but only if they're at the end!)
    legal_suffixes = [' limited', ' ltd', ' inc', ' pte', ' co', ' gmbh', ' bvba', ' llc', ' incorporated']
    for suffix in legal_suffixes:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    
    # Clean extra whitespace
    name = re.sub(r'\s+', ' ', name)
    
    return name

# Step 1: Extract and clean all unique names
unique_names = df[supplier_column].dropna().unique()
clusters = []
canonical_names = []

threshold = 90  # Adjust for stricter/looser matching

# Step 2: Dynamic clustering
name_to_cluster = {}

for name in unique_names:
    cleaned = clean_name(name)
    matched = False
    for i, canon in enumerate(canonical_names):
        if fuzz.token_sort_ratio(cleaned, canon) > threshold:
            clusters[i].append(name)
            name_to_cluster[name] = canonical_names[i]
            matched = True
            break
    if not matched:
        clusters.append([name])
        canonical_names.append(cleaned)
        name_to_cluster[name] = cleaned

# Step 3: Map original names to canonical clusters in DataFrame
df["Clustered_Supplier_Name"] = df[supplier_column].map(name_to_cluster)

# Step 4: Save the result to a new Excel file
output_path = "clustered_suppliers.xlsx"
df.to_excel(output_path, index=False)
print(f"Clustered supplier names saved to: {output_path}")
