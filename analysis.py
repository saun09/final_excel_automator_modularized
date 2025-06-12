import pandas as pd
import streamlit as st
from data_cleaning import safe_numeric_conversion
import calendar
from dateutil import parser

def group_data(df, group_by_columns, aggregation_rules=None):
    """
    Group data by specified columns with optional aggregations.
    
    Args:
        df: DataFrame to group
        group_by_columns: List of columns to group by
        aggregation_rules: Dictionary of {column: aggregation_function}
                           If None, will default to count aggregation
    
    Returns:
        Grouped DataFrame
    """
    if not group_by_columns:
        return df
    
    # Default to count aggregation if no rules provided
    if aggregation_rules is None:
        aggregation_rules = {'__count__': 'size'}
    
    try:
        grouped_df = df.groupby(group_by_columns).agg(aggregation_rules).reset_index()
        return grouped_df
    except Exception as e:
        st.error(f"Error during grouping: {str(e)}")
        return df


def perform_cluster_analysis(df, cluster_col, analysis_type, target_col=None, group_by_col=None, selected_clusters=None):
    """Perform various types of analysis on clustered data"""
    
    if cluster_col not in df.columns:
        return None, "Cluster column not found"
    
    # Filter by selected clusters if specified
    if selected_clusters:
        df_filtered = df[df[cluster_col].isin(selected_clusters)]
    else:
        df_filtered = df
    
    if df_filtered.empty:
        return None, "No data found for selected clusters"
    
    try:
        if analysis_type == "cluster_summary":
            # Basic cluster summary
            result = df_filtered.groupby(cluster_col).agg({
                cluster_col: 'count'
            }).rename(columns={cluster_col: 'Total_Records'})
            
            if target_col and target_col in df_filtered.columns:
                numeric_data = safe_numeric_conversion(df_filtered[target_col])
                df_temp = df_filtered.copy()
                df_temp[f'{target_col}_numeric'] = numeric_data
                
                summary = df_temp.groupby(cluster_col)[f'{target_col}_numeric'].agg([
                    'sum', 'mean', 'count'
                ]).round(2)
                summary.columns = [f'{target_col}_Total', f'{target_col}_Average', f'{target_col}_Count']
                
                result = pd.concat([result, summary], axis=1)
            
            return result, "Analysis completed successfully"
        
        elif analysis_type == "top_clusters":
            if not target_col or target_col not in df_filtered.columns:
                return None, "Target column required for top clusters analysis"
            
            numeric_data = safe_numeric_conversion(df_filtered[target_col])
            df_temp = df_filtered.copy()
            df_temp[f'{target_col}_numeric'] = numeric_data
            
            result = df_temp.groupby(cluster_col)[f'{target_col}_numeric'].sum().sort_values(ascending=False).head(10)
            result = result.to_frame(f'Total_{target_col}')
            
            return result, "Top clusters analysis completed"
        
        elif analysis_type == "cluster_by_category":
            if not group_by_col or group_by_col not in df_filtered.columns:
                return None, "Group by column required for categorical analysis"
            
            if target_col and target_col in df_filtered.columns:
                numeric_data = safe_numeric_conversion(df_filtered[target_col])
                df_temp = df_filtered.copy()
                df_temp[f'{target_col}_numeric'] = numeric_data
                
                result = df_temp.groupby([cluster_col, group_by_col])[f'{target_col}_numeric'].sum().unstack(fill_value=0)
            else:
                result = df_filtered.groupby([cluster_col, group_by_col]).size().unstack(fill_value=0)
            
            return result, "Categorical analysis completed"
        
        elif analysis_type == "detailed_breakdown":
            if not group_by_col or group_by_col not in df_filtered.columns:
                return None, "Group by column required for detailed breakdown"
            
            result_list = []
            
            for cluster in df_filtered[cluster_col].unique():
                cluster_data = df_filtered[df_filtered[cluster_col] == cluster]
                
                breakdown = cluster_data.groupby(group_by_col).agg({
                    cluster_col: 'count'
                }).rename(columns={cluster_col: 'Record_Count'})
                
                if target_col and target_col in df_filtered.columns:
                    numeric_data = safe_numeric_conversion(cluster_data[target_col])
                    cluster_data_temp = cluster_data.copy()
                    cluster_data_temp[f'{target_col}_numeric'] = numeric_data
                    
                    summary = cluster_data_temp.groupby(group_by_col)[f'{target_col}_numeric'].sum()
                    breakdown[f'Total_{target_col}'] = summary
                
                breakdown['Cluster'] = cluster
                result_list.append(breakdown.reset_index())
            
            if result_list:
                result = pd.concat(result_list, ignore_index=True)
                return result, "Detailed breakdown completed"
            else:
                return None, "No data to analyze"
        
    except Exception as e:
        return None, f"Analysis error: {str(e)}"
    
    return None, "Unknown analysis type"

def normalize(s):
    return str(s).strip().lower()

def filter_trade_data(df, trade_type_col, country_col, supplier_col, 
                      selected_trade_type=None, selected_country=None, selected_supplier=None):
    
    st.write("### 🔎 Selected Filters")
    st.write(f"- Trade Type: `{selected_trade_type}`")
    st.write(f"- Importer Country: `{selected_country}`")
    st.write(f"- Supplier Country: `{selected_supplier}`")

    if selected_trade_type and trade_type_col in df.columns:
        df = df[df[trade_type_col].astype(str).apply(normalize) == normalize(selected_trade_type)]
    if selected_country and country_col in df.columns:
        if "All" not in selected_country:
            normalized_selected = set(normalize(val) for val in selected_country)
            df = df[df[country_col].astype(str).apply(normalize).isin(normalized_selected)]

    if selected_supplier and supplier_col in df.columns:
        if "All" not in selected_supplier:
            normalized_suppliers = set(normalize(val) for val in selected_supplier)
            df = df[df[supplier_col].astype(str).apply(normalize).isin(normalized_suppliers)]


    st.success(f"✅ Filtered data shape: {df.shape}")
    return df

import pandas as pd

def perform_trade_analysis(df, product_col, quantity_col, value_col, importer_col, supplier_col):
    analysis = {}

    if df.empty:
        return {"error": "No data available for analysis."}

    # Top products by quantity
    product_summary = (
        df.groupby(product_col)[quantity_col]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    analysis["Top Products by Quantity"] = product_summary

    # Top suppliers
    supplier_summary = (
        df.groupby(supplier_col)[quantity_col]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    analysis["Top Suppliers"] = supplier_summary

    # Top importers
    importer_summary = (
        df.groupby(importer_col)[quantity_col]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    analysis["Top Importers"] = importer_summary

    # Unit price (value/quantity) by product
    df["unit_price"] = df[value_col] / df[quantity_col]
    unit_price_summary = (
        df.groupby(product_col)["unit_price"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    analysis["Average Unit Price by Product"] = unit_price_summary

    return analysis




def parse_custom_month_format(date_str):
    """
    Converts 'mar--2021' to a datetime object (e.g., 2021-03-01)
    """
    try:
        return pd.to_datetime(date_str.replace('--', '-'), format='%b-%Y')
    except:
        return pd.NaT

def get_fy(date):
    if pd.isnull(date): return None
    if date.month <= 3:
        return f"FY {date.year - 1}-{str(date.year)[-2:]}"
    else:
        return f"FY {date.year}-{str(date.year + 1)[-2:]}"


def full_periodic_analysis(df, date_col, value_col):
    if date_col not in df.columns or value_col not in df.columns:
        return None, "Required columns not found"

    df_clean = df.copy()
    df_clean["_numeric"] = safe_numeric_conversion(df_clean[value_col])

    # Use custom parser for 'Month' column format like 'mar--2021'
    if df_clean[date_col].str.contains('--', na=False).any():
        df_clean["Parsed_Date"] = df_clean[date_col].apply(parse_custom_month_format)
    else:
        df_clean["Parsed_Date"] = pd.to_datetime(df_clean[date_col], errors="coerce")

    df_clean.dropna(subset=["Parsed_Date"], inplace=True)

    df_clean["Month_Period"] = df_clean["Parsed_Date"].dt.to_period("M").astype(str)
    df_clean["Quarter"] = df_clean["Parsed_Date"].dt.to_period("Q").astype(str)
    df_clean["Calendar Year"] = df_clean["Parsed_Date"].dt.year.astype(str)
    df_clean["Financial Year"] = df_clean["Parsed_Date"].apply(get_fy)

    monthly_avg = df_clean.groupby("Month_Period")["_numeric"].mean().reset_index(name="Monthly Avg")
    quarterly_avg = df_clean.groupby("Quarter")["_numeric"].mean().reset_index(name="Quarterly Avg")
    fy_avg = df_clean.groupby("Financial Year")["_numeric"].mean().reset_index(name="FY Avg")
    cy_avg = df_clean.groupby("Calendar Year")["_numeric"].mean().reset_index(name="CY Avg")

    return {
        "Monthly Average": monthly_avg,
        "Quarterly Average": quarterly_avg,
        "Financial Year Average": fy_avg,
        "Calendar Year Average": cy_avg
    }, "✅ All time-based averages computed"