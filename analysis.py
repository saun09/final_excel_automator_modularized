import pandas as pd
import streamlit as st
from data_cleaning import safe_numeric_conversion
import calendar
from dateutil import parser
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing

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


    st.success(f"Filtered data shape: {df.shape}")
    return df

import pandas as pd
import numpy as np

def perform_trade_analysis(df, product_col, quantity_col, value_col, importer_col, supplier_col):
    results = {}

    try:
        # 1. Which importer country is importing the most from a particular supplier country for the selected product?
        most_importing = df.groupby([importer_col, supplier_col])[value_col].sum().reset_index()
        most_importing = most_importing.sort_values(by=value_col, ascending=False).head(10)
        results["1. Top Importer-Supplier Combinations"] = most_importing

        # 2. What are the top countries exporting for a given product?
        top_exporting = df.groupby(supplier_col)[value_col].sum().reset_index()
        top_exporting = top_exporting.sort_values(by=value_col, ascending=False).head(10)
        results["2. Top Exporting Countries"] = top_exporting

        # 3. What are the top importing cities/states for a given product from a supplier country?
        top_importing_cities = df.groupby([importer_col, supplier_col])[value_col].sum().reset_index()
        top_importing_cities = top_importing_cities.sort_values(by=value_col, ascending=False).head(10)
        results["3. Top Importing Cities/States by Supplier"] = top_importing_cities

        # 4. Is there any country that dominates in export of selected product?
        dominant_export = top_exporting.copy()
        total_export = dominant_export[value_col].sum()
        dominant_export["% Share"] = (dominant_export[value_col] / total_export) * 100
        results["4. Export Dominance Share"] = dominant_export

        # 5. Which supplier country is sending the highest value of the product to particular importer country/city?
        top_supplier_to_importer = df.groupby([supplier_col, importer_col])[value_col].sum().reset_index()
        top_supplier_to_importer = top_supplier_to_importer.sort_values(by=value_col, ascending=False).head(10)
        results["5. Highest Supplier to Importer Values"] = top_supplier_to_importer

        # 6. Has the trade value for the selected HSCode+Item increased or decreased over time?
        if "year_extracted" in df.columns:
            time_col = "year_extracted"
        elif "year" in df.columns:
            time_col = "year"
        elif "Month" in df.columns:
            df["year_temp"] = pd.to_datetime(df["Month"], errors="coerce").dt.year
            time_col = "year_temp"
        else:
            time_col = None

        if time_col:
            trend_df = df.groupby(time_col)[value_col].sum().reset_index()
            trend_df = trend_df.sort_values(by=time_col)
            trend_df["Change"] = trend_df[value_col].diff()
            trend_df["% Change"] = trend_df[value_col].pct_change() * 100
            results["6. Trade Value Trend Over Time"] = trend_df

        # 7. Which supplier country is giving the lowest/highest average value per unit to an importer country?
        df["Unit_Value"] = df[value_col] / df[quantity_col].replace(0, np.nan)
        avg_unit_value = df.groupby([supplier_col, importer_col])["Unit_Value"].mean().reset_index()
        highest_avg = avg_unit_value.sort_values(by="Unit_Value", ascending=False).head(5)
        lowest_avg = avg_unit_value.sort_values(by="Unit_Value", ascending=True).head(5)
        results["7A. Highest Avg Value per Unit"] = highest_avg
        results["7B. Lowest Avg Value per Unit"] = lowest_avg

        # 8. Heatmap: For selected item+HSCode, which importer/supplier pairs show highest trade value
        heatmap_data = df.groupby([importer_col, supplier_col])[value_col].sum().reset_index()
        heatmap_pivot = heatmap_data.pivot(index=importer_col, columns=supplier_col, values=value_col).fillna(0)
        results["8. Importer-Supplier Heatmap Data"] = heatmap_pivot

    except Exception as e:
        results["error"] = f"Trade analysis failed: {str(e)}"

    return results


def analyze_trend(df, trade_type, product_name, selected_years):
    """
    Analyze trend in trade value for a selected product across years.
    """
    try:
        if "item_description" not in df.columns or "cth_hscode" not in df.columns:
            return ""

        if "year_extracted" in df.columns:
            year_col = "year_extracted"
        elif "year" in df.columns:
            year_col = "year"
        elif "Month" in df.columns:
            df["year_temp"] = pd.to_datetime(df["Month"], errors="coerce").dt.year
            year_col = "year_temp"
        else:
            return ""

        item_col = "item_description"
        trade_col = "Type"
        value_col = df.select_dtypes(include="number").columns[0]

        filtered = df[
            (df[trade_col].str.lower() == trade_type.lower())
            & (df[item_col].str.lower() == product_name.lower())
            & (df[year_col].isin(selected_years))
        ]

        if filtered.empty or len(filtered[year_col].unique()) < 2:
            return ""

        trend = filtered.groupby(year_col)[value_col].sum().reset_index()
        trend = trend.sort_values(by=year_col)

        y1, y2 = trend.iloc[0][year_col], trend.iloc[-1][year_col]
        v1, v2 = trend.iloc[0][value_col], trend.iloc[-1][value_col]

        if v2 > v1:
            status = "increased"
        elif v2 < v1:
            status = "decreased"
        else:
            status = "remained constant"

        change = v2 - v1
        percent = (change / v1) * 100 if v1 != 0 else 0
        return f"From {y1} to {y2}, the trade value has **{status}** from **{v1:,.0f}** to **{v2:,.0f}** (change: {percent:.2f}%)."

    except Exception as e:
        return f"Trend analysis failed: {e}"



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
    }," All time-based averages computed"


def analyze_trend(df, trade_type, product_name, selected_years):
    df_filtered = df[
        (df["Type"] == trade_type) &
        (df["Item_Description_cluster"] == product_name) &
        (df["YEAR"].isin(selected_years))
    ]

    if len(selected_years) < 2:
        return "Please select at least two years to perform trend analysis."

    years_sorted = sorted(selected_years)
    year1, year2 = years_sorted[0], years_sorted[-1]

    q1 = df_filtered[df_filtered["YEAR"] == year1]["Quantity"].sum()
    q2 = df_filtered[df_filtered["YEAR"] == year2]["Quantity"].sum()
    diff = q2 - q1

    trend = "increased" if diff > 0 else "decreased"
    trend_type = "growing" if diff > 0 else "declining"

    result = (
        f"From {year1} to {year2}, {trade_type.lower()}s have {trend} by {abs(diff):,.0f} units, "
        f"indicating a {trend_type} trend for the product '{product_name}'.\n\n"
        f"It was {q1:,.0f} units in {year1} and {q2:,.0f} units in {year2}, "
        f"hence it is {trend}."
    )
    return result


def comparative_analysis(df, selected_years, time_period_type, selected_quarter_or_month, selected_hscode, selected_item, quantity_col='Quantity', month_col='Month'):
    import pandas as pd

    # Convert "Month" column to datetime
    df[month_col] = pd.to_datetime(df[month_col], errors='coerce')

    # Extract year and month/quarter
    df['year'] = df[month_col].dt.year
    df['month_num'] = df[month_col].dt.month
    df['quarter'] = df[month_col].dt.quarter

    # Step 1: Filter by selected years
    df_filtered = df[df['year'].isin(selected_years)]

    # Step 2: Filter by time period
    if time_period_type.lower() == 'quarter':
        quarter_map = {'Q1': 1, 'Q2': 2, 'Q3': 3, 'Q4': 4}
        selected_q = quarter_map[selected_quarter_or_month.upper()]
        df_filtered = df_filtered[df_filtered['quarter'] == selected_q]

    elif time_period_type.lower() == 'month':
        month_map = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                     'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
        selected_m = month_map[selected_quarter_or_month.upper()]
        df_filtered = df_filtered[df_filtered['month_num'] == selected_m]

    # Step 3: Filter by HS Code and Item Description
    df_filtered = df_filtered[
        (df_filtered['CTH_HSCODE'] == selected_hscode) &
        (df_filtered['Item_Description_Cluster'] == selected_item)
    ]

    # Step 4: Aggregate quantities by year
    result = df_filtered.groupby('year')[quantity_col].sum().reset_index()

    return result
