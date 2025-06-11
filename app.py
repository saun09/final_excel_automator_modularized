import streamlit as st
import pandas as pd

from io import BytesIO
# Import from modularized files
from data_cleaning import (
    detect_string_columns, 
    detect_numeric_columns, 
    detect_categorical_columns,
    standardize_dataframe,
    convert_df_to_csv_bytes,
    convert_to_kg,
    fetch_supported_currencies,
    convert_sheet_to_usd,
    convert_currency
)

from clustering import (
    add_cluster_column
)

from analysis import (
    group_data,
    perform_cluster_analysis
)

from export_excel import (
    create_colored_excel
)

# Streamlit UI starts here
st.title("Automatic String Column Standardizer with Clustering")

uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

if uploaded_file:
    # Read CSV with fallback encoding
    try:
        df = pd.read_csv(uploaded_file)
    except UnicodeDecodeError:
        uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file, encoding='ISO-8859-1')

    st.subheader("Original Data Sample")
    st.dataframe(df.head(10))

    string_cols = detect_string_columns(df)

    st.write(f"**Detected string columns (to standardize):** {string_cols}")
    if st.button("🔁 Convert Units to Kilograms"):
        df_converted, converted_rows, deleted_rows = convert_to_kg(df.copy())
        st.session_state['df_converted'] = df_converted

        st.subheader("✅ Converted Data (Units → kg)")
        st.dataframe(df_converted.head(10))

        csv_kg = df_converted.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Converted CSV (kg)",
            data=csv_kg,
            file_name="converted_to_kg.csv",
            mime="text/csv"
    )

        # Show converted rows
        if converted_rows:
            st.subheader("🔁 Rows Converted to kg")
            st.dataframe(pd.DataFrame(converted_rows))

    # Show and log deleted rows
        if deleted_rows:
            st.subheader("🗑️ Rows Deleted (Non-Convertible Units)")
            st.warning("These rows had unrecognized units like '2 pcs', 'hands full', etc. and were removed.")
            st.dataframe(pd.DataFrame(deleted_rows))

    currency_col = st.selectbox("Select the currency column", df.columns)
    value_cols = st.multiselect("Select the columns to convert to USD", df.columns)

    if st.button("Convert to USD"):
        with st.spinner("Converting..."):
            progress_bar = st.progress(0)
            status_text = st.empty()

            def progress_cb(p): progress_bar.progress(p)
            def status_cb(msg): status_text.text(msg)
            def warning_cb(msg): st.warning(msg)
            def success_cb(msg): st.success(msg)

            df_converted = convert_sheet_to_usd(
                df,
                currency_col,
                value_cols,
                progress_callback=progress_cb,
                status_callback=status_cb,
                warning_callback=warning_cb,
                success_callback=success_cb,
            )

        st.subheader("Converted Data")
        st.dataframe(df_converted.head())

        csv = df_converted.to_csv(index=False).encode('utf-8')
        st.download_button("Download Converted CSV", csv, "converted_data.csv", "text/csv")

# Optional API Test
if st.sidebar.button("Test API Connection"):
    st.sidebar.write("Testing API...")
    rate = get_conversion_rate("EUR")
    if rate:
        st.sidebar.success(f"1 EUR = ${rate} USD")
    else:
        st.sidebar.error("API Connection Failed")


    if st.button("Standardize String Columns"):
        df_clean = standardize_dataframe(df, string_cols)
        st.subheader("Standardized Data Sample")
        st.dataframe(df_clean.head(10))

        # Prepare CSV for download
        csv = df_clean.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Standardized CSV",
            data=csv,
            file_name="standardized_output.csv",
            mime="text/csv"
        )
        
        # Store standardized data in session state for clustering
        st.session_state['df_standardized'] = df_clean
        st.session_state['string_cols'] = string_cols

# Clustering section (only show if standardized data exists)
if 'df_standardized' in st.session_state:
    st.subheader("Product Name Clustering")
    st.write("Select a column to cluster similar product names:")
    
    df_std = st.session_state['df_standardized']
    cluster_column = st.selectbox(
        "Choose column for clustering:",
        options=st.session_state['string_cols'],
        key="cluster_column_select"
    )
    
    if st.button("Create Clusters"):
        df_clustered = add_cluster_column(df_std, cluster_column)
        
        # Store clustered data in session state
        st.session_state['df_clustered'] = df_clustered
        st.session_state['cluster_column_name'] = cluster_column
        
        st.subheader("Data with Clusters")
        # Show original, standardized, and clustered columns side by side
        display_cols = [cluster_column, f"{cluster_column}_cluster"]
        if cluster_column in df_clustered.columns:
            st.dataframe(df_clustered[display_cols].head(20))
        
        # Show cluster summary
        cluster_col = f"{cluster_column}_cluster"
        if cluster_col in df_clustered.columns:
            cluster_counts = df_clustered[cluster_col].value_counts()
            st.subheader("Cluster Summary")
            st.write(f"Total unique clusters: {len(cluster_counts)}")
            st.dataframe(cluster_counts.head(10).to_frame("Count"))
        
        # Download clustered data as CSV
        csv_clustered = df_clustered.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Data with Clusters (CSV)",
            data=csv_clustered,
            file_name="clustered_output.csv",
            mime="text/csv"
        )

# Show clustered data and Excel export if clustering has been done
if 'df_clustered' in st.session_state:
    df_clustered = st.session_state['df_clustered']
    cluster_column = st.session_state['cluster_column_name']
    
    # Show the clustered data again
    st.subheader("Clustered Data (Persistent)")
    display_cols = [cluster_column, f"{cluster_column}_cluster"]
    st.dataframe(df_clustered[display_cols].head(20))
    
    # Show cluster summary
    cluster_col = f"{cluster_column}_cluster"
    cluster_counts = df_clustered[cluster_col].value_counts()
    st.write(f"**Total unique clusters:** {len(cluster_counts)}")
    
    # Color-Coded Excel Export Section
    st.subheader("Color-Coded Excel Export")
    st.write("Generate an Excel file where each cluster is color-coded and grouped together:")
    
    if st.button("Generate Color-Coded Excel", key="excel_export"):
        with st.spinner("Creating color-coded Excel file..."):
            excel_data = create_colored_excel(df_clustered, cluster_column)
            
            if excel_data:
                st.session_state['excel_data'] = excel_data
                st.session_state['excel_ready'] = True
                st.success("✅ Excel file generated successfully!")
    
    # Show download button if Excel is ready
    if st.session_state.get('excel_ready', False) and 'excel_data' in st.session_state:
        st.download_button(
            label="📊 Download Color-Coded Excel File",
            data=st.session_state['excel_data'],
            file_name="clustered_data_colored.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        # Show preview of what the Excel will contain
        cluster_col = f"{cluster_column}_cluster"
        preview_data = df_clustered.groupby(cluster_col).size().reset_index(name='Row_Count')
        st.write("**Excel File Contents:**")
        st.write("- **Clustered_Data** sheet: All rows grouped by cluster with color coding")
        st.write("- **Cluster_Summary** sheet: Summary of clusters with counts")
        st.write("**Cluster Distribution:**")
        st.dataframe(preview_data.head(10))

    # DATA ANALYTICS SECTION
    st.subheader("📊 Data Analytics & Insights")
    st.write("Query your clustered data to get analytical insights:")
    
    # Detect column types for better user experience
    numeric_cols = detect_numeric_columns(df_clustered)
    categorical_cols = detect_categorical_columns(df_clustered)
    
    # Analytics interface
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Available Numeric Columns (for calculations):**")
        st.write(numeric_cols if numeric_cols else "No numeric columns detected")
        
    with col2:
        st.write("**Available Categorical Columns (for grouping):**")
        st.write(categorical_cols if categorical_cols else "No categorical columns detected")
    
    # Analysis type selection
    analysis_type = st.selectbox(
        "Select Analysis Type:",
        [
            "cluster_summary",
            "top_clusters", 
            "cluster_by_category",
            "detailed_breakdown"
        ],
        format_func=lambda x: {
            "cluster_summary": "📈 Cluster Summary (Total records, sums, averages)",
            "top_clusters": "🏆 Top Clusters (Ranked by selected metric)",
            "cluster_by_category": "📊 Cross-Analysis (Clusters vs Categories)",
            "detailed_breakdown": "🔍 Detailed Breakdown (Complete analysis by category)"
        }[x]
    )
    
    # Dynamic input fields based on analysis type
    target_col = None
    group_by_col = None
    selected_clusters = None
    
    if analysis_type in ["cluster_summary", "top_clusters", "cluster_by_category", "detailed_breakdown"]:
        if numeric_cols:
            target_col = st.selectbox(
                "Select Numeric Column for Calculations (optional):",
                ["None"] + numeric_cols
            )
            target_col = None if target_col == "None" else target_col
    
    if analysis_type in ["cluster_by_category", "detailed_breakdown"]:
        if categorical_cols:
            group_by_col = st.selectbox(
                "Group By Column:",
                categorical_cols
            )
    
    # Cluster selection
    all_clusters = sorted(df_clustered[cluster_col].unique())
    selected_clusters = st.multiselect(
        "Select Specific Clusters (leave empty for all):",
        all_clusters,
        default=[]
    )
    
    if not selected_clusters:
        selected_clusters = None
    
    # Run analysis button
    if st.button("🔍 Run Analysis", key="run_analysis"):
        with st.spinner("Analyzing data..."):
            result, message = perform_cluster_analysis(
                df_clustered, 
                cluster_col, 
                analysis_type, 
                target_col, 
                group_by_col, 
                selected_clusters
            )
            
            if result is not None:
                st.success(message)
                st.subheader("Analysis Results")
                st.dataframe(result)
                
                # Download results
                csv_results = result.to_csv().encode('utf-8')
                st.download_button(
                    label="📥 Download Analysis Results",
                    data=csv_results,
                    file_name=f"analysis_{analysis_type}.csv",
                    mime="text/csv"
                )
                
                # Store results in session state
                st.session_state['analysis_results'] = result
                st.session_state['analysis_type'] = analysis_type
                
            else:
                st.error(f"Analysis failed: {message}")
    
    # Quick insights section
    if 'analysis_results' in st.session_state:
        st.subheader("💡 Quick Insights")
        result = st.session_state['analysis_results']
        analysis_type = st.session_state['analysis_type']
        
        if analysis_type == "cluster_summary":
            st.write(f"**Total Clusters Analyzed:** {len(result)}")
            if 'Total_Records' in result.columns:
                st.write(f"**Largest Cluster:** {result['Total_Records'].idxmax()} ({result['Total_Records'].max()} records)")
            
            if target_col and f'{target_col}_Total' in result.columns:
                st.write(f"**Highest {target_col} Total:** {result[f'{target_col}_Total'].idxmax()} ({result[f'{target_col}_Total'].max():,.2f})")
        
        elif analysis_type == "top_clusters":
            st.write(f"**Top Performing Cluster:** {result.index[0]} ({result.iloc[0, 0]:,.2f})")
            st.write(f"**Bottom Performing Cluster:** {result.index[-1]} ({result.iloc[-1, 0]:,.2f})")

    # DATA GROUPING SECTION
    st.subheader("📊 Data Grouping")
    st.write("Group your data by categorical columns to analyze patterns:")
    
    # Grouping interface
    col1, col2 = st.columns(2)
    
    with col1:
        # Multiselect for grouping columns
        group_by_cols = st.multiselect(
            "Select columns to group by:",
            categorical_cols,
            default=[]
        )
    
    with col2:
        # Select numeric column to aggregate (optional)
        agg_col = st.selectbox(
            "Select numeric column to aggregate (optional):",
            ["None"] + numeric_cols,
            key="agg_col_select"
        )
        
        # Select aggregation function
        agg_func = st.selectbox(
            "Select aggregation function:",
            ["count", "sum", "mean", "median", "min", "max"],
            disabled=(agg_col == "None"),
            key="agg_func_select"
        )
    
    # Prepare aggregation rules
    aggregation_rules = None
    if agg_col != "None":
        aggregation_rules = {agg_col: agg_func}
    
    if st.button("🔢 Group Data", key="group_data_button"):
        if not group_by_cols:
            st.warning("Please select at least one column to group by")
        else:
            with st.spinner("Grouping data..."):
                grouped_df = group_data(df_clustered, group_by_cols, aggregation_rules)
                
                st.subheader("Grouped Data Results")
                st.dataframe(grouped_df.head(50))
                
                # Download results
                csv_grouped = grouped_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Grouped Data",
                    data=csv_grouped,
                    file_name="grouped_data.csv",
                    mime="text/csv"
                )
                
                # Store results in session state
                st.session_state['grouped_data'] = grouped_df
                st.session_state['group_by_cols'] = group_by_cols
                
                # Show quick summary
                st.subheader("💡 Quick Insights")
                st.write(f"Data grouped by: {', '.join(group_by_cols)}")
                if agg_col != "None":
                    st.write(f"Aggregated column: {agg_col} ({agg_func})")
                    st.write(f"Total {agg_col}: {grouped_df[agg_col].sum():,.2f}")
                st.write(f"Number of groups: {len(grouped_df)}")