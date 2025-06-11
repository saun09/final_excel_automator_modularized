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
    convert_currency,
    get_conversion_rate
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

# App Title
st.title("Automatic String Column Standardizer with Clustering")

# File Upload
if 'df_original' not in st.session_state:
    uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
        except UnicodeDecodeError:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, encoding='ISO-8859-1')
        
        st.session_state['df_original'] = df

# Show uploaded file sample
if 'df_original' in st.session_state:
    df = st.session_state['df_original']
    st.subheader("Original Data Sample")
    st.dataframe(df.head(10))
    
    string_cols = detect_string_columns(df)
    st.session_state['string_cols'] = string_cols
    
    st.write(f"**Detected string columns (to standardize):** {string_cols}")
    
    currency_col = st.selectbox("Select the currency column", df.columns)
    value_cols = st.multiselect("Select the columns to convert to USD", df.columns)
    quantity_col = st.selectbox("Select the quantity column", df.columns)
    unit_col = st.selectbox("Select the unit column", df.columns)

    if st.button("🧹 Standardize + Convert Units & Currency"):
        with st.spinner("Standardizing and converting..."):
            df_clean = standardize_dataframe(df.copy(), string_cols)
            df_weight, converted_rows, deleted_rows = convert_to_kg(df_clean)
            
            progress_bar = st.progress(0)
            status_text = st.empty()

            def progress_cb(p): progress_bar.progress(min(p, 1.0))
            def status_cb(msg): status_text.text(msg)
            def warning_cb(msg): st.warning(msg)
            def success_cb(msg): st.success(msg)

            df_final = convert_sheet_to_usd(
                df_weight,
                currency_col=currency_col,
                value_cols=value_cols,
                progress_callback=progress_cb,
                status_callback=status_cb,
                warning_callback=warning_cb,
                success_callback=success_cb,
            )

            st.session_state["df_final"] = df_final
            st.session_state["converted_rows"] = converted_rows
            st.session_state["deleted_rows"] = deleted_rows
            st.rerun()

# Post-Standardization Pipeline
if 'df_final' in st.session_state:
    df_final = st.session_state["df_final"]
    string_cols = st.session_state.get("string_cols", detect_string_columns(df_final))
    
    st.subheader("Final Data Sample")
    st.dataframe(df_final.head(10))

    converted_rows = st.session_state.get("converted_rows", [])
    deleted_rows = st.session_state.get("deleted_rows", [])

    if converted_rows:
        st.subheader("🔁 Rows Converted to kg")
        st.dataframe(pd.DataFrame(converted_rows))

    if deleted_rows:
        st.subheader("🗑️ Rows Deleted (Non-Convertible Units)")
        st.warning("These rows had unrecognized units and were removed.")
        st.dataframe(pd.DataFrame(deleted_rows))

    # Final CSV download
    csv_final = df_final.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Final Cleaned + Converted CSV",
        csv_final,
        "final_output.csv",
        "text/csv"
    )

    # ------------------------ CLUSTERING ------------------------
    st.subheader("🔗 Product Name Clustering")
    cluster_column = st.selectbox("Choose column to cluster:", string_cols, key="cluster_column")

    if st.button("Create Clusters"):
        df_clustered = add_cluster_column(df_final.copy(), cluster_column)
        st.session_state["df_clustered"] = df_clustered
        st.session_state["cluster_column_name"] = cluster_column
        st.rerun()

# Show clustering results
if 'df_clustered' in st.session_state:
    df_clustered = st.session_state["df_clustered"]
    cluster_column = st.session_state["cluster_column_name"]
    cluster_col = f"{cluster_column}_cluster"

    st.subheader("Clustered Data")
    st.dataframe(df_clustered[[cluster_column, cluster_col]].head(20))

    cluster_counts = df_clustered[cluster_col].value_counts()
    st.subheader("Cluster Summary")
    st.write(f"Total unique clusters: {len(cluster_counts)}")
    st.dataframe(cluster_counts.head(10).to_frame("Count"))

    csv_clustered = df_clustered.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Data with Clusters (CSV)",
        data=csv_clustered,
        file_name="clustered_output.csv",
        mime="text/csv"
    )

    # ------------------------ EXCEL EXPORT ------------------------
    st.subheader("📊 Color-Coded Excel Export")
    if st.button("Generate Color-Coded Excel", key="excel_export"):
        with st.spinner("Creating color-coded Excel file..."):
            excel_data = create_colored_excel(df_clustered, cluster_column)
            st.session_state['excel_data'] = excel_data
            st.session_state['excel_ready'] = True
            st.success("✅ Excel file generated successfully!")

    if st.session_state.get('excel_ready', False):
        st.download_button(
            "📥 Download Excel",
            data=st.session_state['excel_data'],
            file_name="clustered_data_colored.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # ------------------------ ANALYTICS ------------------------
    st.subheader("📈 Data Analytics & Insights")

    numeric_cols = detect_numeric_columns(df_clustered)
    categorical_cols = detect_categorical_columns(df_clustered)

    col1, col2 = st.columns(2)
    with col1:
        st.write("**Numeric Columns:**")
        st.write(numeric_cols or "None")
    with col2:
        st.write("**Categorical Columns:**")
        st.write(categorical_cols or "None")

    analysis_type = st.selectbox(
        "Select Analysis Type:",
        ["cluster_summary", "top_clusters", "cluster_by_category", "detailed_breakdown"],
        format_func=lambda x: {
            "cluster_summary": "📈 Cluster Summary",
            "top_clusters": "🏆 Top Clusters",
            "cluster_by_category": "📊 Cross-Analysis",
            "detailed_breakdown": "🔍 Full Breakdown"
        }[x]
    )

    target_col = None
    group_by_col = None

    if numeric_cols:
        target_col = st.selectbox("Select numeric column:", ["None"] + numeric_cols)
        target_col = None if target_col == "None" else target_col

    if analysis_type in ["cluster_by_category", "detailed_breakdown"] and categorical_cols:
        group_by_col = st.selectbox("Group by column:", categorical_cols)

    selected_clusters = st.multiselect("Filter Clusters:", sorted(df_clustered[cluster_col].unique()), default=[] or None)

    if st.button("🔍 Run Analysis"):
        with st.spinner("Analyzing..."):
            result, message = perform_cluster_analysis(
                df_clustered,
                cluster_col,
                analysis_type,
                target_col,
                group_by_col,
                selected_clusters or None
            )

            if result is not None:
                st.success(message)
                st.subheader("Analysis Results")
                st.dataframe(result)
                csv_results = result.to_csv(index=False).encode("utf-8")
                st.download_button("📥 Download Results", csv_results, f"analysis_{analysis_type}.csv", "text/csv")
            else:
                st.error(f"Error: {message}")
