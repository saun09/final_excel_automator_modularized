import streamlit as st
import pandas as pd
from io import BytesIO
import plotly.express as px
import matplotlib.pyplot as plt
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
    get_conversion_rate,
    drop_unwanted_columns,
    convert_month_column_to_datetime,
    clean_supplier_name, 
    cluster_supplier_names
)

from clustering import (
    add_cluster_column
)

from analysis import (
    group_data,
    perform_cluster_analysis,
    filter_trade_data,
    full_periodic_analysis,
    get_fy,  
    analyze_trend,
    comparative_analysis
)

from export_excel import (
    create_colored_excel
)

from forecasting import forecast_item

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
    st.session_state['string_cols'] = string_cols
    
    #st.write(f"**Detected string columns (to standardize):** {string_cols}")
    
    # Automatically detect required columns regardless of case
    df_columns_lower = [col.lower() for col in df.columns]
    df_col_map = {col.lower(): col for col in df.columns}

    currency_col = df_col_map.get("invoice_currency")
    unit_col = df_col_map.get("uqc")
    quantity_col = df_col_map.get("quantity")

    required_value_cols_lower = ["unit_price", "total_ass_value", "invoice_unit_price_fc"]
    value_cols = [df_col_map[col] for col in required_value_cols_lower if col in df_col_map]




    if st.button("Clean Data Automatically"):
        with st.spinner("Standardizing and converting..."):

        # Drop unnecessary columns from original df
            df_cleaned = drop_unwanted_columns(df)
            st.session_state["df_cleaned"] = df_cleaned

        # Detect standard columns (case-insensitive)
            df_columns_lower = [col.lower() for col in df_cleaned.columns]
            df_col_map = {col.lower(): col for col in df_cleaned.columns}

            currency_col = df_col_map.get("invoice_currency")
            unit_col = df_col_map.get("uqc")
            quantity_col = df_col_map.get("quantity")
            required_value_cols_lower = ["unit_price", "total_ass_value", "invoice_unit_price_fc"]
            value_cols = [df_col_map[col] for col in required_value_cols_lower if col in df_col_map]

        # Standardize strings
            df_clean = standardize_dataframe(df_cleaned.copy(), detect_string_columns(df_cleaned))

        # Convert units
            df_weight, converted_rows, deleted_rows = convert_to_kg(df_clean, quantity_col, unit_col)

        # Progress bar
            progress_bar = st.progress(0)
            status_text = st.empty()

            def progress_cb(p): progress_bar.progress(min(p, 1.0))
            def status_cb(msg): status_text.text(msg)
            def warning_cb(msg): st.warning(msg)
            def success_cb(msg): st.success(msg)

        # Currency conversion
            df_final = convert_sheet_to_usd(
            df_weight,
            currency_col=currency_col,
            value_cols=value_cols,
            progress_callback=progress_cb,
            status_callback=status_cb,
            warning_callback=warning_cb,
            success_callback=success_cb,
        )
            df_final = convert_month_column_to_datetime(df_final)
            supplier_column = df_col_map.get("supplier_name")  # Case-insensitive match
            if supplier_column:
                df_final = cluster_supplier_names(df_final, supplier_column=supplier_column)
                st.success("Supplier names clustered successfully.")

        # Store in session state
            st.session_state["df_final"] = df_final
            st.session_state["converted_rows"] = converted_rows
            st.session_state["deleted_rows"] = deleted_rows
            st.rerun()



# Post-Standardization Pipeline
if 'df_final' in st.session_state:
    df_final = st.session_state["df_final"]
    df_cleaned = st.session_state.get("df_cleaned", df_final)  # Fallback just in case
    string_cols = detect_string_columns(df_cleaned)

   
    csv_final = df_final.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Final Cleaned + Converted CSV",
        csv_final,
        "final_output.csv",
        "text/csv"
    )

    # ------------------------ CLUSTERING ------------------------
    st.subheader("Product Name Clustering")
    string_cols = list(dict.fromkeys(string_cols))

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
    st.subheader("Color-Coded Excel Export")
    if st.button("Generate Color-Coded Excel", key="excel_export"):
        with st.spinner("Creating color-coded Excel file..."):
            excel_data = create_colored_excel(df_clustered, cluster_column)
            st.session_state['excel_data'] = excel_data
            st.session_state['excel_ready'] = True
            st.success("Excel file generated successfully!")

    if st.session_state.get('excel_ready', False):
        st.download_button(
            "Download Excel",
            data=st.session_state['excel_data'],
            file_name="clustered_data_colored.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
# ------------------------ ANALYTICS ------------------------
if 'df_clustered' in st.session_state:
    df_clustered = st.session_state["df_clustered"]
    cluster_col = f"{st.session_state['cluster_column_name']}_cluster"

    st.subheader("Data Analytics & Insights")

 # --- Trade Filters Section ---
    with st.expander("Filter Trade Data"):
        def clean_list(series):
            return sorted(set(s.strip().lower() for s in series.dropna().unique() if isinstance(s, str)))

        trade_type_col = st.selectbox("Select Trade Type Column (e.g. Import/Export)", df_clustered.columns)
        importer_country_col = st.selectbox("Select Importer Country Column", df_clustered.columns)
        supplier_country_col = st.selectbox("Select Supplier Country Column", df_clustered.columns)

        def clean_unique_options(series):
            return sorted(set(str(val).strip().lower().replace(" ,", ",").replace(", ", ",") for val in series.dropna()))

        trade_type_options = clean_unique_options(df_clustered[trade_type_col])
        importer_options = clean_unique_options(df_clustered[importer_country_col])
        supplier_options = clean_unique_options(df_clustered[supplier_country_col])

        selected_trade_type = st.selectbox("Filter by Trade Type", trade_type_options)
        selected_importer = st.multiselect("Filter by Importer City/State", ["All"] + importer_options, default=["All"])
        selected_supplier = st.multiselect("Filter by Supplier Country", ["All"] + supplier_options, default=["All"])

    # Apply the main filters first
        filtered_df = filter_trade_data(
        df_clustered.copy(),
        trade_type_col,
        importer_country_col,
        supplier_country_col,
        selected_trade_type if selected_trade_type != "All" else None,
        selected_importer if selected_importer != ["All"] else None,
        selected_supplier if selected_supplier != ["All"] else None,
    )

    # --- Time-Based Filtering Section ---
        st.markdown("### Period-Based Aggregation")
        st.info("Date Column is assumed to be `Month`")
        month_col = "Month"

        value_cols = filtered_df.select_dtypes(include='number').columns.tolist()
        selected_value_col = st.selectbox("Select Value Column", value_cols, key="value_col_select")
        aggregation_type = st.selectbox("Choose Aggregation Type", ["Monthly", "Quarterly", "Financial Year", "Calendar Year"], key="aggregation_type_select")

        filtered_by_time_df = filtered_df.copy()

        if aggregation_type == "Monthly":
            unique_months = sorted(filtered_df[month_col].dropna().unique())
            selected_month = st.selectbox("Select Month", unique_months,key="month_select")
            filtered_by_time_df = filtered_df[filtered_df[month_col] == selected_month]

        elif aggregation_type == "Quarterly":
            try:
                df_temp = filtered_df.copy()
                df_temp[month_col] = pd.to_datetime(df_temp[month_col])
                df_temp["Quarter"] = df_temp[month_col].dt.to_period("Q").astype(str)
                unique_quarters = sorted(df_temp["Quarter"].dropna().unique())
                selected_quarter = st.selectbox("Select Quarter", unique_quarters,key="quarter_select")
                filtered_by_time_df = df_temp[df_temp["Quarter"] == selected_quarter]
            except Exception as e:
                st.warning(f"Quarterly parsing failed: {e}")

        elif aggregation_type == "Financial Year":
            df_temp = filtered_df.copy()
            df_temp[month_col] = pd.to_datetime(df_temp[month_col], errors='coerce')
            df_temp["FY"] = df_temp[month_col].apply(lambda x: f"{x.year - 1}-{x.year}" if x.month <= 3 else f"{x.year}-{x.year + 1}")
            unique_fys = sorted(df_temp["FY"].dropna().unique())
            selected_fy = st.selectbox("Select Financial Year", unique_fys,key="fy_select")
            filtered_by_time_df = df_temp[df_temp["FY"] == selected_fy]

        elif aggregation_type == "Calendar Year":
            df_temp = filtered_df.copy()
            df_temp[month_col] = pd.to_datetime(df_temp[month_col], errors='coerce')
            df_temp["Year"] = df_temp[month_col].dt.year
            unique_years = sorted(df_temp["Year"].dropna().unique())
            selected_year = st.selectbox("Select Calendar Year", unique_years,key="year_select")
            filtered_by_time_df = df_temp[df_temp["Year"] == selected_year]

    # --- Filter by CTH_HSCODE ---
        # Step 1: Detect and normalize column names
        columns_lower = [col.lower() for col in filtered_df.columns]
        col_map = {col.lower(): col for col in filtered_df.columns}

        # Step 2: Handle year filtering from either 'YEAR' or 'Month'
        if "year" in columns_lower:
            year_col = col_map["year"]
            filtered_df[year_col] = filtered_df[year_col].astype(int)
        else:
            if "month" in columns_lower:
                month_col = col_map["month"]
                filtered_df[month_col] = pd.to_datetime(filtered_df[month_col], errors="coerce")
                filtered_df["year_extracted"] = filtered_df[month_col].dt.year
                year_col = "year_extracted"
            else:
                year_col = None

        if year_col:
            unique_years = sorted(filtered_df[year_col].dropna().unique())
            selected_years = st.multiselect("Filter by Year", ["All"] + list(map(str, unique_years)), default=["All"])
            if "All" not in selected_years:
                selected_years_int = list(map(int, selected_years))
                filtered_df = filtered_df[filtered_df[year_col].isin(selected_years_int)]
            else:
                selected_years_int = unique_years

        # Step 3: Filter by HS Code
        if "cth_hscode" in columns_lower:
            cth_col = col_map["cth_hscode"]
            cth_hscode_options = sorted(filtered_df[cth_col].dropna().astype(str).unique())
            selected_cth = st.multiselect("Filter by CTH_HSCODE", ["All"] + cth_hscode_options, default=["All"])
            if "All" not in selected_cth:
                filtered_df = filtered_df[filtered_df[cth_col].astype(str).isin(selected_cth)]

        # Step 4: Filter by HSCode + Item Description Combo
        if "item_description" in columns_lower and "cth_hscode" in columns_lower:
            item_col = col_map["item_description"]
            cth_col = col_map["cth_hscode"]

            filtered_df["hs_desc_combo"] = (
                filtered_df[cth_col].astype(str) + " : " + filtered_df[item_col].astype(str)
            )

            item_combo_options = sorted(filtered_df["hs_desc_combo"].dropna().unique())
            selected_combos = st.multiselect("Filter by Item Description + HSCode", ["All"] + item_combo_options, default=["All"])

            if "All" not in selected_combos:
                selected_descriptions = [combo.split(" : ", 1)[1] for combo in selected_combos]
                filtered_df = filtered_df[filtered_df[item_col].isin(selected_descriptions)]
            else:
                selected_descriptions = filtered_df[item_col].dropna().unique().tolist()

        # Step 5: Show filters summary
        #st.markdown("### 🧮 Selected Filters Summary")
        #st.write(f"**Years:** {selected_years}")
        #st.write(f"**HS Codes:** {selected_cth if 'selected_cth' in locals() else 'All'}")
        #st.write(f"**Item Descriptions:** {selected_descriptions }")

        # Step 6: Trigger analyze_trend automatically for multiple products
        if (not filtered_df.empty and selected_descriptions and len(selected_years_int) >= 2):
            trade_type = selected_trade_type if 'selected_trade_type' in locals() else "Imports"
            st.markdown("#### 🧪 Analysis Results")

            for product_name in selected_descriptions:
                try:
                    result = analyze_trend(df, trade_type, product_name, selected_years_int)
                    if result:  # only display if result is not empty
                        st.markdown(f"- **{product_name}**: {result}")
                except Exception:
                    # Silently ignore if trend analysis fails
                    pass

            st.dataframe(filtered_by_time_df)

            csv_filtered = filtered_by_time_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Download Filtered Data",
                csv_filtered,
                "filtered_trade_data.csv",
                "text/csv"
            )

            # --- Time-Based Aggregation ---
            if st.button("🧮 Compute Time-Based Averages for Filtered Data"):
                from analysis import full_periodic_analysis
                with st.spinner("Computing Aggregations..."):
                    try:
                        results, msg = full_periodic_analysis(filtered_by_time_df, "Month", selected_value_col)
                        if results:
                            st.success(msg)
                            for label, table in results.items():
                                st.subheader(label)
                                st.dataframe(table)
                                st.download_button(
                                    f"📥 Download {label}",
                                    table.to_csv(index=False),
                                    f"{label.lower().replace(' ', '_')}.csv"
                                )
                        else:
                            st.error(msg)
                    except Exception as e:
                        st.error(f"Aggregation failed.")


        # --- Trade Analysis ---
    with st.expander("Trade Data Analysis"):
        st.markdown("### Trade Data Analysis")
        string_cols = filtered_by_time_df.select_dtypes(include='object').columns.tolist()
        numeric_cols = filtered_by_time_df.select_dtypes(include='number').columns.tolist()
            

        st.write("String Columns:", string_cols)
        st.write("Numeric Columns:", numeric_cols)

        if string_cols and numeric_cols:
            product_col = st.selectbox("Select Product Column", string_cols,key="product_col")
            quantity_col = st.selectbox("Select Quantity Column", numeric_cols, key="quantity_col")
            value_col = st.selectbox("Select Value Column", numeric_cols,key="value_col_analysis")
            importer_col = st.selectbox("Select Importer Column", string_cols, key="importer_col")
            supplier_col = st.selectbox("Select Supplier Column", string_cols, key="supplier_col")

            from analysis import perform_trade_analysis
            try:
                    analysis_results = perform_trade_analysis(
                        filtered_by_time_df,
                        product_col,
                        quantity_col,
                        value_col,
                        importer_col,
                        supplier_col
                    )
            except Exception as e:
                st.error(f"Error during trade analysis: {e}")
                st.stop()

            if "error" in analysis_results:
                st.error(analysis_results["error"])
            else:
                for section, df_section in analysis_results.items():
                    st.subheader(section)
                    st.dataframe(df_section)
                    if not df_section.empty:
                        st.bar_chart(df_section.set_index(df_section.columns[0]))
        else:
            st.warning("Not enough string or numeric columns in the filtered data.")

        

    #  numeric_cols = detect_numeric_columns(filtered_df)
    # categorical_cols = detect_categorical_columns(filtered_df)

        string_cols = filtered_by_time_df.select_dtypes(include='object').columns.tolist()
        numeric_cols = filtered_by_time_df.select_dtypes(include='number').columns.tolist()
            

        st.write("String Columns:", string_cols)
        st.write("Numeric Columns:", numeric_cols)
        #""" col1, col2 = st.columns(2)
        #with col1:
        #    st.write("**Numeric Columns:**")
    #     st.write(numeric_cols or "None")
    # with col2:
        #  st.write("**Categorical Columns:**")
        #  st.write(categorical_cols or "None") """

        analysis_type = st.selectbox(
            "Select Analysis Type:",
            ["cluster_summary", "top_clusters", "cluster_by_category", "detailed_breakdown"],
            format_func=lambda x: {
                "cluster_summary": "Cluster Summary",
                "top_clusters": "Top Clusters",
                "cluster_by_category": "Cross-Analysis",
                "detailed_breakdown": "Full Breakdown"
            }[x]
            )

        target_col = None
        group_by_col = None

        if numeric_cols:
            target_col = st.selectbox("Select numeric column:", ["None"] + numeric_cols)
            target_col = None if target_col == "None" else target_col

        if analysis_type in ["cluster_by_category", "detailed_breakdown"] and string_cols:
            group_by_col = st.selectbox("Group by column:", string_cols)

        selected_clusters = st.multiselect("Filter Clusters:", sorted(filtered_df[cluster_col].unique()), default=[])

        if st.button("Run Analysis"):
            with st.spinner("Analyzing..."):
                result, message = perform_cluster_analysis(
                    filtered_df,
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
                    st.download_button("Download Results", csv_results, f"analysis_{analysis_type}.csv", "text/csv")
                else:
                    st.error(f"Error: {message}")

            st.markdown("---")


    
    with st.expander("Period-Based Aggregation (Monthly, Quarterly, FY, CY)"):
        date_col = st.selectbox("Select Date Column (e.g. BE_Date or Month)", df_clustered.columns)
        value_col = st.selectbox("Select Value Column (e.g. Unit Price)", df_clustered.columns)

        if st.button("Compute Time-Based Averages"):
            with st.spinner("Computing time-based aggregations..."):
                from analysis import full_periodic_analysis

                results, msg = full_periodic_analysis(filtered_df, date_col, value_col)

                if results:
                    st.success(msg)

                    st.subheader("Monthly Average")
                    st.dataframe(results["Monthly Average"])
                    st.download_button("Download Monthly Avg", results["Monthly Average"].to_csv(index=False), "monthly_avg.csv")

                    st.subheader("Quarterly Average")
                    st.dataframe(results["Quarterly Average"])
                    st.download_button("Download Quarterly Avg", results["Quarterly Average"].to_csv(index=False), "quarterly_avg.csv")

                    st.subheader("Financial Year Average")
                    st.dataframe(results["Financial Year Average"])
                    st.download_button("Download FY Avg", results["Financial Year Average"].to_csv(index=False), "fy_avg.csv")

                    st.subheader("Calendar Year Average")
                    st.dataframe(results["Calendar Year Average"])
                    st.download_button("Download CY Avg", results["Calendar Year Average"].to_csv(index=False), "cy_avg.csv")
                else:
                    st.error(msg)

    with st.expander("📈 Forecast Product Price or Quantity"):
        st.markdown("Select an HS Code and the product you'd like to forecast.")

        if 'df_clustered' in st.session_state:
            df_clustered = st.session_state["df_clustered"]
            cluster_col = f"{st.session_state['cluster_column_name']}_cluster"

            if cluster_col not in df_clustered.columns:
                st.error(f"Column `{cluster_col}` not found in the dataset.")
            else:
                # Normalize HSCODE column
                hscode_col = "CTH_HSCODE"
                item_col = "Item_Description_cluster"
                month_col = "Month"  # Ensure this is datetime64[ns]

                df_clustered[month_col] = pd.to_datetime(df_clustered[month_col], errors='coerce')

                # Step 1: Select HS Code
                unique_hscodes = sorted(df_clustered[hscode_col].dropna().unique())
                selected_hscode = st.selectbox("Select HS Code", unique_hscodes, key="forecast_hscode")

                # Step 2: Filter by selected HSCODE
                hscode_filtered = df_clustered[df_clustered[hscode_col] == selected_hscode].copy()

                # Step 3: Build list of products with >= 6 unique months
                valid_items = []
                for item in hscode_filtered[item_col].dropna().unique():
                    months_present = hscode_filtered[hscode_filtered[item_col] == item][month_col].dt.to_period("M").nunique()
                    if months_present >= 6:
                        valid_items.append(item)

                if not valid_items:
                    st.warning("No products with 6 or more months of data for the selected HS Code.")
                else:
                    # Step 4: Select Product and Forecast Column
                    item_selected = st.selectbox("Choose Product", sorted(valid_items), key="forecast_item")

                    column_choice = st.selectbox(
                        "Column to Forecast",
                        ["Quantity", "Unit_Price_USD", "Total_Ass_Value_USD"],
                        key="forecast_metric"
                    )

                    if st.button("Run Forecast", key="run_forecast_btn"):
                        from forecasting import forecast_item  # ensure this function is defined

                        forecast_df, description, plot_buf = forecast_item(hscode_filtered, item_selected, column_choice, cluster_col)

                        if isinstance(description, str) and "error" in description.lower():
                            st.error(description)
                        elif forecast_df is not None:
                            st.success(f"Forecast for '{item_selected}' over the next 5 years:")
                            st.dataframe(forecast_df)

                            st.markdown(f"📊 **Trend Insight:** {description}")
                            st.image(plot_buf, caption="Historical (green) vs Forecast (red)", use_container_width=True)

                            st.download_button(
                                label="Download Forecast CSV",
                                data=forecast_df.to_csv(index=False),
                                file_name=f"{item_selected}_{column_choice}_forecast.csv",
                                mime="text/csv"
                            )
                        else:
                            st.warning(description)


    with st.expander("Comparative Quantity Analysis (Quarter-wise)"):
    # Step 1: Ensure 'Month' is datetime
        df_clustered["Month"] = pd.to_datetime(df_clustered["Month"], errors="coerce")

        # Step 2: Select Years
        available_years = sorted(df_clustered["Month"].dt.year.dropna().unique())
        selected_years = st.multiselect("Select Years", available_years, default=available_years[:2])

        if len(selected_years) < 2:
            st.warning("Please select at least two years to compare.")
        else:
            # Step 3: Quarter Selection
            quarter_map = {
                "Jan - Mar": [1, 2, 3],
                "Apr - Jun": [4, 5, 6],
                "Jul - Sep": [7, 8, 9],
                "Oct - Dec": [10, 11, 12]
            }
            selected_quarter = st.selectbox("Select Quarter", list(quarter_map.keys()))
            selected_months = quarter_map[selected_quarter]

            # Step 4: HS Code selection
            hscode_col = "CTH_HSCODE"
            item_col = "Item_Description_cluster"
            quantity_col = "Quantity"

            available_hscodes = sorted(df_clustered[hscode_col].dropna().unique())
            selected_hscode = st.selectbox("Select HS Code", available_hscodes)

            item_col = "Item_Description_cluster"  # change if your column is named differently
            filtered_by_hscode = df_clustered[df_clustered[hscode_col] == selected_hscode]

            # Combine HSCode + Item for dropdown display
            filtered_by_hscode["hs_item_combo"] = (
                filtered_by_hscode[hscode_col].astype(str) + " : " + filtered_by_hscode[item_col].astype(str)
            )

            # Get unique combos
            combo_options = sorted(filtered_by_hscode["hs_item_combo"].dropna().unique())
            selected_combo = st.selectbox("Select Product Description", combo_options)

            # Extract actual item from selection
            selected_item = selected_combo.split(" : ", 1)[1]

            # Step 6: Filter data for selected years, HS code, product, and quarter
            filtered_df = df_clustered[
                (df_clustered["Month"].dt.year.isin(selected_years)) &
                (df_clustered["Month"].dt.month.isin(selected_months)) &
                (df_clustered[hscode_col] == selected_hscode) &
                (df_clustered[item_col] == selected_item)
            ]

            # Step 7: Group and display comparison
            if filtered_df.empty:
                st.warning("No data available for the selected filters.")
            else:
                summary = (
                    filtered_df.groupby(filtered_df["Month"].dt.year)[quantity_col]
                    .sum()
                    .reset_index()
                    .rename(columns={"Month": "Year", quantity_col: "Total Quantity"})
                )
                summary.columns = ["Month", "Total Quantity"]

                st.markdown("### Quarter-wise Comparative Quantity by Year")
                st.dataframe(summary)

                if len(summary) == 2:
                    y1, y2 = summary.iloc[0]["Month"], summary.iloc[1]["Month"]
                    q1, q2 = summary.iloc[0]["Total Quantity"], summary.iloc[1]["Total Quantity"]

                    if q2 > q1:
                        trend = "increased"
                    elif q2 < q1:
                        trend = "decreased"
                    else:
                        trend = "remained the same"

                    st.markdown(f"""
                    Between **{y1}** and **{y2}**, the quantity for product **{selected_item}** under HS Code **{selected_hscode}** 
                    during **{selected_quarter}** has **{trend}** from **{q1:.2f}** to **{q2:.2f}**.
                    """)

    with st.expander("Analysis Company Wise"):
        df_clustered["Month"] = pd.to_datetime(df_clustered["Month"], errors="coerce")

        # Step 1: Select Year and Quarter Group
        available_years = sorted(df_clustered["Month"].dt.year.dropna().unique())
        selected_year = st.selectbox("Select Year", available_years, key="companywise_year")

        quarter_dict = {
            "Jan–Mar": [1, 2, 3],
            "Apr–Jun": [4, 5, 6],
            "Jul–Sep": [7, 8, 9],
            "Oct–Dec": [10, 11, 12]
        }
        quarter_label = st.selectbox("Select Quarter Group", list(quarter_dict.keys()), key="companywise_quarter")
        selected_months = quarter_dict[quarter_label]

        # Step 2: Select Trade Type
        trade_types = df_clustered["Type"].dropna().unique()
        selected_trade = st.selectbox("Select Trade Type", trade_types, key="companywise_trade_type")

        # Step 3: Multi-select Companies
        supplier_col = "Supplier_Name"
        unique_companies = sorted(df_clustered[supplier_col].dropna().unique())
        selected_companies = st.multiselect("Select Company(s)", unique_companies, key="companywise_companies")
        df_clustered[supplier_col] = df_clustered[supplier_col].astype(str).str.strip().str.lower()
        selected_companies = [c.strip().lower() for c in selected_companies]


        if selected_companies:
            df_company_filtered = df_clustered[df_clustered[supplier_col].isin(selected_companies)]
            

            # Step 4: Filter HS Codes based on selected companies
            hscode_col = "CTH_HSCODE"
            hs_codes_filtered = df_company_filtered[hscode_col].dropna().astype(str).unique()
            selected_hscodes = st.multiselect("Select HS Code(s)", sorted(hs_codes_filtered), key="companywise_hscode")

            if selected_hscodes:
                df_hscode_filtered = df_company_filtered[df_company_filtered[hscode_col].astype(str).isin(selected_hscodes)]

                # Step 5: Product combo filtering
                item_col = "Item_Description_cluster"
                df_hscode_filtered["hs_item_combo"] = (
                    df_hscode_filtered[hscode_col].astype(str) + " : " + df_hscode_filtered[item_col].astype(str)
                )
                combo_options = sorted(df_hscode_filtered["hs_item_combo"].dropna().unique())
                selected_combos = st.multiselect("Select HS Code + Product(s)", combo_options, key="companywise_combo")

                selected_items = [combo.split(" : ", 1)[1] for combo in selected_combos]

                # Step 6: Filter for all selections
                final_filtered = df_clustered[
                    (df_clustered["Month"].dt.year == selected_year) &
                    (df_clustered["Month"].dt.month.isin(selected_months)) &
                    (df_clustered["Type"] == selected_trade) &
                    (df_clustered[supplier_col].isin(selected_companies)) &
                    (df_clustered[hscode_col].astype(str).isin(selected_hscodes)) &
                    (df_clustered[item_col].isin(selected_items))
                ]

                if final_filtered.empty:
                    st.warning("No matching records found.")
                else:
                    st.markdown("### Filtered Results")
                    st.dataframe(final_filtered)

                    st.markdown("### Company-wise Summary")
                    for company in selected_companies:
                        company_df = final_filtered[final_filtered[supplier_col] == company]

                        if not company_df.empty:
                            avg_price = company_df["Unit_Price_USD"].mean()
                            total_qty = company_df["Quantity"].sum()
                            hs_used = sorted(company_df[hscode_col].astype(str).unique())
                            items_used = sorted(company_df[item_col].astype(str).unique())

                            st.markdown(f"""
                            **Summary for `{company}`**  
                            - Year: **{selected_year}**, Quarter: **{quarter_label}**  
                            - HS Code(s): **{', '.join(hs_used)}**  
                            - Product(s): **{', '.join(items_used)}**  
                            - Trade Type: **{selected_trade}**  
                            - Total Quantity: **{total_qty:,.2f}**  
                            - Average Unit Price (USD): **{avg_price:,.2f}**
                            """)
                        else:
                            st.info(f"No data found for company: {company}")
            else:
                st.info("Please select at least one HS Code.")
        else:
            st.info("Please select at least one company.")



        # Business Questions
    st.subheader("Business Questions")

    question = st.selectbox("What do you want to analyze?", [
        "Top Exporter Companies",
        "Top Importer Companies",
        "Most Traded Product",
        "Average Unit Price in Month",
        "Top Exporter Countries to Importer"
    ])

    if "product" in filtered_df.columns and question in [
        "Most Traded Product", "Average Unit Price in Month", "Top Exporter Countries to Importer"
    ]:
        product_options = sorted(set(filtered_df["product"].dropna().astype(str)))
        selected_product = st.selectbox("Select a Product", product_options)
    else:
        selected_product = None

    if "month" in filtered_df.columns and question == "Average Unit Price in Month":
        month_options = sorted(set(filtered_df["month"].dropna().astype(str)))
        selected_month = st.selectbox("Select Month", month_options)
    else:
        selected_month = None

    if st.button("Get Insight"):
        with st.spinner("Processing your request..."):
            result_df = None

            try:
                if question == "Top Exporter Companies":
                    result_df = filtered_df[supplier_country_col].value_counts().head(10).reset_index()
                    result_df.columns = ['Exporter Company', 'Export Count']

                elif question == "Top Importer Companies":
                    result_df = filtered_df[importer_country_col].value_counts().head(10).reset_index()
                    result_df.columns = ['Importer Company', 'Import Count']

                elif question == "Most Traded Product":
                    result_df = filtered_df[filtered_df["product"].str.contains(selected_product, case=False, na=False)]
                    result_df = result_df["product"].value_counts().reset_index().head(10)
                    result_df.columns = ["Product", "Trade Count"]

                elif question == "Average Unit Price in Month":
                    price_df = filtered_df[
                        filtered_df["product"].str.contains(selected_product, case=False, na=False) &
                        filtered_df["month"].str.lower().str.contains(selected_month.lower())
                        ]
                    if not price_df.empty and "unit_price" in price_df.columns:
                        avg_price = price_df["unit_price"].astype(float).mean()
                        st.success(f"Average Unit Price of {selected_product} in {selected_month}: **{avg_price:.2f} USD/unit**")
                    else:
                        st.warning("No relevant data found for that product and month.")

                elif question == "Top Exporter Countries to Importer":
                    filtered = filtered_df[
                        (filtered_df[importer_country_col].str.lower() == "india") & 
                        (filtered_df["product"].str.contains(selected_product, case=False, na=False))
                        ]
                    result_df = filtered[supplier_country_col].value_counts().reset_index().head(10)
                    result_df.columns = ["Supplier Country", "Export Count"]

                if result_df is not None:
                    st.subheader("Insight Result")
                    st.dataframe(result_df)
                    st.download_button(
                        "Download Results",
                        result_df.to_csv(index=False).encode("utf-8"),
                        "business_question_results.csv",
                        "text/csv"
                    )
            except Exception as e:
                st.error(f"Error while processing question: {e}")
    else:
        st.warning("No data matches the selected filters.")