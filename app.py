import streamlit as st
import pandas as pd
from io import BytesIO
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns
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
    cluster_supplier_names,
    cluster_location_column,
    clean_location_name,
    detect_categorical_columns
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
    comparative_analysis,
    perform_trade_analysis,
    analyze_trend
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

            importer_city_col = df_col_map.get("importer_city_state")
            if importer_city_col:
                df_final = cluster_location_column(df_final, column=importer_city_col)
                st.success("Importer city-state values clustered successfully.")

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
    st.dataframe(df_clustered[[cluster_column, cluster_col]].head(5))

    cluster_counts = df_clustered[cluster_col].value_counts()
    st.subheader("Cluster Summary")
    st.write(f"Total unique clusters: {len(cluster_counts)}")
    st.dataframe(cluster_counts.head(3).to_frame("Count"))

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
    with st.expander("Filter & Analyze Trade Data"):
        def clean_unique(series):
            return sorted(set(str(x).strip().lower() for x in series.dropna()))

        columns_lower = [col.lower() for col in df_clustered.columns]
        col_map = {col.lower(): col for col in df_clustered.columns}

        # Step 1 & 2: Select Columns
        importer_country_col = st.selectbox("Select Importer Country Column", df_clustered.columns)
        supplier_country_col = st.selectbox("Select Supplier Country Column", df_clustered.columns)

        # Step 3: Trade Type
        trade_type_col = "Type"
        trade_types = clean_unique(df_clustered[trade_type_col])
        selected_trade_type = st.selectbox("Select Trade Type", trade_types)

        # Step 4 & 5: Importer and Supplier Filters
        importer_options = clean_unique(df_clustered[importer_country_col])
        supplier_options = clean_unique(df_clustered[supplier_country_col])
        selected_importer = st.multiselect("Filter by Importer City/State", ["All"] + importer_options, default=["All"])
        selected_supplier = st.multiselect("Filter by Supplier Country", ["All"] + supplier_options, default=["All"])

        # Apply Filters
        df_filtered = df_clustered.copy()
        df_filtered = df_filtered[df_filtered[trade_type_col].str.lower() == selected_trade_type.lower()]
        if "All" not in selected_importer:
            df_filtered = df_filtered[df_filtered[importer_country_col].str.lower().isin(selected_importer)]
        if "All" not in selected_supplier:
            df_filtered = df_filtered[df_filtered[supplier_country_col].str.lower().isin(selected_supplier)]

        st.subheader("Filtered Data Before Value/Year/HSCode")
        st.dataframe(df_filtered)

        # Step 6: Value Column
        value_cols = df_filtered.select_dtypes(include="number").columns.tolist()
        selected_value_col = st.selectbox("Select Value Column", value_cols)

        # Step 7: Year Filter
        if "Month" in df_filtered.columns:
            df_filtered["Month"] = pd.to_datetime(df_filtered["Month"], errors='coerce')
            df_filtered["year_extracted"] = df_filtered["Month"].dt.year
            year_col = "year_extracted"
        elif "year" in columns_lower:
            year_col = col_map["year"]
        else:
            year_col = None

        selected_years_int = []
        if year_col:
            unique_years = sorted(df_filtered[year_col].dropna().unique())
            selected_years = st.multiselect("Filter by Year", ["All"] + list(map(str, unique_years)), default=["All"])
            if "All" not in selected_years:
                selected_years_int = list(map(int, selected_years))
                df_filtered = df_filtered[df_filtered[year_col].isin(selected_years_int)]
            else:
                selected_years_int = unique_years

        # Step 8 & 9: HSCode and Item + HSCode
        selected_items = []
        if "cth_hscode" in columns_lower and "item_description" in columns_lower:
            cth_col = col_map["cth_hscode"]
            item_col = col_map["item_description"]

            # Select HSCode first
            cth_hscode_options = sorted(df_filtered[cth_col].dropna().astype(str).unique())
            selected_cth = st.multiselect("Select HSCode(s)", ["All"] + cth_hscode_options, default=["All"])
            if "All" not in selected_cth:
                df_filtered = df_filtered[df_filtered[cth_col].astype(str).isin(selected_cth)]

            # Combo: HSCode + Description
            df_filtered["hs_desc_combo"] = df_filtered[cth_col].astype(str) + " : " + df_filtered[item_col].astype(str)
            item_combo_options = sorted(df_filtered["hs_desc_combo"].dropna().unique())
            selected_combos = st.multiselect("Select Item Description + HSCode", ["All"] + item_combo_options, default=item_combo_options[:1])

            selected_items = (
                df_filtered[item_col].dropna().unique().tolist()
                if "All" in selected_combos else
                [combo.split(" : ", 1)[1] for combo in selected_combos]
            )
            df_filtered = df_filtered[df_filtered[item_col].isin(selected_items)]

        st.subheader("Final Filtered Data")
        st.dataframe(df_filtered)


        # ========== CUSTOM ANALYSIS ==========
        st.markdown("### Trade Summary Analysis")
        string_cols = df_filtered.select_dtypes(include="object").columns.tolist()
        numeric_cols = df_filtered.select_dtypes(include="number").columns.tolist()

        if string_cols and numeric_cols:
            product_col = "Item_Description_cluster"
            quantity_col = "Quantity"
            value_col = selected_value_col
            importer_col = importer_country_col
            supplier_col = supplier_country_col

            if st.button("" Run Full Trade Analysis"):
                insights = perform_trade_analysis(
                    df_filtered,
                    product_col=product_col,
                    quantity_col=quantity_col,
                    value_col=value_col,
                    importer_col=importer_col,
                    supplier_col=supplier_col
                )
                for label, df_result in insights.items():
                    st.subheader(label)
                    if "Heatmap" in label:
                        fig, ax = plt.subplots(figsize=(10, 6))
                        sns.heatmap(df_result, annot=True, fmt=".1f", cmap="YlGnBu", ax=ax)
                        st.pyplot(fig)
                    else:
                        st.dataframe(df_result)
                        if df_result.shape[0] > 1 and df_result.select_dtypes(include='number').shape[1] >= 1:
                            st.bar_chart(df_result.set_index(df_result.columns[0]))
        else:
            st.warning("Not enough string or numeric columns to proceed.")
    
    
    with st.expander(" Forecast Product Price or Quantity"):
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

                            st.markdown(f" **Trend Insight:** {description}")
                            st.image(plot_buf, caption="Historical (green) vs Forecast (red)", use_container_width=True)

                            st.download_button(
                                label="Download Forecast CSV",
                                data=forecast_df.to_csv(index=False),
                                file_name=f"{item_selected}_{column_choice}_forecast.csv",
                                mime="text/csv"
                            )
                        else:
                            st.warning(description)


    with st.expander(" Comparative Quantity Analysis (Multi-Quarter Wise)"):
    # Step 1: Ensure 'Month' is datetime
        df_clustered["Month"] = pd.to_datetime(df_clustered["Month"], errors="coerce")

        # Step 2: Select Years
        available_years = sorted(df_clustered["Month"].dt.year.dropna().unique())
        selected_years = st.multiselect("Select Years", available_years, default=available_years[:2])

        if len(selected_years) < 1:
            st.warning("Please select at least one year.")
        else:
            # Step 3: Quarter Selection (Multi)
            quarter_map = {
                "Jan - Mar": [1, 2, 3],
                "Apr - Jun": [4, 5, 6],
                "Jul - Sep": [7, 8, 9],
                "Oct - Dec": [10, 11, 12]
            }
            selected_quarters = st.multiselect("Select Quarter(s)", list(quarter_map.keys()), default=["Jan - Mar"])

            if not selected_quarters:
                st.warning("Please select at least one quarter.")
            else:
                selected_months = []
                for q in selected_quarters:
                    selected_months.extend(quarter_map[q])

                # Step 4: HS Code and Item Selection
                hscode_col = "CTH_HSCODE"
                item_col = "Item_Description_cluster"
                quantity_col = "Quantity"

                available_hscodes = sorted(df_clustered[hscode_col].dropna().unique())
                selected_hscode = st.selectbox("Select HS Code", available_hscodes)

                filtered_by_hscode = df_clustered[df_clustered[hscode_col] == selected_hscode]
                filtered_by_hscode["hs_item_combo"] = (
                    filtered_by_hscode[hscode_col].astype(str) + " : " + filtered_by_hscode[item_col].astype(str)
                )

                combo_options = sorted(filtered_by_hscode["hs_item_combo"].dropna().unique())
                selected_combo = st.selectbox("Select Product Description", combo_options)
                selected_item = selected_combo.split(" : ", 1)[1]

                # Step 5: Filter data by year, quarter, HS Code, and item
                filtered_df = df_clustered[
                    (df_clustered["Month"].dt.year.isin(selected_years)) &
                    (df_clustered["Month"].dt.month.isin(selected_months)) &
                    (df_clustered[hscode_col] == selected_hscode) &
                    (df_clustered[item_col] == selected_item)
                ].copy()

                # Step 6: Add Quarter column
                filtered_df["Quarter"] = filtered_df["Month"].dt.to_period("Q").astype(str)
                filtered_df["Year"] = filtered_df["Month"].dt.year

                # Step 7: Group and summarize
                if filtered_df.empty:
                    st.warning("No data available for the selected filters.")
                else:
                    summary = (
                        filtered_df.groupby(["Year", "Quarter"])[quantity_col]
                        .sum()
                        .reset_index()
                        .rename(columns={quantity_col: "Total Quantity"})
                    )

                    st.markdown("### Comparative Quantity by Year and Quarter")
                    st.dataframe(summary)

                    # Optional: Show basic trend
                    if len(summary) >= 2:
                        st.markdown("### 📌 Observations")
                        for i in range(1, len(summary)):
                            prev = summary.iloc[i - 1]
                            curr = summary.iloc[i]
                            if curr["Total Quantity"] > prev["Total Quantity"]:
                                trend = "increased"
                            elif curr["Total Quantity"] < prev["Total Quantity"]:
                                trend = "decreased"
                            else:
                                trend = "remained the same"

                            st.markdown(f"""
                            Between **{prev['Year']} Q{prev['Quarter']}** and **{curr['Year']} Q{curr['Quarter']}**, 
                            quantity for **{selected_item}** under HS Code **{selected_hscode}** has **{trend}** 
                            from **{prev['Total Quantity']:.2f}** to **{curr['Total Quantity']:.2f}**.
                            """)


    with st.expander(" Analysis Company Wise"):
        df_clustered["Month"] = pd.to_datetime(df_clustered["Month"], errors="coerce")

        # Step 1: Select Year and Quarter Group(s)
        available_years = sorted(df_clustered["Month"].dt.year.dropna().unique())
        selected_year = st.selectbox("Select Year", available_years, key="companywise_year")

        quarter_dict = {
            "Jan–Mar": [1, 2, 3],
            "Apr–Jun": [4, 5, 6],
            "Jul–Sep": [7, 8, 9],
            "Oct–Dec": [10, 11, 12]
        }
        selected_quarters = st.multiselect("Select Quarter Group(s)", list(quarter_dict.keys()), default=["Jan–Mar"], key="companywise_quarters")

        if not selected_quarters:
            st.warning("Please select at least one quarter.")
        else:
            # Flatten selected months across quarters
            selected_months = [m for q in selected_quarters for m in quarter_dict[q]]

            # Step 2: Select Trade Type
            trade_types = df_clustered["Type"].dropna().unique()
            selected_trade = st.selectbox("Select Trade Type", trade_types, key="companywise_trade_type")

            # Step 3: Multi-select Companies
            supplier_col = "Supplier_Name"
            df_clustered[supplier_col] = df_clustered[supplier_col].astype(str).str.strip().str.lower()
            unique_companies = sorted(df_clustered[supplier_col].dropna().unique())
            selected_companies = st.multiselect("Select Company(s)", unique_companies, key="companywise_companies")
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
                                avg_price = company_df["Unit_Price_USD"].sum() / company_df["Quantity"].sum()


                                total_qty = company_df["Quantity"].sum()
                                hs_used = sorted(company_df[hscode_col].astype(str).unique())
                                items_used = sorted(company_df[item_col].astype(str).unique())
                                quarter_text = ", ".join(selected_quarters)

                                st.markdown(f"""
                                **Summary for `{company}`**  
                                - Year: **{selected_year}**, Quarters: **{quarter_text}**  
                                - HS Code(s): **{', '.join(hs_used)}**  
                                - Product(s): **{', '.join(items_used)}**  
                                - Trade Type: **{selected_trade}**  
                                - Total Quantity(kgs): **{total_qty:,.2f}**  
                                - Average Unit Price (USD/unit): **{avg_price:,.2f}**
                                """)
                            else:
                                st.info(f"No data found for company: {company}")
                else:
                    st.info("Please select at least one HS Code.")
            else:
                st.info("Please select at least one company.")




    st.subheader("Business Questions")
    if "CTH_HSCODE" in df_clustered.columns and "Item_Description_cluster" in df_clustered.columns:
        filtered_df["hs_item_combo"] = (
            filtered_df["CTH_HSCODE"].astype(str) + " : " + filtered_df["Item_Description"].astype(str)
        )

    # HS Code selection
    hs_options = sorted(filtered_df["CTH_HSCODE"].dropna().astype(str).unique())
    selected_hscode = st.multiselect("Select HS Code(s)", ["All"] + hs_options, default=["All"])
    if "All" not in selected_hscode:
        filtered_df = filtered_df[filtered_df["CTH_HSCODE"].astype(str).isin(selected_hscode)]

    # HS Code + Item combo
    combo_options = sorted(filtered_df["hs_item_combo"].dropna().unique())
    selected_combos = st.multiselect("Select HS Code + Item(s)", ["All"] + combo_options, default=["All"])
    if "All" not in selected_combos:
        selected_items = [combo.split(" : ", 1)[1] for combo in selected_combos]
        filtered_df = filtered_df[filtered_df["Item_Description"].isin(selected_items)]

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

     # DATA ANALYTICS SECTION
    st.subheader("Data Analytics & Insights")
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
            "cluster_summary": " Cluster Summary (Total records, sums, averages)",
            "top_clusters": " Top Clusters (Ranked by selected metric)",
            "cluster_by_category": "Cross-Analysis (Clusters vs Categories)",
            "detailed_breakdown": "Detailed Breakdown (Complete analysis by category)"
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
                    label="Download Analysis Results",
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
        st.subheader(" Quick Insights")
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
    st.subheader("Data Grouping")
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
                    label="Download Grouped Data",
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