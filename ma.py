# app.py

import streamlit as st
import pandas as pd
from data_cleaning import (
    fetch_supported_currencies,
    convert_sheet_to_usd,
    get_conversion_rate,
)

st.set_page_config(page_title="Currency Converter", layout="wide")
st.title("💱 Currency Conversion Tool")

# Upload CSV
uploaded_file = st.file_uploader("Upload your Excel or CSV file", type=["xlsx", "csv"])

if uploaded_file:
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    st.subheader("Preview of Uploaded Data")
    st.dataframe(df.head())

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
