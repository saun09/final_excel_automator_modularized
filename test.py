
import streamlit as st
import pandas as pd

# Custom parser function for month formats like "Apr--2020"
def parse_custom_month_format(text):
    try:
        clean_text = text.replace('--', ' ').strip()
        return pd.to_datetime(clean_text, format="%b %Y", errors="coerce")
    except:
        return pd.NaT

st.set_page_config(page_title="Month Column Parser", layout="wide")
st.title("🧪 Test: Custom Month Column Parsing")

uploaded_file = st.file_uploader("Upload your Excel or CSV file", type=["csv", "xlsx"])

if uploaded_file:
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    st.subheader("📄 Preview of Uploaded Data")
    st.dataframe(df.head())

    month_col = st.selectbox("📅 Select the column with Month format like 'Apr--2020'", df.columns)
    
    if st.button("🔍 Parse Month Column"):
        df["Parsed_Date"] = df[month_col].apply(parse_custom_month_format)
        df["Parsed_Date"] = pd.to_datetime(df["Parsed_Date"], errors="coerce")
        
        st.write("🗓️ Original Values:", df[month_col].dropna().unique()[:10])
        st.write("📅 Parsed Dates:", df["Parsed_Date"].dropna().astype(str).unique()[:10])
        st.write("Parsed_Date dtype:", df["Parsed_Date"].dtype)
        
        st.subheader("✅ Resulting DataFrame")
        st.dataframe(df[[month_col, "Parsed_Date"]])
