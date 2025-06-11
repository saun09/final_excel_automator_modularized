import streamlit as st
import requests

base_url = "https://marketdata.tradermade.com/api/v1/convert"
api_key = "-eRFVM6ugO_vKeHx0_Yu"

def convert_currency(amount, from_currency, to_currency):
    url = f"{base_url}?api_key={api_key}&from={from_currency}&to={to_currency}&amount={amount}"
    response = requests.get(url)

    if response.status_code == 200:
        rate = response.json()["quote"]
        converted_amount = response.json()["total"]
        return rate, converted_amount
    else:
        st.error(f"Conversion API Error {response.status_code}: {response.text}")
        return None, None

def fetch_supported_currencies():
    url = f"https://marketdata.tradermade.com/api/v1/live_currencies_list?api_key={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        currencies_data = response.json()
        if "available_currencies" in currencies_data:
            currencies = currencies_data["available_currencies"]
            return list(currencies.keys())
        else:
            st.write("Error: 'available_currencies' key not found in response.")
            return None
    else:
        st.write(f"Error {response.status_code}: {response.text}")
        return None

def currency_converter():
    st.title("💱 Real-Time Currency Converter")

    amount = st.number_input("Enter an integer amount to convert:", value=100, step=1)

    with st.spinner("Fetching supported currencies..."):
        supported_currencies = fetch_supported_currencies()

    if supported_currencies is not None:
        from_currency = st.selectbox("From currency:", supported_currencies, index=19)
        to_currencies = st.multiselect("To currencies:", supported_currencies, default=["USD"])

        if st.button("Convert"):
            st.write("Conversion results:")
            for to_currency in to_currencies:
                try:
                    rate, converted_amount = convert_currency(amount, from_currency, to_currency)
                    if rate:
                        st.success(f"{amount} {from_currency} = {converted_amount} {to_currency} "
                                   f"(1 {from_currency} = {rate} {to_currency})")
                    else:
                        st.warning(f"Rate unavailable for {from_currency} to {to_currency}.")
                except Exception as e:
                    st.error(f"Error: Could not convert {from_currency} to {to_currency}. {e}")

if __name__ == "__main__":
    currency_converter()
