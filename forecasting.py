from prophet import Prophet
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import matplotlib.dates as mdates

def forecast_item(df, item_name, value_column, cluster_col, date_col="Month"):
    try:
        # Filter data for the selected item
        filtered_df = df[df[cluster_col] == item_name].copy()

        # Convert date column to datetime
        filtered_df[date_col] = pd.to_datetime(filtered_df[date_col], errors='coerce')
        filtered_df = filtered_df.dropna(subset=[date_col, value_column])

        # Ensure numeric values
        filtered_df[value_column] = pd.to_numeric(filtered_df[value_column], errors='coerce')
        filtered_df = filtered_df.dropna(subset=[value_column])

        # Group by Month and fill missing months
        monthly_df = filtered_df.groupby(pd.Grouper(key=date_col, freq='M'))[value_column].sum().reset_index()

        # Fill missing months with zero
        all_months = pd.date_range(start=monthly_df[date_col].min(), end=monthly_df[date_col].max(), freq='M')
        monthly_df = monthly_df.set_index(date_col).reindex(all_months, fill_value=0).rename_axis("ds").reset_index()
        monthly_df = monthly_df.rename(columns={value_column: "y"})

        if len(monthly_df) < 6:
            return None, "Not enough monthly data to reliably forecast. Please ensure at least 6 data points.", None

        # Fit Prophet
        model = Prophet()
        model.fit(monthly_df)

        # Forecast next 12 months
        future = model.make_future_dataframe(periods=12, freq='M')
        forecast = model.predict(future)

        forecast_df = forecast[["ds", "yhat"]].tail(12)
        forecast_df = forecast_df.rename(columns={"yhat": value_column})

        # Trend analysis
        trend = forecast_df[value_column].diff().mean()
        if trend > 0:
            description = "📈 Increasing trend in forecasted values."
        elif trend < 0:
            description = "📉 Decreasing trend in forecasted values."
        else:
            description = "⚖️ No significant trend detected in forecast."

        # Plot historical (green) and forecast (red) with proper x-axis alignment
        plt.figure(figsize=(12, 6))
        sns.lineplot(data=monthly_df, x="ds", y="y", label="Historical", color="green")
        sns.lineplot(data=forecast_df, x="ds", y=value_column, label="Forecast", color="red")

        plt.title(f"{value_column} Forecast for {item_name}")
        plt.xlabel("Month")
        plt.ylabel("Quantity")
        plt.xticks(rotation=45)

        # Format x-axis as dates
        plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

        plt.legend()
        plt.tight_layout()

        buf = BytesIO()
        plt.savefig(buf, format="png")
        plt.close()
        buf.seek(0)

        return forecast_df, description, buf

    except Exception as e:
        return None, f"Forecast error: {e}", None
