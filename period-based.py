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
