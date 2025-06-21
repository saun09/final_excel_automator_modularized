def perform_trade_analysis(df, product_col, quantity_col, value_col, importer_col, supplier_col):
    analysis = {}

    if df.empty:
        return {"error": "No data available for analysis."}

    # Top products by quantity
    product_summary = (
        df.groupby(product_col)[quantity_col]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    analysis["Top Products by Quantity"] = product_summary

    # Top suppliers
    supplier_summary = (
        df.groupby(supplier_col)[quantity_col]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    analysis["Top Suppliers"] = supplier_summary

    # Top importers
    importer_summary = (
        df.groupby(importer_col)[quantity_col]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    analysis["Top Importers"] = importer_summary

    # Unit price (value/quantity) by product
    df["unit_price"] = df[value_col] / df[quantity_col]
    unit_price_summary = (
        df.groupby(product_col)["unit_price"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    analysis["Average Unit Price by Product"] = unit_price_summary

    return analysis