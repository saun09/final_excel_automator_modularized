import pandas as pd
from data_cleaning import safe_numeric_conversion


def group_data(df, group_by_columns, aggregation_rules=None):
    """
    Group data by specified columns with optional aggregations.
    
    Args:
        df: DataFrame to group
        group_by_columns: List of columns to group by
        aggregation_rules: Dictionary of {column: aggregation_function}
                           If None, will default to count aggregation
    
    Returns:
        Grouped DataFrame
    """
    if not group_by_columns:
        return df
    
    # Default to count aggregation if no rules provided
    if aggregation_rules is None:
        aggregation_rules = {'__count__': 'size'}
    
    try:
        grouped_df = df.groupby(group_by_columns).agg(aggregation_rules).reset_index()
        return grouped_df
    except Exception as e:
        st.error(f"Error during grouping: {str(e)}")
        return df


def perform_cluster_analysis(df, cluster_col, analysis_type, target_col=None, group_by_col=None, selected_clusters=None):
    """Perform various types of analysis on clustered data"""
    
    if cluster_col not in df.columns:
        return None, "Cluster column not found"
    
    # Filter by selected clusters if specified
    if selected_clusters:
        df_filtered = df[df[cluster_col].isin(selected_clusters)]
    else:
        df_filtered = df
    
    if df_filtered.empty:
        return None, "No data found for selected clusters"
    
    try:
        if analysis_type == "cluster_summary":
            # Basic cluster summary
            result = df_filtered.groupby(cluster_col).agg({
                cluster_col: 'count'
            }).rename(columns={cluster_col: 'Total_Records'})
            
            if target_col and target_col in df_filtered.columns:
                numeric_data = safe_numeric_conversion(df_filtered[target_col])
                df_temp = df_filtered.copy()
                df_temp[f'{target_col}_numeric'] = numeric_data
                
                summary = df_temp.groupby(cluster_col)[f'{target_col}_numeric'].agg([
                    'sum', 'mean', 'count'
                ]).round(2)
                summary.columns = [f'{target_col}_Total', f'{target_col}_Average', f'{target_col}_Count']
                
                result = pd.concat([result, summary], axis=1)
            
            return result, "Analysis completed successfully"
        
        elif analysis_type == "top_clusters":
            if not target_col or target_col not in df_filtered.columns:
                return None, "Target column required for top clusters analysis"
            
            numeric_data = safe_numeric_conversion(df_filtered[target_col])
            df_temp = df_filtered.copy()
            df_temp[f'{target_col}_numeric'] = numeric_data
            
            result = df_temp.groupby(cluster_col)[f'{target_col}_numeric'].sum().sort_values(ascending=False).head(10)
            result = result.to_frame(f'Total_{target_col}')
            
            return result, "Top clusters analysis completed"
        
        elif analysis_type == "cluster_by_category":
            if not group_by_col or group_by_col not in df_filtered.columns:
                return None, "Group by column required for categorical analysis"
            
            if target_col and target_col in df_filtered.columns:
                numeric_data = safe_numeric_conversion(df_filtered[target_col])
                df_temp = df_filtered.copy()
                df_temp[f'{target_col}_numeric'] = numeric_data
                
                result = df_temp.groupby([cluster_col, group_by_col])[f'{target_col}_numeric'].sum().unstack(fill_value=0)
            else:
                result = df_filtered.groupby([cluster_col, group_by_col]).size().unstack(fill_value=0)
            
            return result, "Categorical analysis completed"
        
        elif analysis_type == "detailed_breakdown":
            if not group_by_col or group_by_col not in df_filtered.columns:
                return None, "Group by column required for detailed breakdown"
            
            result_list = []
            
            for cluster in df_filtered[cluster_col].unique():
                cluster_data = df_filtered[df_filtered[cluster_col] == cluster]
                
                breakdown = cluster_data.groupby(group_by_col).agg({
                    cluster_col: 'count'
                }).rename(columns={cluster_col: 'Record_Count'})
                
                if target_col and target_col in df_filtered.columns:
                    numeric_data = safe_numeric_conversion(cluster_data[target_col])
                    cluster_data_temp = cluster_data.copy()
                    cluster_data_temp[f'{target_col}_numeric'] = numeric_data
                    
                    summary = cluster_data_temp.groupby(group_by_col)[f'{target_col}_numeric'].sum()
                    breakdown[f'Total_{target_col}'] = summary
                
                breakdown['Cluster'] = cluster
                result_list.append(breakdown.reset_index())
            
            if result_list:
                result = pd.concat(result_list, ignore_index=True)
                return result, "Detailed breakdown completed"
            else:
                return None, "No data to analyze"
        
    except Exception as e:
        return None, f"Analysis error: {str(e)}"
    
    return None, "Unknown analysis type"