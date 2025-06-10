import polars as pl
from typing import Dict

def rename_polars_columns_for_mysql(df: pl.DataFrame) -> pl.DataFrame:
    """
    🔄 Renames Polars DataFrame columns to match MySQL table column names.
    Converts 'dataX__colY' to 'dataX_colY' and standardizes 'Message'.
    This adheres to a specific naming convention often used in SQL.
    """
    print("Renaming Polars DataFrame columns for MySQL compatibility... 🔄")
    
    # Define a mapping for explicit renames
    column_mapping = {
        'data1__id': 'data1_id',
        'data1__value1': 'data1_value1',
        'data1__created_at': 'data1_created_at',
        'data1__dc1': 'data1_dc1',
        'data1__dc2': 'data1_dc2',
        'data2__roll': 'data2_roll',
        'data2__value1': 'data2_value1',
        'data2__created_at': 'data2_created_at',
        'data_in_json__un': 'data_in_json_un',
        'data_in_json__unval': 'data_in_json_unval',
        'data_in_json__created_at': 'data_in_json_created_at',
        'Message': 'Message' # Keep as is, or rename if desired
    }

    # Ensure all expected columns are present before renaming
    missing_columns = [col for col in column_mapping.keys() if col not in df.columns]
    if missing_columns:
        raise ValueError(f"❌ Missing expected columns in DataFrame: {missing_columns}. Please check your input DataFrame schema.")

    df_renamed = df.rename(column_mapping)
    print("✅ Columns renamed.")
    return df_renamed

def convert_datetime_columns(df: pl.DataFrame, datetime_cols: list[str]) -> pl.DataFrame:
    """
    Converts specified string columns in a Polars DataFrame to datetime objects.
    Polars handles timezone-naive datetimes as 'datetime[ns]'.
    """
    print(f"Converting specified columns to Datetime: {datetime_cols}...")
    for col in datetime_cols:
        if col in df.columns:
            try:
                # Attempt to parse common datetime formats
                df = df.with_columns(
                    pl.col(col).str.to_datetime(
                        format=None, # Auto-detect format
                        strict=False # Allow some parsing errors, resulting in nulls
                    ).alias(col)
                )
            except Exception as e:
                raise ValueError(f"❌ Could not convert column '{col}' to datetime: {e}")
        else:
            print(f"⚠️ Warning: Datetime column '{col}' not found in DataFrame. Skipping conversion.")
    print("✅ Datetime conversion complete.")
    return df

