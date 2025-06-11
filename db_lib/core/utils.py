import polars as pl
from typing import Dict
from utils.logger import logger

def rename_polars_columns_for_mysql(df: pl.DataFrame) -> pl.DataFrame:
    """
    🔄 Renames Polars DataFrame columns to match MySQL table column names.
    Converts 'dataX__colY' to 'dataX_colY' and standardizes 'Message'.
    This adheres to a specific naming convention often used in SQL.
    """
    print("Renaming Polars DataFrame columns for MySQL compatibility... 🔄")
    
    # Define a mapping for explicit renames
    column_mapping = {
        'data_lakh1__name': 'data1_name',
        'data_lakh1__value': 'data1_value',
        'data_lakh1__created_at': 'data1_created_at',
        'data_lakh1__dc1': 'data1_dc1',
        # 'data_lakh1__dc2': 'data1_dc2',
        
        'data_lakh2__name': 'data2_name',
        'data_lakh2__value': 'data2_value',
        'data_lakh2__created_at': 'data2_created_at',
        'data_lakh2__dc1': 'data2_dc1',
        
        'data_lakh3__name': 'data3_name',
        'data_lakh3__value': 'data3_value',
        'data_lakh3__created_at': 'data3_created_at',
        'data_lakh3__dc1': 'data3_dc1',
        # 'Message': 'Message' # Keep as is, or rename if desired
    }

    # Ensure all expected columns are present before renaming
    missing_columns = [col for col in column_mapping.keys() if col not in df.columns]
    # logger.easyPrint("Printing cols")
    # logger.easyPrint(missing_columns)
    # logger.easyPrint(df.columns)
    # logger.easyPrint(column_mapping.keys())
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

