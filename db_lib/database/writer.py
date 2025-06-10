# import polars as pl
# import pandas as pd
# from sqlalchemy.engine import Engine
# from sqlalchemy.orm import sessionmaker
# from core.interfaces import IDataWriter
# from core.exceptions import DataWriteError, DataTransformationError
# from core.utils import rename_polars_columns_for_mysql, convert_datetime_columns
# from config import AppConfig # To get the table name
# from database.models import YourDataTable # To get the table name dynamically

# class MySQLDataWriter(IDataWriter):
#     """
#     ✍️ Implements IDataWriter for writing Polars DataFrames to MySQL using SQLAlchemy.
#     Uses efficient batch insertion via pandas.to_sql.
#     """
#     def __init__(self, engine: Engine, session_maker: sessionmaker, config: AppConfig):
#         self.engine = engine
#         self.session_maker = session_maker
#         self.config = config
#         # Define the datetime columns that need conversion
#         self.datetime_columns = [
#             'data1__created_at', # Original name in Polars
#             'data2__created_at',
#             'data_in_json__created_at'
#         ]

#     def write_data(self, df: pl.DataFrame, table_name: str, batch_size: int = None):
#         """
#         Writes a Polars DataFrame to the specified MySQL table using batch insertion.
#         Converts specified columns to datetime objects before writing.
#         """
#         print(f"Starting batch insertion to MySQL table '{table_name}'...")
        
#         if batch_size is None:
#             batch_size = self.config.DEFAULT_BATCH_SIZE

#         # Step 1: Convert specified columns to datetime type in Polars
#         try:
#             df_with_datetime = convert_datetime_columns(df, self.datetime_columns)
#         except ValueError as e:
#             raise DataTransformationError(f"Error during datetime conversion: {e}") from e

#         # Step 2: Rename Polars DataFrame columns to match MySQL table column names
#         try:
#             df_mysql_ready = rename_polars_columns_for_mysql(df_with_datetime)
#         except ValueError as e:
#             raise DataTransformationError(f"Error during column renaming: {e}") from e

#         # Step 3: Convert Polars DataFrame to Pandas DataFrame for compatibility with .to_sql
#         # For very large DFs that don't fit in memory, iterate over Polars batches
#         # and convert each batch to pandas, then write.
#         # For 1M rows, converting once is often acceptable.
#         try:
#             pandas_df = df_mysql_ready.to_pandas()
#             print(f"Converted Polars DataFrame to Pandas DataFrame with {len(pandas_df)} rows.")
#         except Exception as e:
#             raise DataTransformationError(f"Error converting Polars to Pandas DataFrame: {e}") from e

#         # Step 4: Use pandas .to_sql method for efficient batch writing
#         try:
#             # `chunksize` parameter in to_sql enables batching
#             pandas_df.to_sql(
#                 name=table_name,
#                 con=self.engine,
#                 if_exists='append', # 'append', 'replace', 'fail'
#                 index=False, # Don't write the pandas DataFrame index as a column
#                 chunksize=batch_size # Key for efficient batch insertion!
#             )
#             print(f"✅ Data successfully written to '{table_name}' in MySQL in batches of {batch_size}.")
#         except Exception as e:
#             raise DataWriteError(f"❌ Error during batch insertion to MySQL: {e}") from e


import polars as pl
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from core.interfaces import IDataWriter
from core.exceptions import DataWriteError, DataTransformationError
from core.utils import rename_polars_columns_for_mysql, convert_datetime_columns
from config import AppConfig
from database.models import YourDataTable # Import the ORM model

class MySQLDataWriter(IDataWriter):
    """
    ✍️ Implements IDataWriter for writing Polars DataFrames to MySQL using
    direct SQLAlchemy ORM batch insertion.
    """
    def __init__(self, engine: Engine, session_maker: sessionmaker, config: AppConfig):
        self.engine = engine
        self.session_maker = session_maker
        self.config = config
        # Define the datetime columns that need conversion
        self.datetime_columns = [
            'data1__created_at', # Original name in Polars
            'data2__created_at',
            'data_in_json__created_at'
        ]

    def write_data(self, df: pl.DataFrame, table_name: str, batch_size: int = None):
        """
        Writes a Polars DataFrame to the specified MySQL table using
        direct SQLAlchemy ORM batch insertion.
        Converts specified columns to datetime objects before writing.
        """
        print(f"Starting direct SQLAlchemy ORM batch insertion to MySQL table '{table_name}'...")
        
        if batch_size is None:
            batch_size = self.config.DEFAULT_BATCH_SIZE

        # Step 1: Convert specified columns to datetime type in Polars
        try:
            df_with_datetime = convert_datetime_columns(df, self.datetime_columns)
        except ValueError as e:
            raise DataTransformationError(f"Error during datetime conversion: {e}") from e

        # Step 2: Rename Polars DataFrame columns to match MySQL table column names (ORM field names)
        # The ORM model column names (e.g., data1_id) must match the DataFrame column names after renaming.
        try:
            df_orm_ready = rename_polars_columns_for_mysql(df_with_datetime)
        except ValueError as e:
            raise DataTransformationError(f"Error during column renaming: {e}") from e

        # Step 3: Iterate through DataFrame in chunks and insert using SQLAlchemy ORM
        total_rows = len(df_orm_ready)
        current_row = 0
        
        # Use a list of column names from the ORM model for dictionary creation order
        # This ensures the dictionary keys match the ORM attributes
        # We exclude 'id' as it's auto-incrementing and not in the source DataFrame
        orm_column_names = [
            'data1_id', 'data1_value1', 'data1_created_at', 'data1_dc1', 'data1_dc2',
            'data2_roll', 'data2_value1', 'data2_created_at',
            'data_in_json_un', 'data_in_json_unval', 'data_in_json_created_at',
            'Message'
        ]

        # Use Polars' iter_slices for efficient chunking without converting to Pandas upfront
        for i in range(0, total_rows, batch_size):
            batch_df = df_orm_ready.slice(i, batch_size)
            print(f"Processing batch from row {i} to {min(i + batch_size, total_rows)}...")

            # Convert each row in the batch to a dictionary, then to an ORM object
            # Polars' to_dicts() is efficient for this.
            orm_objects = []
            for row_dict in batch_df.to_dicts():
                # Ensure the dictionary keys match the ORM model attributes
                # Also handle datetime objects correctly if they were strings in source
                # (convert_datetime_columns in utils already handles this for Polars,
                # SQLAlchemy ORM will handle the Python datetime object to DB DATETIME)
                try:
                    # Create an ORM object, unpacking the dictionary.
                    # Ensure 'id' is NOT in row_dict as it's auto-incrementing.
                    orm_objects.append(YourDataTable(**{k: row_dict[k] for k in orm_column_names}))
                except Exception as e:
                    print(f"⚠️ Warning: Could not create ORM object for row {row_dict}. Skipping. Error: {e}")
                    # You might want more sophisticated error handling here, e.g., logging bad rows
                    continue
            
            if not orm_objects:
                print("No valid ORM objects in this batch. Skipping insertion.")
                continue

            session:Session = self.session_maker()
            try:
                session.add()
                session.bulk_save_objects(orm_objects) # Efficiently save objects in bulk
                session.commit() # Commit the batch
                current_row += len(orm_objects)
                print(f"  ✅ Batch inserted. Total rows processed: {current_row}/{total_rows}")
            except Exception as e:
                session.rollback() # Rollback the transaction on error
                raise DataWriteError(f"❌ Error during SQLAlchemy ORM batch insertion for chunk starting at row {i}: {e}") from e
            finally:
                session.close() # Always close the session

        print(f"✅ All {total_rows} rows successfully written to '{table_name}' in MySQL using SQLAlchemy ORM.")
