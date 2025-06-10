import polars as pl
from config import AppConfig
from database.connection import DatabaseConnection
from database.models import create_tables, YourDataTable
from database.writer import MySQLDataWriter
from core.exceptions import DatabaseConnectionError, DataWriteError, DataTransformationError
import datetime
import sys

def create_sample_polars_df(num_rows: int) -> pl.DataFrame:
    """
    ✨ Creates a dummy Polars DataFrame matching your schema for testing.
    Now generates proper datetime strings using standard Python lists first.
    """
    print(f"Creating a sample Polars DataFrame with {num_rows} rows... 🚀")
    
    # Generate datetimes that can be parsed
    start_date = datetime.datetime(2023, 1, 1, 0, 0, 0)
    datetimes = [start_date + datetime.timedelta(minutes=i) for i in range(num_rows)]
    
    # Generate data as standard Python lists
    data = {
        'data1__id': list(range(num_rows)), # Using list(range())
        'data1__value1': [i * 10 for i in range(num_rows)],
        'data1__created_at': [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in datetimes],
        'data1__dc1': [f"DC1_Region_{(i % 5) + 1}" for i in range(num_rows)],
        'data1__dc2': [f"DC2_Zone_{(i % 3) + 1}" for i in range(num_rows)],
        'data2__roll': [i + 1000 for i in range(num_rows)],
        'data2__value1': [i * 5 for i in range(num_rows)],
        'data2__created_at': [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in datetimes],
        'data_in_json__un': [i % 100 for i in range(num_rows)],
        'data_in_json__unval': [f"JSON_Val_{i % 20}" for i in range(num_rows)],
        'data_in_json__created_at': [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in datetimes],
        'Message': [f"This is a sample message for row {i}. It can be quite long, demonstrating the TEXT type." for i in range(num_rows)]
    }
    
    # Polars will infer types from these Python lists, which is usually fine.
    # If specific types are crucial from the start, you can define schema:
    # schema = {
    #     'data1__id': pl.Int64,
    #     'data1__value1': pl.Int64,
    #     'data1__created_at': pl.String, # Still string at this stage, converted later
    #     # ... other types
    # }
    # df = pl.DataFrame(data, schema=schema)
    
    df = pl.DataFrame(data)
    print("✅ Polars DataFrame created.")
    return df

def main():
    """
    🏁 Main function to orchestrate the data loading process.
    """
    print("Starting data ingestion process... 🚀")

    # 1. Initialize configuration
    app_config = AppConfig()

    # 2. Setup database connection
    db_conn = DatabaseConnection(app_config)
    try:
        engine = db_conn.get_engine()
        Session = db_conn.get_session_maker()
    except DatabaseConnectionError as e:
        print(f"🔴 Fatal Error: {e}")
        sys.exit(1) # Exit if database connection fails

    # 3. Create tables if they don't exist
    try:
        create_tables(engine)
    except Exception as e: # Catch any exception from create_tables
        print(f"🔴 Fatal Error: Could not create tables: {e}")
        sys.exit(1)

    # 4. Create or load your Polars DataFrame
    # ⚠️ Replace this with your actual pre-made Polars DataFrame
    NUM_ROWS_TO_WRITE = 1_000_000 # Example: 1 million rows for testing
    your_premade_dataframe = create_sample_polars_df(NUM_ROWS_TO_WRITE)

    # 5. Initialize the data writer with the specific implementation
    data_writer = MySQLDataWriter(engine, Session, app_config)

    # 6. Write the DataFrame to MySQL
    try:
        data_writer.write_data(
            df=your_premade_dataframe,
            table_name=app_config.MAIN_TABLE_NAME,
            batch_size=app_config.DEFAULT_BATCH_SIZE
        )
        print("\n🎉 Process completed successfully! Data loaded to MySQL. 🎉")
    except (DataTransformationError, DataWriteError) as e:
        print(f"🔴 Error during data write operation: {e}")
        sys.exit(1) # Exit if data writing fails
    except Exception as e:
        print(f"🔴 An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

