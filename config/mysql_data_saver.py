# import polars as pl
# from config.db import SessionLocal
# from config.models import ParsedValues

# def save_to_mysql(parsed_dict):
#     session = SessionLocal()
#     try:
#         parsed = ParsedValues(
#             name1 = parsed_dict['data_million3__name'],
#             value1 =  parsed_dict['data_million3__value'],
#             quantity1 = parsed_dict['data_million3__quantity'],
#             created_at1 = parsed_dict['data_million3__created_at'],
#             name2 = parsed_dict['data_million4__name'],
#             value2 = parsed_dict['data_million4__value'],
#             quantity2 = parsed_dict['data_million4__quantity'],
#             created_at2 = parsed_dict['data_million4__created_at'],
#             name3 = parsed_dict['data_million5__name'],
#             value3 = parsed_dict['data_million5__value'],
#             quantity3 = parsed_dict['data_million5__quantity'],
#             created_at3 = parsed_dict['data_million5__created_at']
#         )
#         session.add(parsed)
#         session.commit()
#         print("Data saved to MySQL.")
#     except Exception as e:
#         session.rollback()
#         print("Error:", e)
#     finally:
#         session.close()

# save_to_mysql(pl.read_csv("C:\\Assignments\\Python_Team_Assignments\\task_meta\\data\\output\\processed_output.csv"))

import polars as pl
from sqlalchemy.orm import Session
from config.db import SessionLocal
from config.models import ParsedValues

def save_csv_to_db(df:pl.DataFrame):

    with SessionLocal() as session:
        for _, row in df.iter_rows(named=True):
            parsedValue = ParsedValues(
                name1 = row['data_million3__name'],
                value1 =  row['data_million3__value'],
                quantity1 = row['data_million3__quantity'],
                created_at1 = row['data_million3__created_at'],
                name2 = row['data_million4__name'],
                value2 = row['data_million4__value'],
                quantity2 = row['data_million4__quantity'],
                created_at2 = row['data_million4__created_at'],
                name3 = row['data_million5__name'],
                value3 = row['data_million5__value'],
                quantity3 = row['data_million5__quantity'],
                created_at3 = row['data_million5__created_at'],
                message = row['message']
            )
            session.add(parsedValue)
        session.commit()
    print("✅ Data saved successfully.")