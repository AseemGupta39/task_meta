from fastapi import HTTPException
from models.pydantic_models import PrimaryFile, JoinFile
from utils.logger import logger
from typing import List,Dict
import polars as pl
import re

def make_join_statement(left_join_cols: List[str], right_join_cols: List[str]) -> str:
    if len(left_join_cols) != len(left_join_cols):
        logger.error("Joins Columns number mismatch")
        raise Exception()  # is line ko complete kar dena
    join_string = ""
    n = len(left_join_cols)
    for i in range(n):
        join_string = f"df1.{left_join_cols[i]} = df2.{right_join_cols[i]}"
        if i != n - 1:
            join_string += " and "
    return join_string

def join_files(df_map: Dict[str,pl.DataFrame], primary_info: PrimaryFile, secondary_files: list[JoinFile]) -> pl.DataFrame:
    try:
        # Clear any previous context (optional but safe in long-running apps)
        # pl.sql.clear()

        # Register the primary dataframe
        primary_df_name = primary_info.file_name
        primary_join_columns = primary_info.join_columns

        df1=df_map[primary_df_name]
        
        # pl.sql.register("primary", primary_df)

        # Iteratively join all secondary files
        for file_details in secondary_files:
            df2=df_map[file_details.file_name]
            join_columns = file_details.join_columns

            # pl.sql.register(secondary.file_name, df)

            join_expr = f"""
                SELECT * 
                FROM df1 
                {file_details.join_type} JOIN df2
                on 
            """
            logger.easyPrint(join_expr)
            join_expr += make_join_statement(primary_join_columns,join_columns)
            # logger.info(f"Executing SQL: {join_expr}")
            logger.info(f"Executing SQL: {join_expr}")
            joined_df = pl.sql(join_expr)
            # pl.sql.register("primary", joined_df)  # Update primary for next join
            df1 = joined_df

            # case_stmt = parse_case_statement(primary_info.derived_columns)
            # filter_expr = f"""
            #     SELECT *
            #     {case_stmt}
            #     FROM df1                
            # """
            # filtered_expr = pl.sql(filter_expr)

        return joined_df

    except Exception as e:
        logger.error(f"Error during file join: {e}")
        raise HTTPException(status_code=500,detail="Error occured during file joining")
    
# import re
# def parse_case_statement(stmt: str):
#     # Normalize whitespace and quotes
#     stmt = stmt.strip()

#     # Extract column name inside square brackets
#     col_match = re.search(r'(.*?)', stmt)
#     col_name = col_match.group(1) if col_match else None

#     # Extract boolean operator and comparison value
#     op_val_match = re.search(r'\s*(==|!=|>=|<=|>|<)\s*([^ ]+)', stmt)
#     operator = op_val_match.group(1) if op_val_match else None
#     comp_value = op_val_match.group(2).strip("'\"") if op_val_match else None

#     # Extract THEN and ELSE values
#     then_match = re.search(r'THEN\s+([^\s]+|\'[^\']+\'|\"[^\"]+\")', stmt, re.IGNORECASE)
#     else_match = re.search(r'ELSE\s+([^\s]+|\'[^\']+\'|\"[^\"]+\")', stmt, re.IGNORECASE)
#     then_value = then_match.group(1).strip("'\"") if then_match else None
#     else_value = else_match.group(1).strip("'\"") if else_match else None

#     # Extract table name after FROM
#     table_match = re.search(r'FROM\s+(\w+)', stmt, re.IGNORECASE)
#     table_name = table_match.group(1) if table_match else None

#     # Return as dictionary
#     return {
#         "col_name": col_name,
#         "operator": operator,
#         "comparison_value": comp_value,
#         "then_value": then_value,
#         "else_value": else_value,
#         "table_name": table_name
#     }

# print(parse_case_statement("CASE WHEN ['sarabjeet'] THEN 'that employee' ELSE 'not that employee' END FROM tab1"))