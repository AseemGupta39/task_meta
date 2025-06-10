from fastapi import HTTPException
from models.pydantic_models import PrimaryFile, JoinFile
from utils.logger import logger
from typing import List,Dict
import polars as pl

def make_join_statement(left_join_cols: List[str], right_join_cols: List[str]) -> str:
    if len(left_join_cols) != len(right_join_cols):
        logger.error("Joins Columns number mismatch")
        raise Exception("Joins Columns number mismatch")  # is line ko complete kar dena
    join_string = ""
    n = len(left_join_cols)
    for i in range(n):
        join_string = f"df1.{left_join_cols[i]} = df2.{right_join_cols[i]}"
        if i != n - 1:
            join_string += " and "
    return join_string

def join_files(df_map: Dict[str,pl.DataFrame], primary_info: PrimaryFile, secondary_files: list[JoinFile]) -> pl.DataFrame:
    try:
        primary_df_name = primary_info.file_name
        primary_join_columns = primary_info.join_columns

        df1=df_map[primary_df_name]

        # Iteratively join all secondary files
        for file_details in secondary_files:
            df2=df_map[file_details.file_name]
            join_columns = file_details.join_columns

            join_expr = f"""
                SELECT * 
                FROM df1 
                {file_details.join_type} JOIN df2
                on 
            """

            join_expr += make_join_statement(primary_join_columns,join_columns)
            # logger.info(f"Executing SQL: {join_expr}")
            joined_df = pl.sql(join_expr)

            df1 = joined_df

        return joined_df

    except Exception as e:
        logger.error(f"Error during file join: {e}")
        raise HTTPException(status_code=500,detail="Error occured during file joining")
