import polars as pl
from models.schemas import PrimaryFile, JoinFile
from utils.logger import logger


def join_files(df_map: dict, primary_info: PrimaryFile, secondary_files: list[JoinFile]) -> pl.DataFrame:
    try:
        # Clear any previous context (optional but safe in long-running apps)
        # pl.sql.clear()

        # Register the primary dataframe
        primary_df_name = primary_info.filename
        primary_join_columns = primary_info.join_columns

        df1=df_map[primary_df_name]
        
        # pl.sql.register("primary", primary_df)

        # Iteratively join all secondary files
        for secondary in secondary_files:
            df2=df_map[secondary.file_name]
            # pl.sql.register(secondary.file_name, df)

            join_expr = f"""
                SELECT * 
                FROM df1 
                inner JOIN df2
                on df1.id = df2.roll;
            """
            logger.info(f"Executing SQL: {join_expr}")
            joined_df = pl.sql(join_expr)
            # pl.sql.register("primary", joined_df)  # Update primary for next join
            df1 = joined_df

        return joined_df

    except Exception as e:
        logger.error(f"Error during file join: {e}")
        return pl.DataFrame()

# import polars as pl
# from models.schemas import PrimaryFile, JoinFile
# from utils.logger import logger
# from typing import List

# def make_join_statement(left_join_cols:List[str],right_join_cols:List[str]) -> str:
#     if(len(left_join_cols) != len(left_join_cols)):
#         logger.error("Joins Columns number mismatch")
#         raise Exception() # is line ko complete kar dena
#     join_string = ""
#     n = len(left_join_cols)
#     for i in range(n):
#         join_string = f"df1.{left_join_cols[i]} = df2.{right_join_cols[i]}"
#         if(i != n-2):
#             join_string += " and "

# def join_files(
#     df_map: dict, primary_info: PrimaryFile, secondary_files: list[JoinFile]
# ) -> pl.DataFrame:
#     try:
#         # Register the primary dataframe
#         primary_df_name = primary_info.filename
#         primary_join_columns = primary_info.join_columns

#         df1 = df_map[primary_df_name]

#         # Iteratively join all secondary files
#         for file_details in secondary_files:
#             df2 = df_map[file_details.file_name]
#             join_type = file_details.join_type
#             join_columns = file_details.join_columns

#             join_expr = f"""
#                 SELECT * 
#                 FROM df1 
#                 {join_type} JOIN df2
#                 on ;
#             """
#             join_expr += make_join_statement(primary_join_columns,join_columns)
#             logger.info(f"Executing SQL: {join_expr}")
#             joined_df = pl.sql(join_expr)
#             df1 = joined_df

#         return joined_df

#     except Exception as e:
#         logger.error(f"Error during file join: {e}")
#         raise Exception() # raise here too