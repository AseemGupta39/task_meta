from fastapi import APIRouter, HTTPException
from models.pydantic_models import InputModel
from services.file_joiner import join_files
from services.filter_process import apply_filters
from utils.file_reader import createDataframe
from utils.path_util import getFullOutputPath
from utils.logger import logger
from utils.fileNameAppender import file_append
from utils.column_adder import add_column_on_given_condition
import time
from utils.sql_parser import (
    parse_sql_case_statement,
    get_things_from_sql_drive_statement,
)
from utils.fileNameAppender import generateColumnName

router = APIRouter()


@router.post("/process")
def process_files(request: InputModel):
    try:
        try:
            request = file_append(request)
            file_read_start_time = time.time()
            primary_file = request.files_and_join_info.primary_file
            primary_file_name = primary_file.file_name

            df_map = {primary_file_name: createDataframe(primary_file_name)}

            if primary_file.derived_columns:
                counter = 1
                for stmt in primary_file.derived_columns:
                    required_values_for_new_column = parse_sql_case_statement(
                        stmt.sql_statement
                    )
                    df_map[primary_file_name] = add_column_on_given_condition(
                        df_map[primary_file_name],
                        generateColumnName(
                            primary_file_name, "__", "dc" + str(counter)
                        ),
                        required_values_for_new_column["col_name"],
                        required_values_for_new_column["operator"],
                        required_values_for_new_column["comparison_value"],
                        required_values_for_new_column["then_value"],
                        required_values_for_new_column["else_value"],
                    )
                    counter += 1

                    # df_map[primary_file_name] = add_column_on_given_condition(df_map[primary_file_name],
                    # generateColumnName(primary_file_name.split(".")[0],"__","dc" + str(counter)),'data1__id','>',3,'H','L')

            # logger.easyPrint(df_map)
            if request.files_and_join_info.secondary_files:
                for (
                    secondary_file_details
                ) in request.files_and_join_info.secondary_files:
                    secondary_file_name = secondary_file_details.file_name
                    df_map[secondary_file_name] = createDataframe(
                        secondary_file_name
                    )
                    if secondary_file_details.derived_columns:
                        counter = 1
                        for stmt in secondary_file_details.derived_columns:
                            required_values_for_new_column = parse_sql_case_statement(
                                stmt.sql_statement
                            )
                            df_map[secondary_file_name] = add_column_on_given_condition(
                                df_map[secondary_file_name],
                                generateColumnName(
                                    secondary_file_name, "__", "dc" + str(counter)
                                ),
                                required_values_for_new_column["col_name"],
                                required_values_for_new_column["operator"],
                                required_values_for_new_column["comparison_value"],
                                required_values_for_new_column["then_value"],
                                required_values_for_new_column["else_value"],
                            )
                            counter += 1


            file_read_end_time = time.time()
            total_file_read_time = file_read_end_time - file_read_start_time
            logger.info(f"Time taken to read the file: {total_file_read_time}")
        except Exception as e:
            logger.error(f"Error occurred during reading the file: {e}")
            raise HTTPException(status_code=404, detail=str(e))

        try:
            if request.filter:
                file_filter_start_time = time.time()
                for file_details in request.filter:
                    filter_file_name = file_details.file_name
                    if filter_file_name not in df_map:
                        logger.error(" This file is not in join files ")
                        raise HTTPException(
                            status_code=404,
                            detail="Error occurred as file is not in join files list.",
                        )
                    df_map[filter_file_name] = apply_filters(
                        df_map[filter_file_name],
                        file_details.conditions,
                        file_details.convert_condition,
                    )

                file_filter_end_time = time.time()
                total_file_filter_time = file_filter_end_time - file_filter_start_time
                logger.info(f"Time taken to filter the file: {total_file_filter_time}")

            logger.info("After applying filter")
        except Exception as e:
            logger.error(f"Error occurred during filtering the file: {e}")
            raise HTTPException(status_code=404, detail=str(e))

        try:
            if request.files_and_join_info.secondary_files:
                file_join_start_time = time.time()
                final_processed_df = join_files(
                    df_map,
                    request.files_and_join_info.primary_file,
                    request.files_and_join_info.secondary_files,
                )
                file_join_end_time = time.time()
                total_file_join_time = file_join_end_time - file_join_start_time
                logger.info(f"Time taken to join the file: {total_file_join_time}")
                final_processed_df.collect().write_csv(getFullOutputPath())

            else:
                final_processed_df = df_map[primary_file]
                final_processed_df.write_csv(getFullOutputPath())

        except Exception as e:
            logger.error(f"Error occurred during joining the file: {e}")
            raise HTTPException(status_code=404, detail=str(e))
        # message need to be added here before iteratilevely

        return {"message": getFullOutputPath()}
    except Exception as e:
        logger.error(f"API error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
