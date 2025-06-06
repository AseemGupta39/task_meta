from fastapi import APIRouter, HTTPException
from models.pydantic_models import InputModel
from services.file_joiner import join_files
from services.filter_process import apply_filters
from utils.file_reader import createDataframe
from utils.path_util import getFullOutputPath
from utils.logger import logger
from utils.fileNameAppender import file_append
import time
from utils.sql_parser import get_things_from_sql_drive_statement
router = APIRouter()

@router.post("/process")
def process_files(request: InputModel):
    try:
        try:
            request = file_append(request)
            file_read_start_time = time.time()
            primary_file = request.files_and_join_info.primary_file
            primary_file_name = primary_file.file_name
            df_map = {primary_file_name:createDataframe(primary_file_name) }
            # logger.easyPrint(f" value print hogi ki nahi : {primary_file.derived_columns}")
            if primary_file.derived_columns:
                logger.easyPrint("Prtinting derived  columns ")
                logger.easyPrint(primary_file.derived_columns)
                # for stmt in primary_file.derived_columns:
                #     logger.easyPrint(f"stmt is => {stmt}")
                #     get_things_from_sql_drive_statement(stmt)
            if request.files_and_join_info.secondary_files:
                for secondary_file_details in request.files_and_join_info.secondary_files:
                    df_map[secondary_file_details.file_name] = createDataframe(secondary_file_details.file_name)
            file_read_end_time = time.time()
            total_file_read_time = file_read_end_time - file_read_start_time
            logger.info(f"Time taken to read the file: {total_file_read_time}")
        except Exception as e:
            logger.error(f"Error occurred during reading the file: {e}")
            raise HTTPException(status_code=404,detail=str(e))
        
        try:
            if request.filter:
                file_filter_start_time = time.time()
                for file_details in request.filter:
                    filter_file_name = file_details.file_name
                    if(filter_file_name not in df_map):
                        logger.error(" This file is not in join files ")
                        raise HTTPException(status_code=404,detail="Error occurred as file is not in join files list.")
                    df_map[filter_file_name] = apply_filters(df_map[filter_file_name],file_details.conditions,file_details.convert_condition)
                
                file_filter_end_time = time.time()
                total_file_filter_time = file_filter_end_time - file_filter_start_time
                logger.info(f"Time taken to filter the file: {total_file_filter_time}")

            logger.info("After applying filter")
        except Exception as e:
            logger.error(f"Error occurred during filtering the file: {e}")
            raise HTTPException(status_code=404,detail=str(e))

        try:
            if request.files_and_join_info.secondary_files:
                file_join_start_time = time.time()
                final_processed_df = join_files(df_map,request.files_and_join_info.primary_file,request.files_and_join_info.secondary_files)
                file_join_end_time = time.time()
                total_file_join_time = file_join_end_time - file_join_start_time
                logger.info(f"Time taken to join the file: {total_file_join_time}")
                final_processed_df.collect().write_csv(getFullOutputPath())

            else:
                final_processed_df = df_map[primary_file]
                final_processed_df.write_csv(getFullOutputPath())
        except Exception as e:
            logger.error(f"Error occurred during joining the file: {e}")
            raise HTTPException(status_code=404,detail=str(e))
        
        return {"message": getFullOutputPath()}
    except Exception as e:
        logger.error(f"API error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))