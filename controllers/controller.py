from fastapi import APIRouter, HTTPException
from models.schemas import InputModel
from services.file_joiner import join_files
from services.filter_process import apply_filters
from utils.file_reader import createDataframe # create datafame
from utils.path_util import getFullOutputPath
from utils.logger import logger
from typing import List
import time

router = APIRouter()

@router.post("/process")
def process_files(request: InputModel):
    try:
        request = file_append(request)
        file_read_start_time = time.time()
        primary_file = request.files_and_join_info.primary_file.filename
        df_map = {primary_file:createDataframe(primary_file) }
        if request.files_and_join_info.secondary_files:
            for secondary_file_details in request.files_and_join_info.secondary_files:
                df_map[secondary_file_details.file_name] = createDataframe(secondary_file_details.file_name)
        # logger.info(f"\n\n{df_map}\n\n")
        file_read_end_time = time.time()
        total_file_read_time = file_read_end_time - file_read_start_time
        logger.info(f"Time taken to read the file: {total_file_read_time}")
    
        if request.filter:
            file_filter_start_time = time.time()
            for file_details in request.filter:
                filter_file_name = file_details.file_name
                if(filter_file_name not in df_map):
                    logger.error(" This file is not in join files ")
                    raise HTTPException(status_code=404,detail="Error occurred as file is not in join files list.")
                df_map[filter_file_name] = apply_filters(df_map[filter_file_name],file_details.conditions)
            file_filter_end_time = time.time()
            total_file_filter_time = file_filter_end_time - file_filter_start_time
            logger.info(f"Time taken to filter the file: {total_file_filter_time}")

        logger.info("After applying filter")
        # logger.info(df_map)


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

        # logger.info(final_processed_df)
        
        return {"message": getFullOutputPath()}
    except Exception as e:
        logger.error(f"API error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def file_append(request:InputModel) -> InputModel:
    primary_file_name = request.files_and_join_info.primary_file.filename.split('.')[0]
    primary_file = request.files_and_join_info.primary_file
    connector = "__"
    for i in range(len(primary_file.join_columns)): #make it for all columns for this file
        primary_file.join_columns[i] =  primary_file_name + connector + primary_file.join_columns[i]
        # logger.info(primary_file.join_columns[i])
    request.files_and_join_info.primary_file = primary_file 
    # logger.info(primary_file)
    # logger.info(request.files_and_join_info.primary_file)

    logger.info('Primary file append is done.')
    secondary_files = request.files_and_join_info.secondary_files
    if secondary_files is not None:
        for i in secondary_files:
            secondary_filename = i.file_name.split('.')[0]
            for j in range(len(i.join_columns)):
                i.join_columns[j] =  secondary_filename + connector + i.join_columns[j]
                # logger.info(i.join_columns[j])
    request.files_and_join_info.secondary_files = secondary_files
    # logger.info(secondary_files)
    # logger.info(request.files_and_join_info.secondary_files)

    logger.info('Both primary and secondary filenames done')
    
    filter_conditions = request.filter
    if filter_conditions is not None:
        for i in filter_conditions:
            filename = i.file_name.split('.')[0]
            expression = i.conditions.expressions
            for j in range(len(expression)):
                expression[j] = filename + connector +expression[j]
                logger.info(expression[j])
        request.filter = filter_conditions

    # logger.info(request.filter)

    logger.info("All append work is done")

    return request