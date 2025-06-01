from fastapi import APIRouter, HTTPException
from models.schemas import InputModel
from services.file_joiner import join_files
from services.filter_process import apply_filters
from utils.file_reader import createDataframe # create datafame
from utils.path_util import getFullOutputPath
from utils.logger import logger


router = APIRouter()

@router.post("/process")
def process_files(request: InputModel):
    try:
        primary_file = request.files_and_join_info.primary_file.filename
        df_map = {primary_file:createDataframe(primary_file) }
        if request.files_and_join_info.secondary_files:
            for secondary_file_details in request.files_and_join_info.secondary_files:
                df_map[secondary_file_details.file_name] = createDataframe(secondary_file_details.file_name)
        
    
        if request.filter:
            for file_details in request.filter:
                filter_file_name = file_details.file_name
                if(filter_file_name not in df_map):
                    logger.error(" This file is not in join files ")
                df_map[filter_file_name] = apply_filters(df_map[filter_file_name],file_details.conditions)

        logger.info("After applying filter")
        logger.info(df_map)

        final_processed_df = join_files(df_map,request.files_and_join_info.primary_file,request.files_and_join_info.secondary_files)
        logger.info(final_processed_df)
        final_processed_df.collect().write_csv(getFullOutputPath())

        return {"message": getFullOutputPath()}
    except Exception as e:
        logger.error(f"API error: {e}", exc_info=True)
        return {"error came":"yes"}
        # raise HTTPException(status_code=500, detail=str(e))