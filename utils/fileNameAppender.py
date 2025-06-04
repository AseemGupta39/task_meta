from models.schemas import InputModel
from utils.logger import logger

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