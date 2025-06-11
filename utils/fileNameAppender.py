# from models.pydantic_models import InputModel
# from utils.logger import logger
# from utils.sql_parser import parse_sql_case_statement

# def generateColumnName(file_name: str, connector: str, col_name: str) -> str:
#     return file_name + connector + col_name


# def file_append(request: InputModel) -> InputModel:
#     primary_file = request.files_and_join_info.primary_file
#     primary_file_name = primary_file.file_name.split(".")[0]
#     connector = "__"
#     for filter_detail in range(
#         len(primary_file.join_columns)
#     ):  # make it for all columns for this file
#         primary_file.join_columns[filter_detail] = generateColumnName(
#             primary_file_name, connector, primary_file.join_columns[filter_detail]
#         )
#     request.files_and_join_info.primary_file = primary_file
#     if primary_file.derived_columns:
#         derived_cols = primary_file.derived_columns
#         for filter_detail in range(len(derived_cols)):
#             result = parse_sql_case_statement(
#                 derived_cols[filter_detail].sql_statement
#             )  # check here for string then value this might remove the quetes => ''
#             statement = f"CASE WHEN [{generateColumnName(primary_file_name,connector,result['col_name'])}] {result['operator']} {result['comparison_value']} THEN '{result['then_value']}' ELSE '{result['else_value']}' END FROM {result['table_name']}"
#             derived_cols[filter_detail].sql_statement = statement
#             # logger.easyPrint(statement)
#         primary_file.derived_columns = derived_cols


#     # bas idhar check karna hai then else value ko check karna hai aur us hisab se quote lagana hai 
#     secondary_files = request.files_and_join_info.secondary_files
#     if secondary_files is not None:
#         for filter_detail in range(len(secondary_files)):
#             secondary_filename = secondary_files[filter_detail].file_name.split(".")[0]
#             for j in range(len(secondary_files[filter_detail].join_columns)):
#                 secondary_files[filter_detail].join_columns[j] = generateColumnName(
#                     secondary_filename, connector, secondary_files[filter_detail].join_columns[j]
#                 )
#             if secondary_files[filter_detail].derived_columns:
#                 derived_cols = secondary_files[filter_detail].derived_columns
#                 for j in range(len(derived_cols)):
#                     result = parse_sql_case_statement(
#                         derived_cols[j].sql_statement
#                     )  # check here for string then value this might remove the quetes => ''
#                     statement = f"CASE WHEN [{generateColumnName(secondary_filename,connector,result['col_name'])}] {result['operator']} {result['comparison_value']} THEN '{result['then_value']}' ELSE '{result['else_value']}' END FROM {result['table_name']}"
#                     derived_cols[j].sql_statement = statement
#                     # logger.easyPrint(statement)
#                 secondary_files[filter_detail].derived_columns = derived_cols
#                 logger.easyPrint(secondary_files[filter_detail].derived_columns)

#     request.files_and_join_info.secondary_files = secondary_files

#     filter_conditions = request.filter
#     if filter_conditions is not None:
#         for filter_detail in filter_conditions:
#             filename = filter_detail.file_name.split(".")[0]
#             if filter_detail.convert_condition is not None:
#                 filter_detail.convert_condition.column_name = generateColumnName(
#                     filename, connector, filter_detail.convert_condition.column_name
#                 )
#             expression = filter_detail.conditions.expressions
#             for j in range(len(expression)):
#                 expression[j] = generateColumnName(filename, connector, expression[j])
#         request.filter = filter_conditions
#     return request


from models.pydantic_models import InputModel
from utils.logger import logger
from utils.sql_parser import parse_sql_case_statement

def generateColumnName(file_name: str, connector: str, col_name: str) -> str:
    return file_name + connector + col_name

def quote_if_string(value: str) -> str:
    try:
        float(value)
        return value
    except ValueError:
        return f"'{value}'"

def file_append(request: InputModel) -> InputModel:
    primary_file = request.files_and_join_info.primary_file
    primary_file_name = primary_file.file_name.split(".")[0]
    connector = "__"

    for i in range(len(primary_file.join_columns)):
        primary_file.join_columns[i] = generateColumnName(primary_file_name, connector, primary_file.join_columns[i])

    if primary_file.derived_columns:
        derived_cols = primary_file.derived_columns
        for i in range(len(derived_cols)):
            result = parse_sql_case_statement(derived_cols[i].sql_statement)
            statement = (
                f"CASE WHEN [{generateColumnName(primary_file_name, connector, result['col_name'])}] "
                f"{result['operator']} {result['comparison_value']} "
                f"THEN {quote_if_string(result['then_value'])} "
                f"ELSE {quote_if_string(result['else_value'])} "
                f"END FROM {result['table_name']}"
            )
            logger.easyPrint(statement)
            derived_cols[i].sql_statement = statement
        primary_file.derived_columns = derived_cols

    secondary_files = request.files_and_join_info.secondary_files
    if secondary_files is not None:
        for i in range(len(secondary_files)):
            secondary_filename = secondary_files[i].file_name.split(".")[0]
            for j in range(len(secondary_files[i].join_columns)):
                secondary_files[i].join_columns[j] = generateColumnName(
                    secondary_filename, connector, secondary_files[i].join_columns[j]
                )
            if secondary_files[i].derived_columns:
                derived_cols = secondary_files[i].derived_columns
                for j in range(len(derived_cols)):
                    result = parse_sql_case_statement(derived_cols[j].sql_statement)
                    statement = (
                        f"CASE WHEN [{generateColumnName(secondary_filename, connector, result['col_name'])}] "
                        f"{result['operator']} {result['comparison_value']} "
                        f"THEN {quote_if_string(result['then_value'])} "
                        f"ELSE {quote_if_string(result['else_value'])} "
                        f"END FROM {result['table_name']}"
                    )
                    derived_cols[j].sql_statement = statement
                secondary_files[i].derived_columns = derived_cols
                logger.easyPrint(secondary_files[i].derived_columns)

    request.files_and_join_info.primary_file = primary_file
    request.files_and_join_info.secondary_files = secondary_files

    filter_conditions = request.filter
    if filter_conditions is not None:
        for filter_detail in filter_conditions:
            filename = filter_detail.file_name.split(".")[0]
            if filter_detail.convert_condition is not None:
                filter_detail.convert_condition.column_name = generateColumnName(
                    filename, connector, filter_detail.convert_condition.column_name
                )
            expressions = filter_detail.conditions.expressions
            for j in range(len(expressions)):
                expressions[j] = generateColumnName(filename, connector, expressions[j])
        request.filter = filter_conditions

    return request