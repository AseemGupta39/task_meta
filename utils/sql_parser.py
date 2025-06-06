import re
from utils.logger import logger


def _clean_captured_value(value_str: str):
    """
    Cleans the captured value:
    - Removes surrounding quotes if present.
    - Converts numeric strings to int/float if applicable.
    """
    if value_str is None:
        return None

    value_str = value_str.strip()

    # Remove surrounding single quotes for strings
    if value_str.startswith("'") and value_str.endswith("'"):
        return value_str[1:-1]

    # Try converting to float or int
    try:
        if "." in value_str:
            return float(value_str)
        return int(value_str)
    except ValueError:
        return value_str  # Return as-is (likely an unquoted string)


def parse_sql_case_statement(sql_statement: str):
    """
    Parses a specific SQL CASE statement format and extracts components.

    Returns:
        dict: Parsed parts of the CASE statement.
              Keys: col_name, operator, comparison_value, then_value, else_value, table_name.
              Returns None if parsing fails.
    """
    pattern = re.compile(
        r"""^\s*CASE\s+WHEN\s+?(?P<col_name>[^?]+)?\s*    # Column name in [brackets]
            (?P<operator>>=|<=|==|!=|>|<)\s*                 # Comparison operator
            (?P<comparison_value>'[^']*'|\d+(?:\.\d+)?)\s*   # Value: 'string' or number
            THEN\s+(?P<then_value>'[^']*'|\d+(?:\.\d+)?)\s+  # THEN value
            ELSE\s+(?P<else_value>'[^']*'|\d+(?:\.\d+)?)\s+  # ELSE value
            END\s+FROM\s+(?P<table_name>\w+)\s*$             # Table name
        """,
        re.IGNORECASE | re.VERBOSE
    )

    match = pattern.match(sql_statement.strip())
    if not match:
        print(f"🚫 Oops! The SQL statement below doesn't match the expected format:\n   '{sql_statement}'")
        return None

    parsed_data = match.groupdict()

    # Clean the captured values
    for key in ['comparison_value', 'then_value', 'else_value']:
        parsed_data[key] = _clean_captured_value(parsed_data[key])

    return parsed_data

# # --- Example Usage ---
# print("🚀 Let's start parsing some SQL CASE statements!\n")

# statements_to_test = [
#     "CASE WHEN [col_name] > 2000 THEN 'L' ELSE 'S' END FROM table_name",
#     "CASE WHEN [Name] == 'sarabjeet' THEN 'that employee' ELSE 'not that employee' END FROM tab1",
#     "  case when [Age] <= 30 then 1 else 0 end from users_table  ",
#     "CASE WHEN [Value] < 100.55 THEN 'Low' ELSE 'High' END FROM data_points",
#     "CASE WHEN [Product_ID] != 'XYZ123' THEN 'Valid' ELSE 'Obsolete' END FROM inventory",
#     "INVALID SQL STATEMENT FORMAT"
# ]

# for i, stmt in enumerate(statements_to_test):
#     print(f"📜 Parsing statement {i+1}: \"{stmt}\"")
#     result = parse_sql_case_statement(stmt)
#     if result:
#         print(f"✅ **Parsed Successfully!** ✨")
#         print(f"   - Column Name      : {result['col_name']}")
#         print(f"   - Operator         : {result['operator']}")
#         print(f"   - Comparison Value : {result['comparison_value']} (Type: {type(result['comparison_value']).__name__})")
#         print(f"   - THEN Value       : {result['then_value']} (Type: {type(result['then_value']).__name__})")
#         print(f"   - ELSE Value       : {result['else_value']} (Type: {type(result['else_value']).__name__})")
#         print(f"   - Table Name       : {result['table_name']}")
#     else:
#         print(f"❌ **Parsing Failed.**")
#     print("-" * 40 + "\n")




def get_things_from_sql_drive_statement(stmt:str):
    result = parse_sql_case_statement(stmt)
    if result:
        logger.easyPrint(f"✅ **Parsed Successfully!** ✨")
        logger.easyPrint(f"   - Column Name      : {result['col_name']}")
        logger.easyPrint(f"   - Operator         : {result['operator']}")
        logger.easyPrint(f"   - Comparison Value : {result['comparison_value']} (Type: {type(result['comparison_value']).__name__})")
        logger.easyPrint(f"   - THEN Value       : {result['then_value']} (Type: {type(result['then_value']).__name__})")
        logger.easyPrint(f"   - ELSE Value       : {result['else_value']} (Type: {type(result['else_value']).__name__})")
        logger.easyPrint(f"   - Table Name       : {result['table_name']}")
    else:
        logger.easyPrint(f"❌ **Parsing Failed.**")