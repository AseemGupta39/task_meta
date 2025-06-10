import re
from utils.logger import logger

def _clean_captured_value(value_str: str):
    """
    ✨ Cleans the captured value:
    - Removes surrounding quotes if present.
    - Converts numeric strings to int/float if applicable.
    - Handles 'None' string literal by returning None.
    """
    if value_str is None: # Added check for None
        return None

    value_str = value_str.strip()

    # 🧐 Remove surrounding single quotes for strings
    # This also handles cases where a string might contain escaped single quotes (e.g., 'It''s a test')
    if value_str.startswith("'") and value_str.endswith("'"):
        # Remove outer quotes and then replace any double single quotes with a single one
        return value_str[1:-1].replace("''", "'")

    # 🔢 Try converting to float or int
    try:
        if "." in value_str:
            return float(value_str)
        return int(value_str)
    except ValueError:
        # If it's not a number or a quoted string, return as-is (e.g., an unquoted string)
        return value_str


def parse_sql_case_statement(sql_statement: str):
    """
    💪 Parses a specific SQL CASE statement format and extracts components.

    Returns:
        dict: Parsed parts of the CASE statement.
              Keys: col_name, operator, comparison_value, then_value, else_value, table_name.
              Returns None if parsing fails.
    """
    # 🎯 Updated regex pattern:
    # 1. `\[(?P<col_name>[^\]]+)\]\s*`: This specifically captures content inside `[]`
    #    and puts it into `col_name` without the brackets themselves.
    # 2. `'(?:[^']|'')*'`: Improved string capture to handle escaped single quotes (e.g., 'It''s').
    pattern = re.compile(
        r"""^\s*CASE\s+WHEN\s+
            \[(?P<col_name>[^\]]+)\]\s* # Column name capturing content WITHOUT brackets
            (?P<operator>>=|<=|==|!=|>|<)\s* # Comparison operator
            (?P<comparison_value>'(?:[^']|'')*'|\d+(?:\.\d+)?)\s* # Value: 'string' (handles escaped quotes) or number
            THEN\s+(?P<then_value>'(?:[^']|'')*'|\d+(?:\.\d+)?)\s+   # THEN value
            ELSE\s+(?P<else_value>'(?:[^']|'')*'|\d+(?:\.\d+)?)\s+   # ELSE value
            END\s+FROM\s+(?P<table_name>\w+)\s*$ # Table name
        """,
        re.IGNORECASE | re.VERBOSE
    )

    match = pattern.match(sql_statement.strip())
    if not match:
        print(f"🚫 Oops! The SQL statement below doesn't match the expected format:\n    '{sql_statement}'")
        return None

    parsed_data = match.groupdict()

    # ✨ Clean the captured values using the helper function.
    # The col_name is now already clean because of the regex.
    for key in ['comparison_value', 'then_value', 'else_value']:
        parsed_data[key] = _clean_captured_value(parsed_data[key])


    return parsed_data



def get_things_from_sql_drive_statement(stmt:str):
    result = parse_sql_case_statement(stmt)
    if result:
        print(f"✅ **Parsed Successfully!** ✨")
        print(f"   - Column Name      : {result['col_name']}")
        print(f"   - Operator         : {result['operator']}")
        print(f"   - Comparison Value : {result['comparison_value']} (Type: {type(result['comparison_value']).__name__})")
        print(f"   - THEN Value       : {result['then_value']} (Type: {type(result['then_value']).__name__})")
        print(f"   - ELSE Value       : {result['else_value']} (Type: {type(result['else_value']).__name__})")
        print(f"   - Table Name       : {result['table_name']}")
    else:
        print(f"❌ **Parsing Failed.**")
