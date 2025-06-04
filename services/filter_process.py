import polars as pl
import re
from models.schemas import FilterConditions
from utils.logger import logger
from models.schemas import ConvertCondition
from datetime import datetime


def apply_filters(df: pl.DataFrame, conditions: FilterConditions, convert_condition: ConvertCondition = None) -> pl.DataFrame:
    try:
        # Apply datetime conversion if needed
        logger.info(f"\n\n\n\n{convert_condition}\n\n\n\n")
        if convert_condition:
            logger.info(f"\n\n\nBefore conversion: {df}\n\n\n")
            df = convert_column_to_format(df, convert_condition)
            logger.info(f"\n\n\nAfter conversion: {df}\n\n\n")
        
        expressions = conditions.expressions
        operator = conditions.operator

        # Parse expressions into Polars expressions
        parsed_exprs = [parse_expression(expr) for expr in expressions]

        if len(parsed_exprs) == 1:
            # Single expression, apply directly
            return df.filter(parsed_exprs[0])
        elif operator == "And":
            combined = parsed_exprs[0]
            for expr in parsed_exprs[1:]:
                combined &= expr
            return df.filter(combined)
        elif operator == "Or":
            combined = parsed_exprs[0]
            for expr in parsed_exprs[1:]:
                combined |= expr
            return df.filter(combined)
        else:
            raise ValueError("Invalid operator for multiple expressions")

    except Exception as e:
        logger.error(f"Error applying filters: {e}")
        raise

'''

# def parse_expression(expr: str) -> pl.Expr:
#     """
#     Convert a string expression like 'age > 30' to a Polars expression.
#     Supported operators: ==, !=, >, >=, <, <=
#     """

#     pattern = r"(.+?)\s*(==|!=|>=|<=|>|<)\s*(.+)"
#     match = re.match(pattern, expr.strip())
#     if not match:
#         raise ValueError(f"Invalid expression format: '{expr}'")

#     column, op, value = match.groups()
#     column = column.strip()
#     value = value.strip().strip('"').strip("'")

#     try:
#         if "-" in value or "/" in value:
#             value = pl.lit(datetime.datetime.fromisoformat(value))
#         elif "." in value:
#             value = float(value)
#         else:
#             value = int(value)
#     except ValueError:
#         pass

#     # Build the Polars expression
#     if op == "==":
#         return pl.col(column) == value
#     elif op == "!=":
#         return pl.col(column) != value
#     elif op == ">":
#         return pl.col(column) > value
#     elif op == ">=":
#         return pl.col(column) >= value
#     elif op == "<":
#         return pl.col(column) < value
#     elif op == "<=":
#         return pl.col(column) <= value
#     else:
#         raise ValueError(f"Unsupported operator: '{op}'")
    

# def convert_column_to_datetime(df: pl.DataFrame, convert_condition: ConvertCondition) -> pl.DataFrame:
#     logger.easyPrint(df.schema)
#     column = convert_condition.column_name
#     logger.easyPrint(column)
#     fmt = convert_condition.format
#     logger.easyPrint(df.schema)

#     logger.easyPrint(fmt)
#     try:
#         # fmt = convert_date_format_to_python_strftime(fmt)
#         fmt = '%Y/%m/%d'

#         logger.easyPrint("chal gya")
#     except Exception as e:
#         fmt = '%d/%m/%y'
#         logger.easyPrint(e)
#     logger.easyPrint(fmt)
#     changed_format_df = df.with_columns(
#         pl.col(column).str.strptime(pl.Datetime, fmt, strict= False)
#     )
#     logger.easyPrint(changed_format_df)
#     logger.easyPrint(changed_format_df.schema)
#     return changed_format_df

# def convert_date_format_to_python_strftime(input_format_string: str) -> str | None:
#     """
#     Converts a common string date format (e.g., 'dd/mm/yy') to a Python strftime format
#     (e.g., '%d/%m/%y').

#     This function supports several common date components and separators.

#     Args:
#         input_format_string (str): The date format string to convert.
#                                    Examples: 'dd/mm/yy', 'mm-dd-yyyy', 'yyyy.mm.dd',
#                                              'dd/mm/yyyy hh:mi:ss', 'hh:mi:ss AM/PM'

#     Returns:
#         str | None: The equivalent Python strftime format string, or None if the
#                     input format is not recognized or is invalid.

#     Raises:
#         ValueError: If the input_format_string is empty or not a string.
#     """

#     # --- Input Validation ---
#     if not isinstance(input_format_string, str):
#         raise ValueError("Input format string must be a string.")
#     if not input_format_string:
#         raise ValueError("Input format string cannot be empty.")

#     # --- Step 1: Define the mapping of common date components to strftime codes ---
#     # We use a list of tuples for ordered replacement, especially for 'yyyy' before 'yy'.
#     # Order matters here to ensure 'yyyy' is matched before 'yy'.
#     format_replacements = [
#         # Year
#         ('yyyy', '%Y'),  # Full year (e.g., 2023)
#         ('yy', '%y'),    # Two-digit year (e.g., 23)

#         # Month
#         ('mm', '%m'),    # Month as a zero-padded decimal number (e.g., 01, 12)
#         ('mon', '%b'),   # Abbreviated month name (e.g., Jan, Feb)
#         ('month', '%B'), # Full month name (e.g., January, February)

#         # Day
#         ('dd', '%d'),    # Day of the month as a zero-padded decimal (e.g., 01, 31)
#         ('day', '%a'),   # Abbreviated weekday name (e.g., Mon, Tue)
#         ('weekday', '%A'), # Full weekday name (e.g., Monday, Tuesday)

#         # Hour
#         ('hh', '%H'),    # Hour (24-hour clock) as a zero-padded decimal (e.g., 00, 23)
#         ('hhr', '%I'),   # Hour (12-hour clock) as a zero-padded decimal (e.g., 01, 12)

#         # Minute
#         ('mi', '%M'),    # Minute as a zero-padded decimal number (e.g., 00, 59)

#         # Second
#         ('ss', '%S'),    # Second as a zero-padded decimal number (e.g., 00, 59)

#         # Millisecond (Python's strftime doesn't directly support ms, but we can map common representations)
#         ('ms', '%f'),    # Microsecond as a decimal number, zero-padded on the left.
#                          # This is the closest in strftime for milliseconds, although it's microseconds.

#         # AM/PM
#         ('am/pm', '%p'), # Locale's equivalent of either AM or PM.
#         ('am_pm', '%p'),
#         ('ap', '%p'),    # Short for AM/PM

#         # Timezone
#         ('tz', '%Z'),    # Time zone name (empty string if the object is naive).
#         ('utc_offset', '%z'), # UTC offset in the form +HHMM or -HHMM.

#         # Other useful formats
#         ('doy', '%j'),   # Day of the year as a zero-padded decimal number.
#         ('wky', '%U'),   # Week number of the year (Sunday as the first day of the week).
#         ('wk_iso', '%W'), # Week number of the year (Monday as the first day of the week).
#     ]

#     # --- Step 2: Convert the input string to lowercase for case-insensitivity ---
#     # This helps in matching 'DD' or 'MM' if the user provides it.
#     processed_format_string = input_format_string.lower()

#     # --- Step 3: Apply replacements iteratively ---
#     # We use a loop and replace to handle multiple occurrences and ensure order.
#     # We also handle potential escaped characters by first replacing them and then
#     # un-escaping them after conversion.
#     temp_string = processed_format_string
#     for common_format, strftime_code in format_replacements:
#         # Use regex to replace full words to avoid partial matches (e.g., 'm' in 'mm')
#         # We need to be careful with common characters like '/' or '-' in the input.
#         # For this simple mapping, direct string replace is okay if order is maintained.
#         temp_string = temp_string.replace(common_format, strftime_code)

#     # --- Step 4: Check if the conversion was successful ---
#     # A simple check: if any of the original components are still present,
#     # it means the format was not fully recognized.
#     # This check needs to be smarter. We can't just check for 'dd', 'mm' etc.
#     # A better approach: check if the resulting string only contains strftime codes
#     # and valid separators, or if it still contains unrecognized placeholders.

#     # For simplicity, we'll assume if our replacements were comprehensive for
#     # common patterns, the remaining parts are separators or literal characters.
#     # If the user provides something like 'dd/xyz/yy', 'xyz' will remain.
#     # A more robust solution might try to parse it with datetime.strptime and catch errors.

#     # Let's add a basic check for common unrecognized patterns that are not separators.
#     # This is a heuristic and might need refinement for very complex cases.
#     unrecognized_patterns = re.findall(r'[a-zA-Z]{2,}', temp_string.replace('%', ''))
#     if unrecognized_patterns:
#         # If there are still alphabetic sequences that are not strftime codes,
#         # it likely means an unrecognized format.
#         # We also need to be careful not to flag valid separators.
#         # This part is tricky. For now, we'll return the converted string.
#         # A full validation would involve trying to parse a dummy date with the generated format.
#         pass # We proceed with the conversion. The responsibility falls on the caller
#              # to validate the output with datetime.strptime if needed.

#     return temp_string


# import polars as pl
# import re
# from datetime import datetime
# from models.schemas import FilterConditions, ConvertCondition
# from utils.logger import logger


# def apply_filters(df: pl.DataFrame, conditions: FilterConditions, convert_condition: ConvertCondition = None) -> pl.DataFrame:
#     try:
#         # Apply datetime conversion if needed
#         logger.info(f"Convert Condition: {convert_condition}")
#         if convert_condition:
#             logger.info(f"Before conversion: {df}")
#             df = convert_column_to_datetime(df, convert_condition)
#             logger.info(f"After conversion: {df}")

#         expressions = conditions.expressions
#         operator = conditions.operator

#         # Parse expressions into Polars expressions
#         parsed_exprs = [parse_expression(expr) for expr in expressions]

#         if len(parsed_exprs) == 1:
#             return df.filter(parsed_exprs[0])
#         elif operator == "And":
#             combined = parsed_exprs[0]
#             for expr in parsed_exprs[1:]:
#                 combined &= expr
#             return df.filter(combined)
#         elif operator == "Or":
#             combined = parsed_exprs[0]
#             for expr in parsed_exprs[1:]:
#                 combined |= expr
#             return df.filter(combined)
#         else:
#             raise ValueError("Invalid operator for multiple expressions")

#     except Exception as e:
#         logger.error(f"Error applying filters: {e}")
#         raise


# def parse_expression(expr: str) -> pl.Expr:
#     """
#     Convert a string expression like 'age > 30' to a Polars expression.
#     """
#     pattern = r"(.+?)\s*(==|!=|>=|<=|>|<)\s*(.+)"
#     match = re.match(pattern, expr.strip())
#     if not match:
#         raise ValueError(f"Invalid expression format: '{expr}'")

#     column, op, value = match.groups()
#     column = column.strip()
#     value = value.strip().strip('"').strip("'")

#     # Try to infer the type
#     try:
#         if "-" in value or "/" in value:
#             value = pl.lit(datetime.fromisoformat(value))
#         elif "." in value:
#             value = float(value)
#         else:
#             value = int(value)
#     except ValueError:
#         value = pl.lit(value)

#     if op == "==":
#         return pl.col(column) == value
#     elif op == "!=":
#         return pl.col(column) != value
#     elif op == ">":
#         return pl.col(column) > value
#     elif op == ">=":
#         return pl.col(column) >= value
#     elif op == "<":
#         return pl.col(column) < value
#     elif op == "<=":
#         return pl.col(column) <= value
#     else:
#         raise ValueError(f"Unsupported operator: '{op}'")


# def convert_column_to_datetime(df: pl.DataFrame, convert_condition: ConvertCondition) -> pl.DataFrame:
#     column = convert_condition.column_name
#     user_format = convert_condition.format

#     logger.easyPrint(f"Converting column '{column}' using format '{user_format}'")

#     try:
#         python_format = convert_date_format_to_python_strftime(user_format)
#     except Exception as e:
#         logger.easyPrint(f"Error in format conversion: {e}")
#         python_format = "%d/%m/%y"
    
#     try:
#         df = df.with_columns(
#             pl.col(column).str.strptime(pl.Datetime, python_format, strict=False).alias(column)
#         )
#         logger.easyPrint(f"Datetime conversion successful for format: {python_format}")
#     except Exception as e:
#         logger.easyPrint(f"Failed to convert column '{column}' to datetime: {e}")
#         raise

#     return df


# def convert_date_format_to_python_strftime(input_format_string: str) -> str:
#     if not isinstance(input_format_string, str):
#         raise ValueError("Input format string must be a string.")
#     if not input_format_string:
#         raise ValueError("Input format string cannot be empty.")

#     format_replacements = [
#         ('yyyy', '%Y'),
#         ('yy', '%y'),
#         ('mm', '%m'),
#         ('mon', '%b'),
#         ('month', '%B'),
#         ('dd', '%d'),
#         ('day', '%a'),
#         ('weekday', '%A'),
#         ('hh', '%H'),
#         ('hhr', '%I'),
#         ('mi', '%M'),
#         ('ss', '%S'),
#         ('ms', '%f'),
#         ('am/pm', '%p'),
#         ('am_pm', '%p'),
#         ('ap', '%p'),
#         ('tz', '%Z'),
#         ('utc_offset', '%z'),
#         ('doy', '%j'),
#         ('wky', '%U'),
#         ('wk_iso', '%W'),
#     ]

#     temp_string = input_format_string.lower()

#     for common_format, strftime_code in format_replacements:
#         temp_string = temp_string.replace(common_format, strftime_code)

#     return temp_string

'''

def convert_column_to_format(df: pl.DataFrame, convert_condition: ConvertCondition) -> pl.DataFrame:
    column = convert_condition.column_name
    user_format = convert_condition.format

    logger.easyPrint(f"Converting column '{column}' using target format '{user_format}'")

    # Step 1: Infer original format from the first row
    example_value = df[column][0]
    original_format = infer_format_from_string(example_value)
    if not original_format:
        raise ValueError(f"Could not infer original format from value: {example_value}")

    logger.easyPrint(f"Inferred original format: {original_format}")

    # Step 2: Convert user-provided format to Python datetime format
    python_target_format = convert_to_python_strftime(user_format)

    # Step 3: Convert to datetime using original format, then format to user format
    try:
        df = df.with_columns(
            pl.col(column)
            .str.strptime(pl.Datetime, original_format, strict=False)
            .dt.strftime(python_target_format)
            .alias(column)
        )
    except Exception as e:
        logger.easyPrint(f"Failed to convert and reformat datetime column '{column}': {e}")
        raise

    logger.easyPrint(f"Successfully reformatted '{column}' to format: {user_format}")
    logger.easyPrint(df)
    return df


def infer_format_from_string(date_str: str) -> str:
    """
    Infer a datetime format from a sample date string.
    """
    # Examples of common formats — expand as needed
    known_formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%y/%m/%d",
        "%d-%b-%Y",
        "%Y.%m.%d"
    ]
    for fmt in known_formats:
        try:
            datetime.strptime(date_str, fmt)
            return fmt
        except ValueError:
            continue
    return None


def convert_to_python_strftime(custom_format: str) -> str:
    """
    Convert user-provided format like 'yy/mm/dd' to Python strftime format.
    """
    replacements = {
        'yyyy': '%Y',
        'yy': '%y',
        'mm': '%m',
        'dd': '%d',
        'hh': '%H',
        'mi': '%M',
        'ss': '%S',
        'mon': '%b',
        'month': '%B',
        'am/pm': '%p',
    }

    python_format = custom_format.lower()
    for key, val in replacements.items():
        python_format = python_format.replace(key, val)

    return python_format


def parse_expression(expr: str) -> pl.Expr:
    """
    Convert a string expression like 'age > 30' to a Polars expression.
    """
    pattern = r"(.+?)\s*(==|!=|>=|<=|>|<)\s*(.+)"
    match = re.match(pattern, expr.strip())
    if not match:
        raise ValueError(f"Invalid expression format: '{expr}'")

    column, op, value = match.groups()
    column = column.strip()
    value = value.strip().strip('"').strip("'")

    # Try to infer the type
    try:
        if "-" in value or "/" in value:
            value = pl.lit(datetime.fromisoformat(value))
        elif "." in value:
            value = float(value)
        else:
            value = int(value)
    except ValueError:
        value = pl.lit(value)

    if op == "==":
        return pl.col(column) == value
    elif op == "!=":
        return pl.col(column) != value
    elif op == ">":
        return pl.col(column) > value
    elif op == ">=":
        return pl.col(column) >= value
    elif op == "<":
        return pl.col(column) < value
    elif op == "<=":
        return pl.col(column) <= value
    else:
        raise ValueError(f"Unsupported operator: '{op}'")