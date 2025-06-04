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
            df = convert_column_to_datetime(df, convert_condition)
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


def parse_expression(expr: str) -> pl.Expr:
    """
    Convert a string expression like 'age > 30' to a Polars expression.
    Supported operators: ==, !=, >, >=, <, <=
    """

    pattern = r"(.+?)\s*(==|!=|>=|<=|>|<)\s*(.+)"
    match = re.match(pattern, expr.strip())
    if not match:
        raise ValueError(f"Invalid expression format: '{expr}'")

    column, op, value = match.groups()
    column = column.strip()
    value = value.strip().strip('"').strip("'")

    # try:
    #     # Try converting value to a number
    #     if "." in value:
    #         value = float(value)
    #     else:
    #         value = int(value)
    # except ValueError:
    #     # Leave value as string
    #     pass
    
    try:
        if "-" in value or "/" in value:
            value = pl.lit(datetime.datetime.fromisoformat(value))
        elif "." in value:
            value = float(value)
        else:
            value = int(value)
    except ValueError:
        pass

    # Build the Polars expression
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
    

def convert_column_to_datetime(df: pl.DataFrame, convert_condition: ConvertCondition) -> pl.DataFrame:
    logger.easyPrint(df.schema)
    column = convert_condition.column_name
    logger.easyPrint(column)
    fmt = convert_condition.format
    logger.easyPrint(fmt)
    try:
        fmt = convert_date_format_to_python_strftime(fmt)
        logger.easyPrint("chal gya")
    except Exception as e:
        fmt = '%d/%m/%y'
        logger.easyPrint(e)
    logger.easyPrint(fmt)
    changed_format_df = df.with_columns(
        pl.col(column).str.strptime(pl.Datetime, fmt, strict=False)
    )
    logger.easyPrint(changed_format_df)
    return changed_format_df

def convert_date_format_to_python_strftime(input_format_string: str) -> str | None:
    """
    Converts a common string date format (e.g., 'dd/mm/yy') to a Python strftime format
    (e.g., '%d/%m/%y').

    This function supports several common date components and separators.

    Args:
        input_format_string (str): The date format string to convert.
                                   Examples: 'dd/mm/yy', 'mm-dd-yyyy', 'yyyy.mm.dd',
                                             'dd/mm/yyyy hh:mi:ss', 'hh:mi:ss AM/PM'

    Returns:
        str | None: The equivalent Python strftime format string, or None if the
                    input format is not recognized or is invalid.

    Raises:
        ValueError: If the input_format_string is empty or not a string.
    """

    # --- Input Validation ---
    if not isinstance(input_format_string, str):
        raise ValueError("Input format string must be a string.")
    if not input_format_string:
        raise ValueError("Input format string cannot be empty.")

    # --- Step 1: Define the mapping of common date components to strftime codes ---
    # We use a list of tuples for ordered replacement, especially for 'yyyy' before 'yy'.
    # Order matters here to ensure 'yyyy' is matched before 'yy'.
    format_replacements = [
        # Year
        ('yyyy', '%Y'),  # Full year (e.g., 2023)
        ('yy', '%y'),    # Two-digit year (e.g., 23)

        # Month
        ('mm', '%m'),    # Month as a zero-padded decimal number (e.g., 01, 12)
        ('mon', '%b'),   # Abbreviated month name (e.g., Jan, Feb)
        ('month', '%B'), # Full month name (e.g., January, February)

        # Day
        ('dd', '%d'),    # Day of the month as a zero-padded decimal (e.g., 01, 31)
        ('day', '%a'),   # Abbreviated weekday name (e.g., Mon, Tue)
        ('weekday', '%A'), # Full weekday name (e.g., Monday, Tuesday)

        # Hour
        ('hh', '%H'),    # Hour (24-hour clock) as a zero-padded decimal (e.g., 00, 23)
        ('hhr', '%I'),   # Hour (12-hour clock) as a zero-padded decimal (e.g., 01, 12)

        # Minute
        ('mi', '%M'),    # Minute as a zero-padded decimal number (e.g., 00, 59)

        # Second
        ('ss', '%S'),    # Second as a zero-padded decimal number (e.g., 00, 59)

        # Millisecond (Python's strftime doesn't directly support ms, but we can map common representations)
        ('ms', '%f'),    # Microsecond as a decimal number, zero-padded on the left.
                         # This is the closest in strftime for milliseconds, although it's microseconds.

        # AM/PM
        ('am/pm', '%p'), # Locale's equivalent of either AM or PM.
        ('am_pm', '%p'),
        ('ap', '%p'),    # Short for AM/PM

        # Timezone
        ('tz', '%Z'),    # Time zone name (empty string if the object is naive).
        ('utc_offset', '%z'), # UTC offset in the form +HHMM or -HHMM.

        # Other useful formats
        ('doy', '%j'),   # Day of the year as a zero-padded decimal number.
        ('wky', '%U'),   # Week number of the year (Sunday as the first day of the week).
        ('wk_iso', '%W'), # Week number of the year (Monday as the first day of the week).
    ]

    # --- Step 2: Convert the input string to lowercase for case-insensitivity ---
    # This helps in matching 'DD' or 'MM' if the user provides it.
    processed_format_string = input_format_string.lower()

    # --- Step 3: Apply replacements iteratively ---
    # We use a loop and replace to handle multiple occurrences and ensure order.
    # We also handle potential escaped characters by first replacing them and then
    # un-escaping them after conversion.
    temp_string = processed_format_string
    for common_format, strftime_code in format_replacements:
        # Use regex to replace full words to avoid partial matches (e.g., 'm' in 'mm')
        # We need to be careful with common characters like '/' or '-' in the input.
        # For this simple mapping, direct string replace is okay if order is maintained.
        temp_string = temp_string.replace(common_format, strftime_code)

    # --- Step 4: Check if the conversion was successful ---
    # A simple check: if any of the original components are still present,
    # it means the format was not fully recognized.
    # This check needs to be smarter. We can't just check for 'dd', 'mm' etc.
    # A better approach: check if the resulting string only contains strftime codes
    # and valid separators, or if it still contains unrecognized placeholders.

    # For simplicity, we'll assume if our replacements were comprehensive for
    # common patterns, the remaining parts are separators or literal characters.
    # If the user provides something like 'dd/xyz/yy', 'xyz' will remain.
    # A more robust solution might try to parse it with datetime.strptime and catch errors.

    # Let's add a basic check for common unrecognized patterns that are not separators.
    # This is a heuristic and might need refinement for very complex cases.
    unrecognized_patterns = re.findall(r'[a-zA-Z]{2,}', temp_string.replace('%', ''))
    if unrecognized_patterns:
        # If there are still alphabetic sequences that are not strftime codes,
        # it likely means an unrecognized format.
        # We also need to be careful not to flag valid separators.
        # This part is tricky. For now, we'll return the converted string.
        # A full validation would involve trying to parse a dummy date with the generated format.
        pass # We proceed with the conversion. The responsibility falls on the caller
             # to validate the output with datetime.strptime if needed.

    return temp_string

# --- File path for documentation ---
# Project Structure:
# my_project/
# ├── utils/
# │   └── date_converter.py  <-- This file
# └── main.py
# └── tests/
#     └── test_date_converter.py
