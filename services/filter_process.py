import polars as pl
import re
from models.pydantic_models import ConvertCondition, FilterConditions
from utils.logger import logger
from utils.constants import replacements, known_formats
from datetime import datetime

def apply_filters(df: pl.DataFrame, conditions: FilterConditions, convert_condition: ConvertCondition = None) -> pl.DataFrame:
    try:
        # Apply datetime conversion if needed

        if convert_condition:
            df = convertDatetimeColumn(df, convert_condition)
        
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
        raise Exception("Got error while applying filter ")


def convertDatetimeColumn(df: pl.DataFrame, convert_condition: ConvertCondition) -> pl.DataFrame:
    column = convert_condition.column_name
    user_format = convert_condition.format

    # logger.easyPrint(f"Converting column '{column}' using target format '{user_format}'")

    # Step 1: Infer original format from the first row
    example_value = df[column][0]
    original_format = infer_format_from_string(example_value)
    random_format = infer_format_from_string("2023/12/23")
    logger.easyPrint(random_format)
    logger.easyPrint(original_format)
    if not original_format:
        raise ValueError(f"Could not infer original format from value: {example_value}")

    # logger.easyPrint(f"Inferred original format: {original_format}")

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
        logger.error(f"Failed to convert and reformat datetime column '{column}': {e}")
        raise

    return df

def infer_format_from_string(date_str: str) -> str:
    """
    Infer a datetime format from a sample date string.
    """
    # Examples of common formats — expand as needed
    
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