import polars as pl
from models.schemas import FilterConditions
from utils.logger import logger


def apply_filters(df: pl.DataFrame, conditions: FilterConditions) -> pl.DataFrame:
    try:
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
    import re

    pattern = r"(.+?)\s*(==|!=|>=|<=|>|<)\s*(.+)"
    match = re.match(pattern, expr.strip())
    if not match:
        raise ValueError(f"Invalid expression format: '{expr}'")

    column, op, value = match.groups()
    column = column.strip()
    value = value.strip().strip('"').strip("'")

    try:
        # Try converting value to a number
        if "." in value:
            value = float(value)
        else:
            value = int(value)
    except ValueError:
        # Leave value as string
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