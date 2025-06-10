import polars as pl
from typing import Any

def add_column_on_given_condition(
    dataframe: pl.DataFrame,
    new_column_name: str, # Added new_column_name as it's a dynamic output
    condition_column: str,
    operator: str,
    condition_value: Any,
    then_value: Any,
    else_value: Any
) -> pl.DataFrame:
    """
    Adds a new column to the DataFrame based on a dynamic condition.

    Args:
        dataframe (pl.DataFrame): The input Polars DataFrame.
        new_column_name (str): The name for the new column to be added.
        condition_column (str): The name of the column to apply the condition on.
        operator (str): The boolean operator as a string (e.g., '>', '<', '==', '>=', '<=', '!=').
        condition_value (Any): The value to compare against in the condition.
        then_value (Any): The value to put in the new column if the condition is True.
        else_value (Any): The value to put in the new column if the condition is False.

    Returns:
        pl.DataFrame: The DataFrame with the new conditional column.

    Raises:
        ValueError: If the condition_column does not exist in the DataFrame
                    or if an unsupported operator is provided.
    """
    # 🎯 Check if the condition column exists
    if condition_column not in dataframe.columns:
        raise ValueError(
            f"❌ Error: Column '{condition_column}' not found in the DataFrame. "
            "Please provide an existing column name."
        )

    # 🗺️ Map string operators to Polars expression methods
    # Using a dictionary for clear and maintainable mapping
    operator_mapping = {
        ">": lambda col, val: col > val,
        "<": lambda col, val: col < val,
        "==": lambda col, val: col == val,
        ">=": lambda col, val: col >= val,
        "<=": lambda col, val: col <= val,
        "!=": lambda col, val: col != val,
    }

    # 🔍 Get the Polars expression for the given operator
    operator_func = operator_mapping.get(operator)
    if not operator_func:
        raise ValueError(
            f"❌ Error: Unsupported operator '{operator}'. "
            "Supported operators are: {', '.join(operator_mapping.keys())}"
        )

    # ➡️ Build the condition expression
    # pl.col(condition_column) gets the column
    # operator_func applies the specific comparison (e.g., .gt(), .eq())
    condition_expression = operator_func(pl.col(condition_column), condition_value)

    # 🚀 Construct the new column using when().then().otherwise()
    df_with_new_column = dataframe.with_columns(
        pl.when(condition_expression)
        .then(pl.lit(then_value)) # Use pl.lit() for fixed values
        .otherwise(pl.lit(else_value)) # Use pl.lit() for fixed values
        .alias(new_column_name) # Name the new column dynamically
    )

    return df_with_new_column
# Apply the function to our sample DataFrame
# df_transformed = add_column_on_given_condition(df)
