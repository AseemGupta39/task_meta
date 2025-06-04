import polars as pl
from file_reader import createDataframe

# Step 1: Read the CSV file
df = createDataframe('data1.csv')

# Step 2: Define column name and target format
column_name = "created_at"  # Replace with actual column name
target_format = "%d-%m"  # Desired output format (e.g., "YYYY-MM-DD")

# Step 3: Get a sample value from the column to infer the input datetime format
sample_value = df.head(1)[column_name][0]

# Step 4: Manually infer or hardcode the input format
# For example, if the sample is '31/12/2023', the format is "%d/%m/%Y"
# This part can be automated with logic, but for simplicity, it's hardcoded here
input_format = "%Y/%m/%d"  # Replace with the correct format based on the sample

# Step 5: Convert the string column to datetime and then to the target format
df = df.with_columns(
    pl.col(column_name)
    .str.strptime(pl.Datetime, fmt=input_format)  # Convert to datetime
    .dt.strftime(target_format)                   # Format to target string
    .alias(column_name)
)

# Step 6: Print or return the updated DataFrame
print(df)