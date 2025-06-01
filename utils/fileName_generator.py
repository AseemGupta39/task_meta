from datetime import datetime

def generate_filename(extension="csv"):
    # Get current date and time
    now = datetime.now()
    # Format it as a string: YYYYMMDD_HHMMSS
    timestamp = now.strftime("%Y_%m_%d__%H_%M_%S")
    # Create the filename
    filename = f"output_{timestamp}.{extension}"
    return filename

# Example usage:
print(generate_filename())