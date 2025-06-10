import os
from dotenv import load_dotenv

load_dotenv()
class AppConfig:
    """
    ⚙️ Centralized configuration for the application.
    Best practice: Load sensitive data from environment variables.
    """
    
    # Database Configuration
    DB_DRIVERNAME = os.getenv("DB_DRIVERNAME", "mysql+pymysql")
    DB_USERNAME = os.getenv("DB_USERNAME", "your_mysql_user") # ⚠️ Replace with your actual user
    DB_PASSWORD = os.getenv("DB_PASSWORD", "your_mysql_password") # ⚠️ Replace with your actual password
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_DATABASE = os.getenv("DB_DATABASE", "your_database_name") # ⚠️ Replace with your actual database name
    
    # Data Loading Configuration
    DEFAULT_BATCH_SIZE = int(os.getenv("DEFAULT_BATCH_SIZE", 50000)) # Rows per batch

    # Table Name
    MAIN_TABLE_NAME = "your_main_data_table" # Make sure this matches your desired MySQL table name
    # print(DB_DRIVERNAME,DB_HOST,DB_USERNAME,DB_PASSWORD,sep="\n\n\n\n")
