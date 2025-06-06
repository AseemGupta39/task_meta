from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.models import Base

# Replace with your MySQL credentials
DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/team"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)