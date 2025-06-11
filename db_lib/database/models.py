from sqlalchemy import Column, BigInteger, String, Text, DateTime, Integer
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from db_lib.config import AppConfig

Base = declarative_base()

class YourDataTable(Base):
    """
    ORM Model for your MySQL table.
    Ensures correct data types mapping from Polars to MySQL,
    especially for DateTime columns.
    """
    __tablename__ = AppConfig.MAIN_TABLE_NAME # Dynamically get table name from config

    # Adding a synthetic primary key column 'id'
    # This column will auto-increment and serve as the primary key
    id = Column(BigInteger, primary_key=True, autoincrement=True) # ✨ NEW SYNTHETIC PK

    data1_name = Column(String(255)) # No longer the primary key
    data1_value = Column(BigInteger)
    data1_created_at = Column(DateTime) # ✨ Mapped to MySQL DATETIME
    data1_dc1 = Column(String(255))
    # data1_dc2 = Column(String(255))

    data2_name = Column(String(255))
    data2_value = Column(BigInteger)
    data2_created_at = Column(DateTime) # ✨ Mapped to MySQL DATETIME
    data2_dc1 = Column(String(255))

    data3_name = Column(String(255))
    data3_value = Column(BigInteger)
    data3_created_at = Column(DateTime) # ✨ Mapped to MySQL DATETIME
    data3_dc1 = Column(String(255))

    Message = Column(Text) # Mapping to TEXT for potentially long messages

    def __repr__(self):
        # Adjusted __repr__ to include the new 'id'
        return (f"<YourDataTable(id={self.id}, data1_id={self.data1_id}, "
                f"data1_value1={self.data1_value1})>")

def create_tables(engine):
    """
    ✨ Creates all defined tables in the database if they do not already exist.
    """
    print(f"Attempting to create table: {YourDataTable.__tablename__} if it doesn't exist...")
    try:
        Base.metadata.create_all(engine)
        print("✅ Tables created or already exist.")
    except SQLAlchemyError as e:
        print(f"❌ Error creating tables: {e}")
        raise # Re-raise to indicate critical failure



