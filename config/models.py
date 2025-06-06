from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

# Create base class for models
Base = declarative_base()

# Define MySQL-compatible table
class ParsedValues(Base):
    __tablename__ = 'parsed_values'

    name_size = 100

    name1 = Column(String(name_size))
    value1 =  Column(Integer)
    quantity1 = Column(Integer)
    created_at1 = Column(DateTime)
    name2 = Column(String(name_size))
    value2 =  Column(Integer)
    quantity2 = Column(Integer)
    created_at2 = Column(DateTime)
    name3 = Column(String(name_size))
    value3 =  Column(Integer)
    quantity3 = Column(Integer)
    created_at3 = Column(DateTime)
    message = Column(String)