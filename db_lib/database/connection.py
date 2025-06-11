import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from db_lib.config import AppConfig
from db_lib.core.exceptions import DatabaseConnectionError

class DatabaseConnection:
    """
    🔗 Manages SQLAlchemy database connection and session creation.
    Adheres to SRP by focusing solely on connection management.
    """
    def __init__(self, config: AppConfig):
        self.config = config
        self._engine = None
        self._Session = None

    def get_engine(self):
        """
        ✨ Creates and returns a SQLAlchemy database engine.
        Establishes a connection pool.
        """
        if self._engine is None:
            try:
                connection_url = URL.create(
                    drivername=self.config.DB_DRIVERNAME,
                    username=self.config.DB_USERNAME,
                    password=self.config.DB_PASSWORD,
                    host=self.config.DB_HOST,
                    port=self.config.DB_PORT,
                    database=self.config.DB_DATABASE
                )
                self._engine = create_engine(
                    connection_url,
                    pool_pre_ping=True, # Helps prevent stale connections
                    pool_recycle=3600 # Recycle connections after 1 hour
                )
                # Test connection immediately
                with self._engine.connect() as connection:
                    connection.execute(sqlalchemy.text("SELECT 1"))
                print("✅ Database connection established successfully!")
            except SQLAlchemyError as e:
                raise DatabaseConnectionError(f"❌ Error connecting to database: {e}") from e
        return self._engine

    def get_session_maker(self):
        """
        ✨ Returns a SQLAlchemy session maker.
        """
        if self._Session is None:
            if self._engine is None:
                self.get_engine() # Ensure engine is created first
            self._Session = sessionmaker(autocommit=False, autoflush=False, bind=self._engine)
        return self._Session
