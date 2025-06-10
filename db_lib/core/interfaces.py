from abc import ABC, abstractmethod
import polars as pl

class IDataWriter(ABC):
    """
    🤝 Interface for data writing operations.
    Ensures any data writer implementation adheres to a common contract.
    """
    @abstractmethod
    def write_data(self, df: pl.DataFrame, table_name: str, **kwargs):
        """
        Abstract method to write a Polars DataFrame to a target.
        """
        pass
