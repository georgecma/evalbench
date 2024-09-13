from abc import ABC, abstractmethod
from typing import Any, Tuple, List


class DBHandler(ABC):

    def __init__(self, db_config: dict):
        pass

    @abstractmethod
    def drop_all_tables(self):
        """
        Generates SQL statement to drop all tables in the database.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def create_schema_statements(self, schema, excluded_columns):
        """
        Generates SQL statement to create table schema.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def create_insert_statements(self):
        """
        Generates SQL insert statements based on CSV file.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def execute(self, queries: List[str]):
        """
        Execute a list of query strings and return the execution results and total time spent.
        Args:
            queries (List[str]): The SQL queries to execute.
        """
        raise NotImplementedError("Subclasses must implement this method")
