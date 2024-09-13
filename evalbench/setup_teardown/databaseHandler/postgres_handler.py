import os
import csv
from typing import Any, Tuple, List
from .db_handler import DBHandler
from databases import get_database

drop_all_tables_query = [
    "DROP SCHEMA public CASCADE;",
    "CREATE SCHEMA public;",
]


class PostgresHandler(DBHandler):

    def __init__(self, db_config: dict):
        self.db_engine = "postgres"
        self.db_config = db_config

    def drop_all_tables(self):
        return self.execute(drop_all_tables_query)

    def create_schema_statements(self, schema, excluded_columns):
        excluded_columns = excluded_columns or set()
        create_statements = []

        for table in schema.tables:
            table_name = table.table
            columns = [
                f"{column.column} {column.data_type}"
                for column in table.columns
                if column.column not in excluded_columns
            ]

            columns_str = ",\n    ".join(columns)
            create_statement = f"CREATE TABLE {table_name} (\n    {columns_str}\n);"
            create_statements.append(create_statement)

        return create_statements

    def create_insert_statements(self, data_directory):
        insertion_strings = []
        for filename in os.listdir(data_directory):
            if filename.endswith(".csv"):
                table_name = filename[:-4]
                with open(os.path.join(data_directory, filename), 'r') as csvfile:
                    reader = csv.reader(csvfile)
                    for row in reader:
                        values = ", ".join([f"{value}" for value in row])
                        insertion_strings.append(f"INSERT INTO public.{table_name} VALUES ({values});")

        return insertion_strings

    def execute(self, queries: List[str]):
        result = None
        error = None
        db_instance = get_database(self.db_config)
        combined_query = "\n".join(queries)
        result, error = db_instance.execute(combined_query)
        return result, error
