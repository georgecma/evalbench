from .mysql_handler import MYSQLHandler
from .postgres_handler import PostgresHandler


def get_db_handler(db_config: dict):
    if db_config['db'] == "mysql":
        return MYSQLHandler(db_config)
    elif db_config['db'] == "postgres":
        return PostgresHandler(db_config)
    else:
        raise ValueError(f"Unsupported database type: {db_config['db']}")
