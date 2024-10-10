from .postgres import PGDB
from .alloydb import AlloyDB
from .mysql import MySQLDB
from .sqlserver import SQLServerDB


def get_database(db_config):
    # AlloyDB
    if db_config["db"] == "alloydb":
        return AlloyDB(db_config)
    # Cloud SQL
    if db_config["db"] == "postgres":
        return PGDB(db_config)
    if db_config["db"] == "mysql":
        return MySQLDB(db_config)
    if db_config["db"] == "sqlserver":
        return SQLServerDB(db_config)
