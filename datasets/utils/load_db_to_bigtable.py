import argparse
import contextlib
import os
import sqlite3
import glob
from pathlib import Path
from google.cloud import bigtable
from google.cloud.bigtable.table import Table
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable_admin_v2 import BigtableInstanceAdminClient, LogicalView
from google.api_core.exceptions import NotFound
from google.cloud.bigtable.row import DirectRow
from google.cloud.bigtable.batcher import MutationsBatcher
from google.cloud.bigtable_v2.services.bigtable.client import BigtableClient
from uuid import uuid4
import hashlib
from abc import ABC, abstractmethod
from enum import Enum

DEFAULT_COLUMN_FAMILY = "columns"


def bigtable_table_id(sqlite_table_id: str) -> str:
    return sqlite_table_id + "-bt"


class TableOp(str, Enum):
    REBUILD = "REBUILD"  # rebuild the entire table and insert data
    DELETE_ONLY = "DELETE_ONLY"  # delete table only
    NO_ACTION = "NO_ACTION"  # don't do anything


class LogicalViewType(str, Enum):
    TYPED = "TYPED"
    UNTYPED = "UNTYPED"
    NO_ACTION = "NO_ACTION"


"""
One-time setup helper to load a sqlite database into Bigtable.

This script can:
- Rebuild Bigtable tables from sqlite tables (--table_op=REBUILD).
- Delete Bigtable tables (--table_op=DELETE_ONLY).
- Create typed or untyped logical views on top of Bigtable tables (--view_op).

Example commands from the project root:

# Rebuild all tables and create typed views for all sqlite dbs in a directory
$ python ./datasets/utils/load_db_to_bigtable.py /
    --db_path="./path/to/your/databases/*.sqlite" /
    --table_op=REBUILD /
    --view_op=TYPED

# Only rebuild specific tables and create untyped views, with a row limit
$ python ./datasets/utils/load_db_to_bigtable.py /
    --db_path="./path/to/your/database.sqlite" /
    --tables="table1,table2" /
    --table_op=REBUILD /
    --view_op=UNTYPED /
    --limit=1000

# Delete all bigtable tables associated with a database
$ python ./datasets/utils/load_db_to_bigtable.py /
    --db_path="./path/to/your/database.sqlite" /
    --table_op=DELETE_ONLY

# "Dry run" - just print the schema of the sqlite databases without changing Bigtable
$ python ./datasets/utils/load_db_to_bigtable.py /
    --db_path="./path/to/your/databases/*.sqlite"

"""


class LogicalViewBuilder(ABC):
    logical_view: LogicalView

    def __init__(
        self,
        instance_admin_client: BigtableInstanceAdminClient,
        gcp_project_id: str,
        instance_id: str,
        from_table: str,
        columns: dict,
        logical_view_id: str,
    ):
        self.instance_admin_client = instance_admin_client
        self.gcp_project_id = gcp_project_id
        self.instance_id = instance_id
        self.from_table = from_table
        self.columns = columns
        self.logical_view_id = logical_view_id
        self.logical_view = LogicalView()

    @abstractmethod
    def query(self) -> str:
        pass

    def parent(self) -> str:
        return f"projects/{self.gcp_project_id}/instances/{self.instance_id}"

    def name(self) -> str:
        return self.parent() + f"/logicalViews/{self.logical_view_id}"

    def test_connection(self):
        self.instance_admin_client.get_logical_view(name=self.name())

    def delete(self):
        print(f"Deleting logical view: {self.logical_view_id}...")
        try:
            self.instance_admin_client.delete_logical_view(name=self.name())
            print(f"Deleted logical view: {self.logical_view_id}")
        except NotFound:
            print(f"Logical view {self.logical_view_id} not found, skipping deletion.")
            pass

    def build(self):
        self.logical_view.name = self.name()
        self.logical_view.query = self.query()

        print(f"Creating logical view: {self.logical_view_id}...")
        self.instance_admin_client.create_logical_view(
            parent=self.parent(),
            logical_view_id=self.logical_view_id,
            logical_view=self.logical_view,
        )
        print(f"Created logical view: {self.logical_view_id}")

        self.test_connection()


class TypedLogicalViewBuilder(LogicalViewBuilder):
    def query(self):
        query_string = "SELECT "
        for col_name in self.columns.keys():
            # sanitize the col to just an alphanumeric string
            sanitized_col_name = "".join([c for c in col_name if c.isalnum()])

            # Cast as type because the default is bytes - which we can't do operations on.
            cast_col = f'CAST({DEFAULT_COLUMN_FAMILY}["{col_name}"] AS STRING)'
            col_type = self.columns[col_name]
            if col_type in ("INTEGER", "REAL"):
                cast_col = f"CAST({cast_col} AS FLOAT64)"
            part = f"{cast_col} AS `{sanitized_col_name}`, "
            query_string += part
        query_string = query_string[:-2]  # remove last comma
        query_string += f" FROM `{self.from_table}`"
        return query_string


class UntypedLogicalViewBuilder(LogicalViewBuilder):
    def query(self):
        query_string = "SELECT "
        for col_name in self.columns.keys():
            # sanitize the col to just an alphanumeric string
            sanitized_col_name = "".join([c for c in col_name if c.isalnum()])
            part = f'{DEFAULT_COLUMN_FAMILY}["{col_name}"] AS `{sanitized_col_name}`, '
            query_string += part
        query_string = query_string[:-2]  # remove last comma
        query_string += f" FROM `{self.from_table}`"
        return query_string


class BigtableTableBuilder:
    columns: dict
    table_id: str
    backing_table_id: str

    table: Table
    instance_admin_client: BigtableInstanceAdminClient

    def __init__(
        self,
        instance_admin_client: BigtableInstanceAdminClient,
        instance: Instance,
        table_id: str,
        columns: dict,
        gcp_project_id: str,
        instance_id: str,
    ):
        # initialize backing table
        self.instance_admin_client = instance_admin_client
        self.gcp_project_id = gcp_project_id
        self.instance_id = instance_id
        self.backing_table_id = bigtable_table_id(table_id)
        self.table = Table(self.backing_table_id, instance)
        self.columns = {}
        for col in columns.keys():
            self.columns[col] = columns[col]

    def delete(self):
        print(f"Deleting Bigtable table: {self.backing_table_id}...")
        try:
            self.table.delete()
            print("Deleted", self.table.name)
        except NotFound:
            print(f"Table {self.backing_table_id} not found, skipping deletion.")
            pass

    def create(self):
        # create table
        print(f"Creating Bigtable table: {self.backing_table_id}...")
        self.table.create()
        self.table.column_family(DEFAULT_COLUMN_FAMILY).create()
        print(f"Created Bigtable table: {self.backing_table_id}")

    def test_connection(self) -> None | Exception:

        if not self.table.exists():
            raise NotFound(f"Table {self.backing_table_id} does not exist.")

    def insert_rows(self, rows):
        mutations_batcher: MutationsBatcher = self.table.mutations_batcher()
        print("Inserting", len(rows), "rows.")
        row_count = 0
        for row in rows:
            # deterministic row keys to prevent duplicate rows
            row_key_elements: list = []
            for i, col_name in enumerate(self.columns):
                row_key_elements.append(f"#{col_name}#{row[i]}")

            # hash rowkey to be less than 4096 bytes
            long_row_key: str = "".join(row_key_elements)
            row_key = hashlib.sha256(long_row_key.encode("utf-8")).hexdigest()

            direct_row: DirectRow = self.table.direct_row(row_key.encode("utf-8"))
            for i, col_name in enumerate(self.columns):
                value = row[i]
                if value is None:
                    continue  # skip nulls
                # Write into the column family (sanitized name) with qualifier b'value'
                direct_row.set_cell(
                    DEFAULT_COLUMN_FAMILY, col_name, str(value).encode("utf-8")
                )
            mutations_batcher.mutate(direct_row)

            row_count += 1
            if row_count % 200 == 0:
                print("Inserted ", row_count, "rows.")
        mutations_batcher.flush()


@contextlib.contextmanager
def get_db_cursor(db_path):
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    try:
        yield conn.cursor()
    finally:
        conn.close()


def get_all_tables_and_columns(cur: sqlite3.Cursor) -> dict:
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    tables = [row[0] for row in cur.fetchall()]
    db_schema = {}
    for table in tables:
        cur.execute(f'PRAGMA table_info("{table}")')
        columns = [(col[1], col[2]) for col in cur.fetchall()]  # (name, type)
        db_schema[table] = columns
    return db_schema


def get_rows_from_sqlite(cur: sqlite3.Cursor, table_name, limit) -> list:
    cur.execute(f"SELECT * FROM `{table_name}`")
    if limit > 0:
        rows = cur.fetchmany(limit)
    else:
        rows = cur.fetchall()
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load a sqlite database into Bigtable."
    )
    parser.add_argument(
        "--db_path", type=str, help="Path or glob pattern for the sqlite database(s)."
    )
    parser.add_argument(
        "--gcp_project_id", type=str, default="cloud-db-nl2sql", help="GCP project ID."
    )
    parser.add_argument(
        "--instance_id", type=str, default="evalbench", help="Bigtable instance ID."
    )
    parser.add_argument(
        "--table", type=str, default=None, help="Table name to inspect."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit the number of rows to fetch. Unprovided or a value of 0 means no limit.",
    )
    # explicitly look for tables, comma separated
    parser.add_argument(
        "--tables",
        type=str,
        default=None,
        help="A comma-separated list of tables to load.",
    )
    parser.add_argument(
        "--table_op",
        type=str,
        choices=[e.value for e in TableOp],
        default=TableOp.NO_ACTION.value,
        help="Choose a table operation, defaults to doing nothing.",
    )
    parser.add_argument(
        "--view_op",
        type=str,
        choices=[e.value for e in LogicalViewType],
        default=LogicalViewType.NO_ACTION.value,
        help="Choose a view operation, defaults to doing nothing.",
    )

    args = parser.parse_args()

    # Initialize Bigtable clients
    admin_client: bigtable.Client = bigtable.Client(
        project=args.gcp_project_id, admin=True
    )
    data_client: BigtableClient = BigtableClient()
    instance: Instance = admin_client.instance(args.instance_id)

    # filter by tables if the arg is provided.
    allowed_tables = None
    if args.tables:
        allowed_tables = {t.strip() for t in args.tables.split(",")}

    # get all .sqlite database file paths that match db_path regex
    db_paths = [
        path
        for path in glob.glob(os.path.expanduser(args.db_path))
        if path.endswith(".sqlite")
    ]

    if not db_paths:
        print(f"No databases found matching path: {args.db_path}")
        exit()
    print(f"Found {len(db_paths)} databases to load.")

    for db_path in db_paths:
        print(f"--- Processing database: {db_path} ---")
        with get_db_cursor(db_path=Path(db_path)) as cur:
            # fetch database schema
            db_schema = get_all_tables_and_columns(cur)

            print("Fetched database schema:")
            for table_id in db_schema.keys():
                print("Table:", table_id)
                columns = db_schema[table_id]
                count = 2
                for col, col_type in columns[:count]:
                    print(f"  {col}: {col_type}")
                if len(columns) > count:
                    print(f"  ... ({len(columns) - count} more columns)")
            print()

            for tbl in db_schema.keys():
                print("Processing table: ", tbl)

                if allowed_tables and tbl not in allowed_tables:
                    continue

                cols = db_schema[tbl]
                # Connect or create the bigtable object
                bt_table = BigtableTableBuilder(
                    admin_client.instance_admin_client,
                    instance,
                    tbl,
                    {col_name: col_type for col_name, col_type in cols},
                    gcp_project_id=args.gcp_project_id,
                    instance_id=args.instance_id,
                )

                # table operations
                if args.table_op == TableOp.DELETE_ONLY:
                    bt_table.delete()
                    continue
                elif args.table_op == TableOp.REBUILD:
                    bt_table.delete()
                    bt_table.create()
                    # get and insert rows
                    rows = get_rows_from_sqlite(cur, tbl, limit=args.limit)
                    print(f"Fetched {len(rows)} rows from sqlite table", tbl)
                    bt_table.insert_rows(rows)
                    print("Inserted rows into Bigtable.")

                # logical view operations
                try:
                    bt_table.test_connection()
                except NotFound:
                    print(
                        f"Table {bt_table.backing_table_id} not found. "
                        "Use --table_op=REBUILD to create it."
                    )
                    continue

                logical_view_builder_args = dict(
                    instance_admin_client=admin_client.instance_admin_client,
                    gcp_project_id=args.gcp_project_id,
                    instance_id=args.instance_id,
                    from_table=bt_table.backing_table_id,
                    columns={col_name: col_type for col_name, col_type in cols},
                )

                if args.view_op == LogicalViewType.TYPED:
                    # logical view has the same name as the sqlite table
                    logical_view_id = tbl
                    view_builder = TypedLogicalViewBuilder(
                        **logical_view_builder_args, logical_view_id=logical_view_id
                    )
                    view_builder.delete()
                    view_builder.build()
                elif args.view_op == LogicalViewType.UNTYPED:
                    logical_view_id = tbl + "-untyped"
                    view_builder = UntypedLogicalViewBuilder(
                        **logical_view_builder_args, logical_view_id=logical_view_id
                    )
                    view_builder.delete()
                    view_builder.build()
