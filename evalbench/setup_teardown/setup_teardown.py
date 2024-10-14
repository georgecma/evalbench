import os
import sys
import importlib
import logging
import csv
import pandas as pd
from google.protobuf import text_format
from .schema_details.schema_detail_pb2 import SchemaDetails
from databases.util import is_bat_dataset

sys.path.append('..')
sys.path.append('setup_teardown')

util_config = importlib.import_module("util.config")
databaseHandler = importlib.import_module("databaseHandler")

logging.getLogger().setLevel(logging.INFO)


def parse_textproto_file(textproto_path):
    schema_details = SchemaDetails()
    with open(textproto_path, 'r') as file:
        text_format.Merge(file.read(), schema_details)
    return schema_details


def save_checksums_to_csv(checksums, output_file):
    try:
        df = pd.DataFrame(checksums, columns=['table_name', 'checksum'])
        df.to_csv(output_file, index=False)
        logging.info(f"Checksums saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save checksums to {output_file}: {e}")


def calculate_checksum(db_config):
    db_handler = databaseHandler.get_db_handler(db_config)
    setup_file = os.path.join(
        os.path.dirname(__file__),
        f"schema_details/bat/{db_config['database_name']}/setup.yaml"
    )
    setup = util_config.load_yaml_config(setup_file)

    db_engine = db_config['db']
    checksum_commands = setup['setup_commands']["post_data_insertion_checks"][db_engine]
    checksum_query = " UNION ALL ".join(checksum_commands) + ";"
    result, error = db_handler.execute([checksum_query])

    if error:
        logging.error(f"Error executing checksum query: {error}")
        return

    output_file = os.path.join(
        os.path.dirname(__file__),
        f"checksum/{db_config['database_name']}_{db_engine}.csv"
    )
    save_checksums_to_csv(result, output_file)


def create_temp_databases(db_config: dict, num_database: int):
    db_handler = databaseHandler.get_db_handler(db_config)
    return db_handler.create_temp_databases(num_database)


def drop_temp_databases(db_config: dict, temp_databases: list):
    db_handler = databaseHandler.get_db_handler(db_config)
    return db_handler.drop_temp_databases(temp_databases)


def setupDatabase(db_config: dict, database: str, no_data: bool = False, create_user: bool = False):
    if not is_bat_dataset(database):
        return

    def run_setup():
        logging.info("Running setup-teardown...")
        db_engine = db_config['db']

        setup_file = os.path.join(os.path.dirname(__file__), f"schema_details/bat/{database}/setup.yaml")
        setup = util_config.load_yaml_config(setup_file)
        if not setup:
            logging.error("setup.yaml file not found.")
            return False

        db_handler = databaseHandler.get_db_handler(db_config)

        # Dropping tables
        result, error = db_handler.drop_all_tables()
        if error:
            logging.error(f"Error while dropping tables: {error}")
            return False

        setup_commands = {
            "pre_setup": [],
            "schema_creation": [],
            "post_schema_creation": [],
            "data_insertion": [],
            "post_setup": [],
            "post_data_insertion_checks": []
        }

        for section in ["pre_setup", "post_schema_creation", "post_setup"]:
            commands = setup['setup_commands'][section][db_engine]
            setup_commands[section].extend(commands)

        schema_file = os.path.join(os.path.dirname(__file__), f"schema_details/bat/{database}/{db_engine}.textproto")
        schema = parse_textproto_file(schema_file)
        setup_commands['schema_creation'] = db_handler.create_schema_statements(
            schema, setup['setup_commands']['excluded_columns'][db_engine]
        )

        if not no_data:
            data_directory = os.path.join(os.path.dirname(__file__), f"datasets/bat/{database}/")
            setup_commands['data_insertion'] = db_handler.create_insert_statements(data_directory)
            setup_commands['post_data_insertion_checks'] = (
                setup['setup_commands']['post_data_insertion_checks'][db_engine]
            )
            if setup_commands['post_data_insertion_checks']:
                combined_query = " UNION ALL ".join(setup_commands['post_data_insertion_checks'])
                setup_commands['post_data_insertion_checks'] = [combined_query]

        for section in setup_commands:
            if no_data and section in ["data_insertion", "post_data_insertion_checks"]:
                continue
            result, error = db_handler.execute(setup_commands[section])
            if error:
                logging.error(f"Error in section {section}: {error}")
                return False
            if section == "post_data_insertion_checks":
                expected_checksum_csv = os.path.join(os.path.dirname(__file__), f"checksum/{database}_{db_engine}.csv")
                with open(expected_checksum_csv, 'r') as file:
                    csv_reader = csv.DictReader(file)
                    csv_data_set = set((row['table_name'], row['checksum'] or None) for row in csv_reader)
                    result_set = set((entry['table_name'], entry['checksum'] or None) for entry in result)

                    if not csv_data_set == result_set:
                        logging.error("Checksums do not match.")
                        return False

        if create_user:
            logging.info("Creating user...")
            error = db_handler.create_user(db_config)
            if error:
                logging.error(f"Error while creating user: {error}")
                return False

        logging.info("Setup completed successfully.")
        return True

    # Attempt the setup process and retry if it fails
    max_retries = 1
    for attempt in range(max_retries + 1):
        success = run_setup()
        if success:
            break
        if attempt < max_retries:
            logging.info(f"Retrying setup process... (Attempt {attempt + 2})")
        else:
            raise Exception("Setup process failed after retrying.")
