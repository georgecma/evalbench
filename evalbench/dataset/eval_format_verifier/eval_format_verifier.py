"""Verifies the format of the JSON file for the evaluation pipeline."""

from collections.abc import Sequence
import json
import os
from typing import Any

from absl import app
import jsonschema

import eval_format


def validate_json_format(json_data: Any) -> None:
    """Validates the JSON format of the data.

    The format of each json input is as follows

      id: int # unique id
      nl_prompt: str # natural language input to sqlgen
      query_type: str # ddl, dml, dql, etc.
      database: str # target database
      dialects: List[str] # supported sql dialects
      golden_sql: Dict[str, List[str]] # ideal sql statements for each dialect
      eval_query: Dict[str, List[str]] # queries to evaluate the result of the
      'golden_sql'
      setup_sql: Dict[str, List[str]] # queries to set up the database before
      execution
      cleanup_sql: Dict[str, List[str]] # queries to clean up the database after
      execution
      tags: List[str] # descriptive tags (e.g., "DML", "INSERT")
      other: Dict[str, str] # additional metadata (e.g., "Compare Type",
      "Context")

    Args:
      json_data: The contents of the JSON file to validate.
    """
    seen_ids = set()
    idx = 0
    try:
        for idx, item in enumerate(json_data):
            jsonschema.validate(instance=item, schema=eval_format.EVAL_FORMAT)
            item_id = item.get("id")
            if item_id in seen_ids:
                raise ValueError(f"id {item_id} is not unique.")
            seen_ids.add(item_id)
            if not item.get("dialects"):
                raise ValueError(f"item {item.get('id')} has no dialects.")
            for dialect in item.get("dialects"):
                if dialect not in eval_format.SUPPORTED_DIALECTS:
                    raise ValueError(
                        f"dialect {dialect} is not supported on item {item.get('id')}"
                    )
            for sql_type in [
                "golden_sql",
                "eval_query",
                "setup_sql",
                "cleanup_sql",
            ]:
                if item.get(sql_type):
                    for dialect in item.get("dialects"):
                        if dialect not in item.get(sql_type):
                            raise ValueError(
                                f"dialect {dialect} is specified but not added to"
                                f" {sql_type} on item {item.get('id')}"
                            )
    except (jsonschema.exceptions.ValidationError, ValueError) as e:
        if idx >= 0:
            item_id = json_data[idx].get("id")
            if item_id:
                raise ValueError(
                    f"Found error on item at index {idx} with id {item_id}."
                ) from e
            else:
                raise ValueError(f"Found error on item at index {idx}.") from e


def validate_json_file(json_file: str) -> None:
    """Opens a JSON file and returns the data as a list of dictionaries."""
    try:
        base_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'datasets')
        json_file_path = os.path.join(base_path, json_file)
        json_file_path = os.path.abspath(json_file_path)
        with open(json_file_path, "r") as f:
            json_data = list()
            json_data.extend(json.load(f))
            if not isinstance(json_data, list):
                raise ValueError("The file does not contain a valid JSON list.")
            validate_json_format(json_data)
    except Exception as e:
        raise ValueError(
            f"Validation error at the file level in file {json_file}."
        ) from e


def main(argv: Sequence[str]) -> None:
    if len(argv) < 1:
        print("Hello")
        raise app.UsageError("Please provide the path to the JSON files.")

    for json_file in argv[1:]:
        validate_json_file(json_file)
        print(f"JSON format is valid for {json_file}.")


if __name__ == "__main__":
    app.run(main)
