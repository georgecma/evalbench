"""Utility functions to manipulate dataset details.""" ""
import re
from typing import Any, Tuple
from eval_format_verifier import eval_format


def is_ddl(sql: str) -> bool:
    """Check if a sql statement is a DDL statement.

    Args:
      sql:

    Returns:
    """
    pattern = r"^\s*(?i:create|alter|drop|grant|revoke)\s"
    return re.fullmatch(pattern, sql) is not None


def is_dml(sql: str) -> bool:
    """Check if a sql statement is a DML statement.

    Args:
      sql:

    Returns:
    """
    pattern = r"(?:^|\s)(?i:insert|update|delete)\s"
    return re.fullmatch(pattern, sql) is not None


def split_sql(statement_string: str) -> list[str]:
    """Split a string of sql statements into a list of statements.

    Args:
      statement_string:

    Returns:
    """
    statement_string_local = statement_string.replace("\\n", "\n")
    return list(map(str.strip, filter(None, statement_string_local.split(";"))))


def get_dialect_based_sql(
    data: eval_format.EvalFormat, sql_type: str, dialect: str
) -> str:
    """Get the dialect based value from the eval format."""
    if sql_type in data:
        value = data.get(sql_type)
        if value and dialect in value:
            dialect_based_value = value[dialect]
        if dialect_based_value:
            return dialect_based_value[0]
    return ""


def transform_csv_row(data: dict[str, str]) -> dict:
    """Transform a csv row into an EvalConfig.

    Args:
      data:

    Returns:
    """
    prompt = data["Prompt"]
    if prompt and str.startswith(prompt, "#"):
        # Hash doesn"t work in all dialects
        prompt = prompt.replace("#", "--", 1)
    if prompt and not str.startswith(prompt, "--"):
        prompt = "-- {}".format(prompt)

    return {
        "context": split_sql(data["Context"]),
        "setup": {
            "default_setup": data["Default Setup"],
            "ddl": split_sql(data["Setup DDL"]),
            "dml": split_sql(data["Setup DML"]),
        },
        "execution_type": data["Query Type"],
        "cleanup": {
            "ddl": split_sql(data["Cleanup DDL"]),
            "dml": split_sql(data["Cleanup DML"]),
        },
        "prompt": prompt,
        "tags": data["Tags (comma separated)"].split(","),
        "cuj_name": data["CUJ"],
        "partials": data["Partials"].split(","),
        "eval_query": data["Eval query"],
        "expected": data["Golden"],
    }


def transform_json_row(
    row: Tuple[eval_format.EvalFormat, str],
) -> dict[str, Any]:
    """Transform a json row into an EvalConfig.

    Args:
      row: dialect and eval json format defined in eval_format.py

    Returns:
      transformed json of prompt and evaluation instructions.
    """
    data, dialect = row
    eval_id = data["id"]
    try:
        return {
            "prompt": data["nl_prompt"],
            "execution_type": data["query_type"],
            "setup": {
                "default_setup": data["database"],
                "ddl": list(
                    filter(
                        is_ddl, get_dialect_based_sql(data, "setup_sql", dialect)
                    )
                ),
                "dml": list(
                    filter(
                        lambda s: is_dml(s) and not is_ddl(s),
                        get_dialect_based_sql(data, "setup_sql", dialect),
                    )
                ),
            },
            "cleanup": {
                "ddl": list(
                    filter(
                        is_ddl, get_dialect_based_sql(data, "cleanup_sql", dialect)
                    )
                ),
                "dml": list(
                    filter(
                        lambda s: is_dml(s) and not is_ddl(s),
                        get_dialect_based_sql(data, "cleanup_sql", dialect),
                    )
                ),
            },
            "tags": data["tags"],
            # Get any tags that match "cuj: x"
            "cuj_name": next(
                (tag for tag in data["tags"] if tag.startswith("cuj:")), ""
            ),

            "eval_query": get_dialect_based_sql(data, "eval_query", dialect),
            "expected": get_dialect_based_sql(data, "golden_sql", dialect),
        }
    except KeyError as e:
        raise KeyError("Failed to parse JSON for test case %s" % eval_id) from e


def transform_legacy_json_row(
    row: Tuple[dict[str, str], str],
) -> dict[str, Any]:
    """Transform a json row into an EvalConfig from the legacy format.

    Args:
      row: dialect and legacy json format for eval prompts and instructions.

    Returns:
      transformed json of prompt and evaluation instructions.
    """
    data, _ = row
    return {
        "prompt": data["prompt"],
        "execution_type": data["query_type"],
        "setup": {
            "default_setup": data["database"],
            "ddl": list(filter(is_ddl, data["setup_sql"])),
            "dml": list(
                filter(lambda s: is_dml(s) and not is_ddl(s), data["setup_sql"])
            ),
        },
        "cleanup": {
            "ddl": list(filter(is_ddl, data["cleanup_sql"])),
            "dml": list(
                filter(lambda s: is_dml(s) and not is_ddl(s), data["cleanup_sql"])
            ),
        },
        "tags": data["tags"],
        "cuj_name": data["cuj"][0] if data["cuj"] else "",
        "partials": data["partials"],
        "eval_query": data["eval_query"][0] if data["eval_query"] else "",
        "expected": data["examples"][0] if data["examples"] else "",
    }
