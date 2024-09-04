"""The format of the eval JSON to be used with jsonschema."""
from typing import Any, List, Optional, TypedDict


class DialectBasedSQL(TypedDict):
    postgres: Optional[List[str]]
    mysql: Optional[List[str]]
    sqlserver: Optional[List[str]]


class EvalFormat(TypedDict):
    """The type to be used with jsonschema."""

    id: int
    nl_prompt: str
    query_type: str
    database: str
    dialects: List[str]
    golden_sql: DialectBasedSQL
    eval_query: Optional[DialectBasedSQL]
    setup_sql: Optional[DialectBasedSQL]
    cleanup_sql: Optional[DialectBasedSQL]
    tags: Optional[List[str]]
    other: Optional[dict[str, Any]]


SUPPORTED_DIALECTS = ["postgres", "mysql", "sqlserver"]

EVAL_FORMAT = {
    "type": "object",
    "properties": {
        "id": {"type": "number"},
        "nl_prompt": {"type": "string"},
        "query_type": {"type": "string"},
        "database": {"type": "string"},
        "dialects": {"type": "array", "items": {"type": "string"}},
        "golden_sql": {"$ref": "#/definitions/dialect_based_sql"},
        "eval_query": {"$ref": "#/definitions/dialect_based_sql"},
        "setup_sql": {"$ref": "#/definitions/dialect_based_sql"},
        "cleanup_sql": {"$ref": "#/definitions/dialect_based_sql"},
        "tags": {"type": "array", "items": {"type": "string"}},
        "other": {"type": "object"},
    },
    "definitions": {
        "dialect_based_sql": {
            "type": "object",
            "properties": {
                "postgres": {
                    "type": "array",
                    "items": {"type": ["string", "null"]},
                },
                "mysql": {
                    "type": "array",
                    "items": {"type": ["string", "null"]},
                },
                "sqlserver": {
                    "type": "array",
                    "items": {"type": ["string", "null"]},
                },
            },
        }
    },
    "required": [
        "id",
        "nl_prompt",
        "query_type",
        "database",
        "dialects",
        "golden_sql",
    ],
}
