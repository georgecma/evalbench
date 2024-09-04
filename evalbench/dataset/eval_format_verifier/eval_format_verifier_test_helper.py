"""Helper functions for testing eval_format_verifier."""

import copy
from typing import Any


def get_default_single_json(
    update: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Returns a list of a single json object with the given update.

    Args:
      update: A dictionary of updates to apply to the default json object.
    """
    json_data = [{
        "id": 1,
        "nl_prompt": "count all the pokemon",
        "query_type": "DQL",
        "database": "pokemon",
        "dialects": ["postgres"],
        "golden_sql": {"postgres": ["SELECT COUNT(*) FROM pokemon"]},
        "eval_query": {"postgres": ["SELECT COUNT(*) FROM pokemon"]},
    }]
    if update:
        json_data[0].update(update)
    return json_data


def get_default_good_json_list(num_items: int = -1) -> list[dict[str, Any]]:
    """Returns a list of json objects with the given number of items.

    Args:
      num_items: The number of items to return. If num_items is negative, returns
        all items in default list.
    """
    json_data = copy.deepcopy(_DEFAULT_GOOD_JSON_LIST)
    if num_items < 0:
        return json_data
    elif num_items == 0:
        return []
    elif num_items > 0 and num_items <= len(json_data):
        return json_data[:num_items]
    else:
        final_item = json_data[-1]
        append_items = [final_item] * (num_items - len(json_data))
        for i, item in enumerate(append_items):
            item.update({"id": final_item.get("id") + i})
        return json_data + append_items


_DEFAULT_GOOD_JSON_LIST = [
    {
        "id": 1,
        "nl_prompt": "count all the pokemon",
        "query_type": "DQL",
        "database": "pokemon",
        "dialects": ["postgres"],
        "golden_sql": {"postgres": ["SELECT COUNT(*) FROM pokemon"]},
        "eval_query": {"postgres": ["SELECT COUNT(*) FROM pokemon"]},
        "setup_sql": {"postgres": []},
        "cleanup_sql": {"postgres": []},
        "tags": ["DQL", "COUNT"],
        "other": {"bulbasaur": "rocks"},
    },
    {
        "id": 2,
        "nl_prompt": "what are all the fire type pokemon",
        "query_type": "DQL",
        "database": "pokemon",
        "dialects": ["postgres", "mysql"],
        "golden_sql": {
            "postgres": ["SELECT * FROM pokemon WHERE type = 'fire'"],
            "mysql": ["SELECT * FROM pokemon WHERE type = 'fire'"],
        },
        "eval_query": {
            "postgres": ["SELECT * FROM pokemon WHERE type = 'fire'"],
            "mysql": ["SELECT * FROM pokemon WHERE type = 'fire'"],
        },
    },
    {
        "id": 3,
        "nl_prompt": "Retype the super_fire type pokemon to 🔥",
        "query_type": "DML",
        "database": "pokemon",
        "dialects": ["postgres", "mysql"],
        "golden_sql": {
            "postgres": [
                "UPDATE pokemon SET type = '🔥' WHERE type = 'super_fire'"
            ],
            "mysql": [
                "UPDATE pokemon SET type = '🔥' WHERE type = 'super_fire'"
            ],
        },
        "eval_query": {
            "postgres": ["SELECT 1"],
            "mysql": ["SELECT 1"],
        },
        "setup_sql": {"postgres": [], "mysql": []},
        "cleanup_sql": {
            "postgres": [
                "UPDATE pokemon SET type = 'super_fire' WHERE type = '🔥'",
                "UPDATE pokemon SET type = 'super_fire' WHERE type = '🔥'",
            ],
            "mysql": [
                "UPDATE pokemon SET type = 'super_fire' WHERE type = '🔥'",
                "UPDATE pokemon SET type = 'super_fire' WHERE type = '🔥'",
            ],
        },
    },
]
