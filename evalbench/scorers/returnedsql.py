from scorers import comparator
from typing import Tuple


class ReturnedSQL(comparator.Comparator):
    """ReturnedSQL scorer checks if the generated SQL query contains anything except comments.

    It assigns a score of 100 if there are non-comment lines, otherwise a score of 0.
    """

    def __init__(self, config: dict):
        self.name = "returned_sql"
        self.config = config

    def compare(
        self,
        eval_item: dict
    ) -> Tuple[float, str]:

        if eval_item["generated_sql"] == "":
            return 100, None

        query_lines = [line.strip() for line in eval_item["generated_sql"].splitlines()]
        has_non_comment_line = any(line and not line.startswith("--") for line in query_lines)

        score = 100 if has_non_comment_line else 0
        return score, None
