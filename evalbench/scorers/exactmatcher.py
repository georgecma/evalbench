"""Simple comparison strategy that checks if the two execution results are exactly the same."""

from typing import Tuple

from scorers import comparator


class ExactMatcher(comparator.Comparator):
    """ExactMatcher.

    Attributes:
      name:
    """

    def __init__(self, config: dict):
        self.name = "exact_match"
        self.config = config

    def compare(
        self,
        eval_item: dict
    ) -> Tuple[float, str]:
        """Simple comparison strategy that checks if the two execution results are exactly the same."""
        if eval_item["golden_error"] or eval_item["generated_error"]:
            return 0, None
        if self.config and "use_eval_sql" in self.config:
            score = 100 if eval_item["golden_eval_result"] == eval_item["generated_eval_result"] else 0
            return score, None
        else:
            score = 100 if eval_item["golden_result"] == eval_item["generated_result"] else 0
            return score, None
