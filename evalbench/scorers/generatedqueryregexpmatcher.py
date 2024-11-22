"""GeneratedQueryRegexpMatcher."""

import re
from typing import Tuple
from scorers import comparator


class GeneratedQueryRegexpMatcher(comparator.Comparator):
    """Comparator that checks if the generated query matches regular expressions."""

    def __init__(self, config: dict):
        """Constructor.

        Args:
            config: Configuration dictionary.
        """
        self.name = "regexp_matcher"
        self.config = config
        # regexp_string_list: List of strings containing regexps.
        self.regexp_string_list = config["regexp_string_list"]
        # invert_results: If true, any match means a score of 0. If false, any match means a score of 100.
        self.invert_results = config["invert_results"]

    def compare(
        self,
        eval_item: dict
    ) -> Tuple[float, str]:
        score = 0
        matching_regexps = []
        for regexp_string in self.regexp_string_list:
            result = re.search(regexp_string, eval_item["generated_sql"])
            if result:
                matching_regexps.append(result.group(0))
                score = 100

        if self.invert_results:
            score = 100 - score
        return score, str(matching_regexps)
