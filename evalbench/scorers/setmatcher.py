"""Set match between the golden query execution result and the generated query execution result. This is the Execution Accuracy measured in BIRD"""

from typing import Tuple

from scorers import comparator


class SetMatcher(comparator.Comparator):
    """SetMatcher.

    Attributes:
      name:
    """

    def __init__(self, config: dict):
        self.name = "set_match"
        self.config = config

    def compare(
        self,
        nl_prompt: str,
        golden_query: str,
        query_type: str,
        golden_execution_result: list,
        golden_eval_result: str,
        golden_error: str,
        generated_query: str,
        generated_execution_result: list,
        generated_eval_result: str,
        generated_error: str,
    ) -> Tuple[float, str]:
        if golden_error or generated_error:
            return 0, None
        else:
            # Current results are a list of Dict. Converting to Tuple for set comparison
            golden_execution_result_tuple = [tuple(d.values()) for d in golden_execution_result]
            generated_execution_result_tuple = [tuple(d.values()) for d in generated_execution_result]
            score = 100 if set(golden_execution_result_tuple) == set(generated_execution_result_tuple) else 0
            return score, None
