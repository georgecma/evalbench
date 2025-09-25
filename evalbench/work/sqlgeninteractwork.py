"""Work is the base class for all work items."""

from typing import Any
from work import Work


class SQLGenInteractWork(Work):
    """SQLGenInteractWork Generates SQL from the generator."""

    def __init__(self, generator: Any, eval_result: dict):
        self.generator = generator
        self.eval_result = eval_result

    def run(self, work_config: str = None) -> dict:
        """Runs the work item.

        Args:
          work_config:

        Returns:

        """
        generated = None
        sql_generator_error = None
        item = self.eval_result["payload"]
        if self.eval_result["prompt_generator_error"] is None:
            try:
                generated = self.generator.generate(item["prompt"])
                item[f"prediction_turn_{item['turn']}"] = generated
            except Exception as e:
                print(e)
                sql_generator_error = str(e)

        self.eval_result["generated"] = generated
        self.eval_result["sql_generator_error"] = sql_generator_error
        return self.eval_result
