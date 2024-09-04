"""
Currently the LLM Rater compares the golden execution results with the
generated sql execution results. It returns a score of 100 for concrete
positive cases, where either there is a Mismatch of Columns names or Extra Relevant
Columns in Generated SQL exists.
"""

from typing import Tuple
import logging

from scorers import comparator
import vertexai
from vertexai.preview.generative_models import GenerationConfig, GenerativeModel


class LLMRater(comparator.Comparator):
    """LLMRater.

    Attributes:
      name:
    """

    def __init__(self, config: dict):
        """Constructor.

        Args:
            config: Configuration dictionary.
        """
        self.name = "llmrater"
        self.config = config
        vertexai.init(
            project=self.config["gcp_project_id"],
            location=self.config["gcp_project_location"],
        )
        self.generation_config = GenerationConfig(temperature=0)
        self.model = GenerativeModel(self.config["model"])

    def compare(
        self,
        nl_prompt: str,
        golden_query: str,
        golden_execution_result: str,
        generated_query: str,
        generated_execution_result: str,
    ) -> Tuple[float, str]:
        only_first_n = 50
        if len(golden_execution_result) > only_first_n:
            golden_execution_result = golden_execution_result[:only_first_n]
        if len(generated_execution_result) > only_first_n:
            generated_execution_result = generated_execution_result[:only_first_n]

        prompt = f"""
        We are trying to answer this question by querying a database:

        QUESTION: {nl_prompt}

        The correct answer to this question is:

        OUTPUT #1:

        {golden_execution_result}


        We get the following answer from a second query.

        OUTPUT #2

        {generated_execution_result}


        Thinking step by step, compare the two outputs and look for differences in data presentation.
        Here are steps to follow:

        1) Map all columns in OUTPUT #1 to the same columns in OUTPUT #2. Their names might be different
           so long as the content represents the same data.
        2) If all columns in OUTPUT #1 do not map to OUTPUT #2 then information is missing.
        3) Compare the data in the mapped columns between OUTPUT #1 and OUTPUT #2. The data should be an
           exact match, there should be no extra or missing data in OUTPUT #2

        Rules:
        1) Always assume that OUTPUT #1 is correct.
        2) Only compare the result sets, don't make other assumptions.
        3) OUTPUT #2 can be correct even if the order of columns is different
        4) Adding additional information, like more columns is not a problem as long as they are relevant.
            Numbers, dates, timestamps, measurements or metrics MUST be the same between the two outputs.
        5) It's ok to concatenate or change strings in minor ways (for example, combining first name and last name)

        FINAL QUESTION: Does OUTPUT #2 provide the same information as OUTPUT #1?
        FINAL ANSWER:
        - INFORMATION_MATCHES -- OUTPUT #1 and OUTPUT #2 provide the same information.
        - MISSING_INFORMATION -- Something important is missing from OUTPUT #2.
        - EXTRA_INFORMATION -- Some non-harmful extra relevant columns were added to OUTPUT #2.
        - INCORRECT_INFORMATION -- Some incorrect information was added to OUTPUT #2, likely due to an incorrect
          filter or incorrect aggregation.

        """
        logging.debug("\n --------- prompt:   --------- \n %s ", prompt)
        response = self.model.generate_content(
            prompt, generation_config=self.generation_config
        ).text

        logging.debug("\n --------- llm_rater_output:   --------- \n %s ", response)
        score = (
            100
            if ("INFORMATION_MATCHES" in response or "EXTRA_INFORMATION" in response)
            else 0
        )
        return score, response
