"""
Currently the LLM Rater compares the golden execution results with the
generated sql execution results. It returns a score of 100 for concrete
positive cases, where either there is a Mismatch of Columns names or Extra Relevant
Columns in Generated SQL exists.
"""

import logging
import backoff
from typing import Tuple

from ratelimit import limits
import vertexai
from vertexai.preview.generative_models import GenerationConfig, GenerativeModel


class LLMRater:
    """LLMRater.

    Attributes:
      name:
    """

    def __init__(self, config: dict):
        """Constructor.

        Args:
            config: Configuration dictionary.
        """
        self.name = "nl2code_llmrater"
        self.config = config

        # Weights for different aspects of similarity
        self.weights = {
            "sql_similarity": 0.45,  # SQL query structure and logic similarity
            "method_similarity": 0.20,  # Method signature and return types
            "safety_similarity": 0.15,  # Safety features implementation
            "error_similarity": 0.15,  # Error handling patterns
            "import_similarity": 0.05,  # Import statements
        }

        vertexai.init(
            project=self.config["gcp_project_id"],
            location=self.config["gcp_project_location"],
        )
        self.generation_config = GenerationConfig(temperature=0)
        self.model = GenerativeModel(self.config["model"])

    @backoff.on_exception(
        backoff.constant,
        exception=Exception,
        max_tries=8,
        interval=80,
        jitter=backoff.full_jitter,
    )
    @limits(calls=30, period=60)
    def compare(
        self,
        nl_prompt: str,
        golden_code: str,
        generated_code: str,
    ) -> Tuple[float, str]:
        """Compares golden code with generated code using LLM evaluation.

        Args:
            nl_prompt: User prompt
            golden_code: The reference implementation
            generated_code: The code to be evaluated

        Returns:
            Tuple of (score, detailed_explanation)
        """
        prompt = f"""
        We are evaluating database-aware code generation using LLM against our reference code. Score how closely the generated code matches the reference implementation in each aspect.
        Here's the user prompt:

        PROMPT: {nl_prompt}

        The reference implementation (IMPLEMENTATION #1):

        {golden_code}

        The generated implementation to evaluate (IMPLEMENTATION #2):

        {generated_code}

        Thinking step by step, strictly compare the implementation of the function mentioned in PROMPT only and provide similarity scores (0-100) for each aspect.
        Higher scores mean the generated code more closely matches the reference implementation. If IMPLEMENTATION #2 is empty score everything as 0.
        
        1. SQL Query Similarity (Score 0-100):
           Compare SQL queries for:
           - Exact matching of table names and column names
           - Equivalent JOIN conditions and relationships
           - Matching WHERE clause predicates and conditions
           - Equivalent ORDER BY, GROUP BY, HAVING clauses
           - Similar query structure and logic
           - Matching SELECT/INSERT/UPDATE/DELETE operations
           Score 100 if queries are functionally identical, even if formatted differently.
        
        2. Method Signature & Implementation Similarity (Score 0-100):
           Compare method signatures and implementation for:
           - Matching method names and signatures
           - Compatible parameter types and return types
           - Similar function structure and logic flow
           - Equivalent business logic implementation
           Score 100 if methods serve the same purpose with same signature.
        
       3. Safety Feature Similarity (Score 0-100):
           Compare safety implementations for:
           - Matching SQL injection prevention (prepared statements)
           - Similar connection management patterns
           - Equivalent transaction handling
           - Resource cleanup approaches
           Score 100 if safety measures are equivalent.

        4. Error Handling Similarity (Score 0-100):
           Compare error handling for:
           - Similar exception types and handling
           - Equivalent error recovery mechanisms
           - Matching logging patterns
           - Database-specific error handling
           Score 100 if error handling approaches match.

        5. Import Statement Similarity (Score 0-100):
           Compare imports for:
           - Required dependencies present
           - No missing critical imports
           - Similar module usage
           Score 100 if all necessary imports are present.


        Strictly follow the format mentioned to give output:
        1. First, provide scores in following format:
        SQL_SIMILARITY_SCORE: [number]
        METHOD_SIMILARITY_SCORE: [number]
        SAFETY_SIMILARITY_SCORE: [number]
        ERROR_SIMILARITY_SCORE: [number]
        IMPORT_SIMILARITY_SCORE: [number]
        
        2. Give DETAILED_ANALYSIS
        [Provide a detailed analysis of similarities and differences in each aspect.
        Focus on explaining why scores were deducted when implementations differ.
        Highlight any functional equivalences even when syntax differs.]
        
        3. Do not add markdown format to output strictly.
        """

        logging.debug("\n --------- prompt:   --------- \n %s ", prompt)
        response = self.model.generate_content(
            prompt, generation_config=self.generation_config
        ).text

        logging.debug("\n --------- llm_rater_output:   --------- \n %s ", response)

        scores = {
            "sql_similarity": 0,
            "method_similarity": 0,
            "safety_similarity": 0,
            "error_similarity": 0,
            "import_similarity": 0,
        }

        for line in response.split("\n"):
            line = line.strip()
            if line.startswith("SQL_SIMILARITY_SCORE:"):
                try:
                    scores["sql_similarity"] = float(line.split(":")[1].strip())
                except ValueError:
                    logging.warning(f"Error parsing SQL_SIMILARITY_SCORE: {line}")
            elif line.startswith("METHOD_SIMILARITY_SCORE:"):
                try:
                    scores["method_similarity"] = float(line.split(":")[1].strip())
                except ValueError:
                    logging.warning(f"Error parsing METHOD_SIMILARITY_SCORE: {line}")
            elif line.startswith("SAFETY_SIMILARITY_SCORE:"):
                try:
                    scores["safety_similarity"] = float(line.split(":")[1].strip())
                except ValueError:
                    logging.warning(f"Error parsing SAFETY_SIMILARITY_SCORE: {line}")
            elif line.startswith("ERROR_SIMILARITY_SCORE:"):
                try:
                    scores["error_similarity"] = float(line.split(":")[1].strip())
                except ValueError:
                    logging.warning(f"Error parsing ERROR_SIMILARITY_SCORE: {line}")
            elif line.startswith("IMPORT_SIMILARITY_SCORE:"):
                try:
                    scores["import_similarity"] = float(line.split(":")[1].strip())
                except ValueError:
                    logging.warning(f"Error parsing IMPORT_SIMILARITY_SCORE: {line}")

        final_score = (
            scores["sql_similarity"] * self.weights["sql_similarity"]
            + scores["method_similarity"] * self.weights["method_similarity"]
            + scores["safety_similarity"] * self.weights["safety_similarity"]
            + scores["error_similarity"] * self.weights["error_similarity"]
            + scores["import_similarity"] * self.weights["import_similarity"]
        )

        detailed_response = f"""
        Similarity Score Breakdown:
        - SQL Query Similarity: {scores['sql_similarity']:.2f} (weight: {self.weights['sql_similarity']})
        - Method Implementation Similarity: {scores['method_similarity']:.2f} (weight: {self.weights['method_similarity']})
        - Safety Feature Similarity: {scores['safety_similarity']:.2f} (weight: {self.weights['safety_similarity']})
        - Error Handling Similarity: {scores['error_similarity']:.2f} (weight: {self.weights['error_similarity']})
        - Import Statement Similarity: {scores['import_similarity']:.2f} (weight: {self.weights['import_similarity']})

        Final Similarity Score: {final_score:.2f}/100

        Detailed Analysis:
        {response}

        Note: Scores reflect how closely the generated code matches the reference implementation.
        Higher scores indicate better matching with the golden implementation.
        """

        return final_score, detailed_response
