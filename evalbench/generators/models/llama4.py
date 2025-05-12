import openai
from .generator import QueryGenerator
from google.auth import default
from google.auth.transport.requests import Request
from util.gcp import get_gcp_project
import logging
from util.sanitizer import sanitize_sql


class Llama4Generator(QueryGenerator):
    """Generator queries using Llama4 model."""

    def __init__(self, querygenerator_config):
        logger = logging.getLogger(__name__)
        super().__init__(querygenerator_config)
        self.name = "gcp_vertex_llama4"

        credentials, _ = default()
        auth_request = Request()
        credentials.refresh(auth_request)

        # GCP project in which Llama4 model API is enabled
        self.project_id = get_gcp_project(querygenerator_config.get("gcp_project_id"))
        # Only us-east5 is supported region for Llama 4 models using Model-as-a-Service (MaaS).
        self.location = "us-east5"

        try:
            self.client = openai.OpenAI(
                base_url=f"https://{self.location}-aiplatform.googleapis.com/v1beta1/projects/{self.project_id}/locations/{self.location}/endpoints/openapi",
                api_key=credentials.token,
            )
        except Exception as e:
            logger.exception(f"Cannot create client due to exception: {e}")

        self.llama4_model = querygenerator_config["llama4_model"]
        self.base_prompt = querygenerator_config.get("base_prompt") or ""

    def generate_internal(self, prompt):
        logger = logging.getLogger(__name__)
        try:
            response = self.client.chat.completions.create(
                model=self.llama4_model,
                messages=[
                    {"role": "user", "content": self.base_prompt + prompt},
                ]
            )

            response_text = response.choices[0].message.content
            response_text = sanitize_sql(response_text)

            return response_text
        except Exception as e:
            logger.exception(f"Error generating responce due to exception: {e}")
