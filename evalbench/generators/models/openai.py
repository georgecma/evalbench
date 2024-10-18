import time
from openai import OpenAI
from .generator import QueryGenerator


class OAIQueryGenerator(QueryGenerator):

    def __init__(self, base_prompt: str = ""):
        super().__init__()
        self.name = "gpt-4-0125-preview"
        self.base_prompt = base_prompt
        self.name = "openai"

    def generate(self, question: str) -> str:
        start_time = time.time()
        prompt = self.base_prompt + question
        client = OpenAI()
        completion = client.chat.completions.create(
            model="gpt-4-0125-preview",
            messages=[
                {"role": "user", "content": prompt},
                {"role": "assistant", "content": ""},
            ],
            temperature=0.0,
        )
        return completion.choices[0].message.content, time.time() - start_time
