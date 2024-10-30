from dataset.nl2code_evalinput import EvalInputRequest


class EvalOutput(dict):
    def __init__(
        self,
        evalinput: EvalInputRequest,
    ):
        data = {
            'id': evalinput.id,
            'prompt': evalinput.user_action.prompt,
            'file_path': evalinput.user_action.file_path,
            'description': evalinput.description,
            'current_file_content': evalinput.current_file_content,
            'generated_code': evalinput.generated_code,
            'dbcodegen_time': evalinput.dbcodegen_time,
            'dbcodegen_error': evalinput.dbcodegen_error,
            'golden_code': evalinput.golden_code,
            'job_id': evalinput.job_id
        }
        self.update(data)