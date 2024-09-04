from dataset.evalinput import EvalInputRequest


class EvalOutput(dict):
    def __init__(
        self,
        evalinput: EvalInputRequest,
    ):
        self.update(evalinput.__dict__)
