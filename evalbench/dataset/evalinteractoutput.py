from dataset.evalinteractinput import EvalInteractInputRequest


class EvalInteractOutput(dict):
    def __init__(
        self,
        evalinteractinputrequest: EvalInteractInputRequest,
    ):
        self.update(evalinteractinputrequest.__dict__)
