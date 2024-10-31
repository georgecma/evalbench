from google.protobuf.json_format import MessageToDict
from google.protobuf.duration_pb2 import Duration
import eval_nl2code_request_pb2

class EvalInputRequest:
    def __init__(
        self,
        id: str,
        user_action: eval_nl2code_request_pb2.UserAction,
        description: str,
        current_file_content: str,
        generated_code: str,
        dbcodegen_time: Duration,
        dbcodegen_error: str,
        golden_code: str,
        job_id: str = "",
    ):
        """Initializes an NL2CodeEvalInputRequest object with all required fields.
        See nl2code_eval_request_pb2 for types
        """
        self.id = id
        self.user_action = user_action
        self.description = description
        self.current_file_content = current_file_content
        self.generated_code = generated_code
        self.dbcodegen_time = dbcodegen_time
        self.dbcodegen_error = dbcodegen_error
        self.golden_code = golden_code
        self.job_id = job_id

    @classmethod
    def init_from_proto(cls, proto: eval_nl2code_request_pb2.EvalInputRequest):
        """Initializes an EvalInputRequest from eval_request_pb2 proto."""
        request = MessageToDict(proto)
        user_action = proto.user_action
        return cls(
            id=request.get("id"),
            user_action=user_action, 
            description=request.get("description"),
            current_file_content=request.get("currentFileContent"),
            generated_code=request.get("generatedCode"),
            dbcodegen_time=request.get("dbcodegenTime"),
            dbcodegen_error=request.get("dbcodegenError"),
            golden_code=request.get("goldenCode"),
            job_id=request.get("jobId"),
        )