"""A gRPC servicer that handles EvalService requests."""

from collections.abc import AsyncIterator

from absl import flags
from absl import logging
from typing import Awaitable, Callable, Optional
import contextvars
import yaml
import grpc
from util.config import load_yaml_config, config_to_df
from util import get_SessionManager
from dataset.dataset import load_json, load_dataset_from_json, load_dataset_from_nl2code_json
from dataset import evalinput
from repository import get_repository
import generators.models as models
import generators.prompts as prompts
import evaluator.evaluator as evaluator
import reporting.report as report
import reporting.bqstore as bqstore
import reporting.analyzer as analyzer
import databases
import pathlib


import eval_nl2code_request_pb2
import eval_nl2code_response_pb2
import eval_nl2code_service_pb2_grpc

_experiment_config = flags.DEFINE_string(
    "self.experiment_config",
    "configs/base_experiment_service.yaml",
    "Path to the eval execution configuration file.",
)

SESSIONMANAGER = get_SessionManager()

rpc_id_var = contextvars.ContextVar("rpc_id", default="default")


class SessionManagerInterceptor(grpc.aio.ServerInterceptor):
    def __init__(self, tag: str, rpc_id: Optional[str] = None) -> None:
        self.tag = tag
        self.rpc_id = rpc_id

    async def intercept_service(
        self,
        continuation: Callable[
            [grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]
        ],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        _metadata = dict(handler_call_details.invocation_metadata)
        if rpc_id_var.get() == "default":
            _metadata = dict(handler_call_details.invocation_metadata)
            rpc_id_var.set(self.decorate(_metadata["client-rpc-id"]))
            SESSIONMANAGER.create_session(rpc_id_var.get())
        else:
            rpc_id_var.set(self.decorate(rpc_id_var.get()))
        return await continuation(handler_call_details)

    def decorate(self, rpc_id: str):
        return f"{self.tag}-{rpc_id}"


class EvalServicer(eval_nl2code_service_pb2_grpc.EvalCodeGenServiceServicer):
    """A gRPC servicer that handles EvalService requests."""

    def __init__(self) -> None:
        super().__init__()

        logging.info("EvalBench v1.0.0")

    async def Ping(
        self,
        request: eval_nl2code_request_pb2.PingRequest,
        context: grpc.ServicerContext,
    ) -> eval_nl2code_response_pb2.EvalResponse:
        return eval_nl2code_response_pb2.EvalResponse(response=f"ack")

    async def Connect(
        self,
        request,
        context,
    ) -> eval_nl2code_response_pb2.EvalResponse:
        return eval_nl2code_response_pb2.EvalResponse(response=f"ack")

    async def EvalConfig(
        self,
        request,
        context,
    ) -> eval_nl2code_response_pb2.EvalResponse:
        experiment_config = yaml.safe_load(request.yaml_config.decode("utf-8"))
        session = SESSIONMANAGER.get_session(rpc_id_var.get())
        session["config"] = experiment_config

        # Create the DB
        session["db_config"] = load_yaml_config(experiment_config["database_config"])
        session["model_config"] = load_yaml_config(experiment_config["model_config"])
        return eval_nl2code_response_pb2.EvalResponse(response=f"ack")

    async def ListEvalInputs(
        self,
        request,
        context,
    ) -> eval_nl2code_request_pb2.EvalInputRequest:
        session = SESSIONMANAGER.get_session(rpc_id_var.get())
        logging.info("Retrieve: %s.", rpc_id_var.get())
        experiment_config = session["config"]
        
        repo = get_repository(experiment_config)
        repo.clone()
        
        dataset_config_json = experiment_config["dataset_config"]

        # Load the dataset
        dataset, database = load_dataset_from_nl2code_json(
            dataset_config_json
        )
        session["db_config"]["database_name"] = database
       
        for eval_input in dataset:
            yield eval_nl2code_request_pb2.EvalInputRequest(
                id=f"{eval_input.id}",
                patch=eval_input.patch,
                user_action=eval_input.user_action,
                verification_command= eval_input.verification_command,
                description=eval_input.description,
                application_context=eval_input.application_context,
            )

    async def Eval(
        self,
        request_iterator: AsyncIterator[eval_nl2code_request_pb2.EvalInputRequest],
        context: grpc.ServicerContext,
    ) -> eval_nl2code_response_pb2.EvalResponse:

        dataset = []
        async for request in request_iterator:
            input = eval_nl2code_request_pb2.EvalInputRequest(
                id=request.id,
                patch=request.patch,
                user_action=request.user_action,
                verification_command= request.verification_command,
                description=request.description,
                application_context=request.application_context,
            )
            dataset.append(input)
        session = SESSIONMANAGER.get_session(rpc_id_var.get())

        session["db"] = databases.get_database(session["db_config"])
        # Load the Query Generator
        session["model_config"]["database_config"] = session["db_config"]
        session["model_generator"] = models.get_generator(session["model_config"])
        # Load the Prompt Generator
        session["prompt_generator"] = prompts.get_generator(
            session["db"], session["config"]
        )
        session["eval"] = evaluator.Evaluator(
            session["config"],
            session["prompt_generator"],
            session["model_generator"],
            session["db"],
        )

        eval = session["eval"]
        job_id, run_time = eval.evaluate(dataset)
        logging.info(f"Run eval job_id:{job_id} run_time:{run_time} for {len(dataset)} eval entries.")

        config_df = config_to_df(
            job_id,
            run_time,
            session["config"],
            session["model_config"],
            session["db_config"],
        )
        
        pathlib.Path(f"/tmp/eval_output_{job_id}.json").unlink()
        pathlib.Path(f"/tmp/score_result_{job_id}.json").unlink()
        
        return eval_nl2code_response_pb2.EvalResponse(response=f"ack")
