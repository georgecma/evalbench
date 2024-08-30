import asyncio
from aiologger import Logger
import grpc
from evalproto import eval_request_pb2, eval_connect_pb2, eval_config_pb2
from evalproto import eval_service_pb2_grpc
import random
import argparse


class EvalbenchClient:
    def __init__(self):
        self.channel = grpc.aio.insecure_channel("127.0.0.1:50051")
        self.stub = eval_service_pb2_grpc.EvalServiceStub(self.channel)
        rpc_id = "{:032x}".format(random.getrandbits(128))
        self.metadata = grpc.aio.Metadata(
            ("client-rpc-id", rpc_id),
        )

    async def ping(self):
        request = eval_request_pb2.EvalRequest()
        response = await self.stub.Ping(request, metadata=self.metadata)
        return response

    async def connect(self):
        request = eval_connect_pb2.EvalConnectRequest()
        request.client_id = "me"
        response = await self.stub.Connect(request, metadata=self.metadata)
        return response

    async def set_evalconfig(self, experiment: str):
        data = None
        with open(experiment, "rb") as f:
            data = f.read()
        request = eval_config_pb2.EvalConfigRequest()
        request.yaml_config = data
        response = await self.stub.EvalConfig(request, metadata=self.metadata)
        return response


async def run(experiment: str) -> None:
    logger = Logger.with_default_handlers(name="evalbench-logger")
    evalbenchclient = EvalbenchClient()
    response = await evalbenchclient.ping()
    logger.info(f"ping Returned: {response.response}")

    response = await evalbenchclient.connect()
    logger.info(f"connect Returned: {response.response}")

    response = await evalbenchclient.ping()
    logger.info(f"ping Returned: {response.response}")

    response = await evalbenchclient.set_evalconfig(experiment)
    logger.info(f"get_evalinput Returned: {response.response}")


async def main():
    logger = Logger.with_default_handlers(name="evalbench-logger")
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", dest="experiment")
    known_args, _ = parser.parse_known_args()

    await run(known_args.experiment)
    # get a set of all running tasks
    all_tasks = asyncio.all_tasks()
    # get the current tasks
    current_task = asyncio.current_task()
    # remove the current task from the list of all tasks
    all_tasks.remove(current_task)
    # report a message
    print(f"Main waiting for {len(all_tasks)} tasks...")
    # suspend until all tasks are completed
    await asyncio.wait(all_tasks)
    await logger.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
