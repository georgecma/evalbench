FROM docker.io/python:3.11-bookworm AS base-env
RUN pip install --no-cache-dir --upgrade pip

FROM base-env AS protoc-env
RUN pip install --no-cache-dir grpcio-tools
COPY ./evalbench/evalproto /proto
RUN mkdir proto_out
RUN python -m grpc_tools.protoc --proto_path=./proto --python_out=./proto_out --pyi_out=./proto_out --grpc_python_out=./proto_out --experimental_editions ./proto/*.proto

FROM base-env AS build-env
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r ../requirements.txt

FROM gcr.io/distroless/python3-debian12
COPY --from=build-env /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --chown=nonroot:nonroot . /app
COPY --from=protoc-env --chown=nonroot:nonroot /proto_out /app/evalbench/
WORKDIR /app/evalbench
ENV PYTHONPATH=/usr/local/lib/python3.11/site-packages
CMD ["nl2code_eval_server.py"]

EXPOSE 50051
