#!/usr/bin/make -f

default:apply
.PHONY: default

SHELL := /bin/bash


build:
	docker build  -t evalbench -f evalbench_service/Dockerfile .

container:
	docker run --rm --net=host --name=evalbench_container -v ~/.config/gcloud:/root/.config/gcloud -e GOOGLE_CLOUD_PROJECT=cloud-db-nl2sql evalbench:latest 

push:
	docker image tag evalbench:latest us-central1-docker.pkg.dev/cloud-db-nl2sql/evalbench/eval_server:latest
	docker push us-central1-docker.pkg.dev/cloud-db-nl2sql/evalbench/eval_server:latest

deploy:
	gcloud container clusters get-credentials evalbench-directpath-cluster --zone us-central1-c --project cloud-db-nl2sql
	kubectl apply -f evalbench_service/k8s/namespace.yaml
	kubectl apply -f evalbench_service/k8s/ksa.yaml
	kubectl apply -f evalbench_service/k8s/service.yaml
	kubectl apply -f evalbench_service/k8s/evalbench.yaml

undeploy:
	gcloud container clusters get-credentials evalbench-directpath-cluster --zone us-central1-c --project cloud-db-nl2sql
	kubectl delete -f evalbench_service/k8s/evalbench.yaml
	kubectl delete -f evalbench_service/k8s/service.yaml

proto:
	@python -m grpc_tools.protoc \
		--proto_path=evalbench/evalproto \
		--python_out=evalbench/evalproto \
		--pyi_out=evalbench/evalproto \
		--grpc_python_out=evalbench/evalproto \
		--experimental_editions evalbench/evalproto/*.proto

clean:
	@rm -fr evalbench/evalproto/*.py
	@rm -fr evalbench/evalproto/*.pyi

test:
	@nox

style:
	@pycodestyle --max-line-length=120 evalbench

run:
	@cd evalbench_service;./run_service.sh

.PHONY: run