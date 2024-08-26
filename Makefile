#!/usr/bin/make -f

default:apply
.PHONY: default

SHELL := /bin/bash


build:
	docker build  -t evalbench -f evalbench_service/Dockerfile .

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