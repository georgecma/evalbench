#!/bin/bash
gcloud network-services grpc-routes delete evalbench-grpc-route --project=cloud-db-nl2sql --location=global
gcloud network-services meshes delete direct-google-access-evalbench-mesh --project=cloud-db-nl2sql --location=global
gcloud beta compute backend-services remove-backend evalbench-directpath-bs \
  --network-endpoint-group evalbench-directpath-neg \
  --network-endpoint-group-zone us-central1-c \
  --global --project=cloud-db-nl2sql
gcloud beta compute backend-services delete evalbench-directpath-bs \
  --global --project=cloud-db-nl2sql
gcloud compute firewall-rules delete gke-evalbench-directpath-firewall \
  --project=cloud-db-nl2sql
gcloud compute health-checks delete evalbench-directpath-health-check \
  --global --project=cloud-db-nl2sql
gcloud beta container clusters delete evalbench-directpath-cluster \
  --zone=us-central1-c --project=cloud-db-nl2sql
gcloud compute networks subnets delete evalbench-directpath-subnet \
  --region us-central1 --project=cloud-db-nl2sql
gcloud compute networks delete evalbench-directpath --project=cloud-db-nl2sql