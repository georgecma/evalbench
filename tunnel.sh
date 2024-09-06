#!/bin/bash

# Allow ingress into the database
function configure_connection() {
  while [[ -z "$pod" ]]
  do
    echo "Waiting for POD to be ready"
    sleep 5
    pod=$(kubectl get pod -l "app=evalbench-eval-server"  --no-headers  -o=custom-columns='NAME:metadata.name' -n evalbench-namespace| head -n 1)
    echo "POD:$pod"
  done
  export pod=$pod
}

configure_connection
kubectl port-forward $pod -n evalbench-namespace 50051
