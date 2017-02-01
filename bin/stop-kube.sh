#!/bin/sh

echo "Deleting all deployments."
kubectl delete deployments --all

echo "Deleting all replica sets."
kubectl delete replicaset --all

echo "Deleting all pods."
kubectl delete pods --all
