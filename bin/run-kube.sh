#!/usr/bin/env sh

echo "Deleting all deployments."
kubectl delete deployments --all
echo

echo "Deleting all replica sets."
kubectl delete replicaset --all
echo

echo "Deleting all pods."
kubectl delete pods --all
echo

sleep 30

echo "Starting kubernetes pod for lasp."
kubectl run lasp \
  --image=docker.io/cmeiklejohn/lasp-dev \
  --env="LASP_BRANCH=kube"
echo

export POD_NAME=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}')
echo "Podname: " $POD_NAME

sleep 10

echo "Tailing logs."
kubectl logs --tail=-1 -f $POD_NAME
