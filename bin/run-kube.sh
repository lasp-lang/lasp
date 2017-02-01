#!/usr/bin/env sh

echo "Starting kubernetes pod for lasp."
kubectl run lasp \
  --image=docker.io/cmeiklejohn/lasp-dev \
  --env="LASP_BRANCH=kube"
