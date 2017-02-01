#!/bin/sh

cd /tmp

echo "Deleting all deployments"
kubectl delete deployments --all

cat <<EOF > lasp-dev.yaml
  apiVersion: extensions/v1beta1
  kind: Deployment
  metadata:
    name: lasp-dev
  spec:
    replicas: 2
    template:
      metadata:
        labels:
          run: lasp-dev
      spec:
        containers:
        - name: lasp-dev
          image: cmeiklejohn/lasp-dev
          ports:
          - containerPort: 80
          env:
          - name: LASP_BRANCH
            value: kube
EOF

kubectl create -f /tmp/lasp-dev.yaml
