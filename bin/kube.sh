#!/bin/sh

cd /tmp

echo "Deleting all deployments"
kubectl delete deployments --all
echo

sleep 30

cat <<EOF > lasp-dev.yaml
  apiVersion: extensions/v1beta1
  kind: Deployment
  metadata:
    name: lasp-dev
  spec:
    replicas: 1
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

echo "Creating deployment"
kubectl create -f /tmp/lasp-dev.yaml
echo

export POD_NAME=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}')
echo "Podname: " $POD_NAME

sleep 10

echo "Tailing logs."
kubectl logs --tail=-1 -f $POD_NAME
