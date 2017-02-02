#!/bin/sh

cd /tmp

cat <<EOF > lasp.yaml
  apiVersion: v1
  kind: Service
  metadata:
    name: lasp
    labels:
      run: lasp
  spec:
    type: NodePort
    ports:
    - port: 8080
      protocol: TCP
      name: http
    - port: 443
      protocol: TCP
      name: https
    selector:
      run: lasp
---
  apiVersion: extensions/v1beta1
  kind: Deployment
  metadata:
    name: lasp
  spec:
    replicas: 1
    template:
      metadata:
        labels:
          run: lasp
      spec:
        containers:
        - name: lasp
          image: cmeiklejohn/lasp-dev
          env:
          - name: WEB_PORT
            value: 8080
          - name: LASP_BRANCH
            value: kube
EOF

echo "Deleting deployments."
kubectl delete -f /tmp/lasp.yaml
echo

echo "Sleeping until deployment terminates."
sleep 30

echo "Creating deployment."
kubectl create -f /tmp/lasp.yaml
echo

export POD_NAME=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}')
echo "Podname: " $POD_NAME

sleep 10

echo "Tailing logs."
kubectl logs --tail=-1 -f $POD_NAME
