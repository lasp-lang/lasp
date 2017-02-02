#!/bin/sh

cd /tmp

# Get Kubernetes API server.
APISERVER=$(kubectl config view | grep server | cut -f 2- -d ":" | tr -d " " | head -1)

# Get Kubernetes access token.
TOKEN=$(kubectl describe secret $(kubectl get secrets | grep default | cut -f1 -d ' ') | grep -E '^token' | cut -f2 -d':' | tr -d '\t')

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
      name: web
    - port: 9090
      protocol: TCP
      name: peer
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
            value: "8080"
          - name: PEER_PORT
            value: "9090"
          - name: LASP_BRANCH
            value: kube
          - name: APISERVER
            value: ${APISERVER}
          - name: TOKEN
            value: ${TOKEN}
          - name: AWS_ACCESS_KEY_ID
            value: ${AWS_ACCESS_KEY_ID}
          - name: AWS_SECRET_ACCESS_KEY
            value: ${AWS_SECRET_ACCESS_KEY}
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
