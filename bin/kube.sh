#!/bin/sh

cd /tmp

# Configure evaluation timestamp.
# TODO: Fix me.
EVAL_TIMESTAMP=0

# Configure client replicas.
CLIENT_REPLICAS=2

# Configure server replicas.
SERVER_REPLICAS=1

# Configure branch.
LASP_BRANCH=kube

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
    name: lasp-server
  spec:
    replicas: ${SERVER_REPLICAS}
    template:
      metadata:
        labels:
          evaluation-timestamp: "${EVAL_TIMESTAMP}"
          tag: server
          run: lasp-server
      spec:
        containers:
        - name: lasp-server
          image: cmeiklejohn/lasp-dev
          env:
          - name: TAG
            value: server
          - name: WEB_PORT
            value: "8080"
          - name: PEER_PORT
            value: "9090"
          - name: LASP_BRANCH
            value: ${LASP_BRANCH}
          - name: APISERVER
            value: ${APISERVER}
          - name: TOKEN
            value: ${TOKEN}
          - name: AWS_ACCESS_KEY_ID
            value: ${AWS_ACCESS_KEY_ID}
          - name: AWS_SECRET_ACCESS_KEY
            value: ${AWS_SECRET_ACCESS_KEY}
          - name: EVALUATION_TIMESTAMP
            value: ${EVALUATION_TIMESTAMP}
---
  apiVersion: extensions/v1beta1
  kind: Deployment
  metadata:
    name: lasp-client
  spec:
    replicas: ${CLIENT_REPLICAS}
    template:
      metadata:
        labels:
          evaluation-timestamp: "${EVAL_TIMESTAMP}"
          tag: client
          run: lasp-client
      spec:
        containers:
        - name: lasp-client
          image: cmeiklejohn/lasp-dev
          env:
          - name: TAG
            value: client
          - name: WEB_PORT
            value: "8080"
          - name: PEER_PORT
            value: "9090"
          - name: LASP_BRANCH
            value: ${LASP_BRANCH}
          - name: APISERVER
            value: ${APISERVER}
          - name: TOKEN
            value: ${TOKEN}
          - name: AWS_ACCESS_KEY_ID
            value: ${AWS_ACCESS_KEY_ID}
          - name: AWS_SECRET_ACCESS_KEY
            value: ${AWS_SECRET_ACCESS_KEY}
          - name: EVALUATION_TIMESTAMP
            value: ${EVALUATION_TIMESTAMP}
EOF

echo "Deleting deployments."
kubectl delete deployments --all
echo

echo "Deleting lasp deployments and servies."
kubectl delete -f /tmp/lasp.yaml
echo

echo "Sleeping until deployment terminates."
sleep 30

echo "Creating deployment."
kubectl create -f /tmp/lasp.yaml
echo

sleep 10

echo "Tailing logs."
export SERVER_POD_NAME=$(kubectl get pods | grep server | awk '{print $1}')
kubectl logs --tail=-1 -f $SERVER_POD_NAME