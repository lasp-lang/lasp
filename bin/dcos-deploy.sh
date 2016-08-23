#!/bin/bash

ENV_VARS=(
  DCOS
  TOKEN
  EVALUATION_PASSPHRASE
  ELB_HOST
  PEER_SERVICE
  MODE
  BROADCAST
  SIMULATION
  EVAL_ID
  EVAL_TIMESTAMP
  CLIENT_NUMBER
  HEAVY_CLIENTS
  PARTITION_PROBABILITY
  AAE_INTERVAL
  DELTA_INTERVAL
  INSTRUMENTATION
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
)

for ENV_VAR in "${ENV_VARS[@]}"
do
  if [ -z "${!ENV_VAR}" ]; then
    echo ">>> ${ENV_VAR} is not configured; please export it."
    exit 1
  fi
done

echo ">>> Beginning deployment!"

echo ">>> Configuring Lasp"
cd /tmp

# Memory of each VM.
MEMORY=1024.0

# CPU of each VM.
CPU=0.5

cat <<EOF > lasp-server.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "lasp-server",
  "dependencies": [],
  "constraints": [["hostname", "UNIQUE", ""]],
  "cpus": $CPU,
  "mem": $MEMORY,
  "instances": 1,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "cmeiklejohn/lasp-hyparview-dcos",
      "network": "HOST",
      "forcePullImage": true,
      "parameters": [
        { "key": "oom-kill-disable", "value": "true" }
      ]
    }
  },
  "ports": [0, 0],
  "env": {
    "AD_COUNTER_SIM_SERVER": "true",
    "DCOS": "$DCOS",
    "TOKEN": "$TOKEN",
    "EVALUATION_PASSPHRASE": "$EVALUATION_PASSPHRASE",
    "PEER_SERVICE": "$PEER_SERVICE",
    "MODE": "$MODE",
    "BROADCAST": "$BROADCAST",
    "SIMULATION": "$SIMULATION",
    "EVAL_ID": "$EVAL_ID",
    "EVAL_TIMESTAMP": "$EVAL_TIMESTAMP",
    "CLIENT_NUMBER": "$CLIENT_NUMBER",
    "HEAVY_CLIENTS": "$HEAVY_CLIENTS",
    "PARTITION_PROBABILITY": "$PARTITION_PROBABILITY",
    "AAE_INTERVAL": "$AAE_INTERVAL",
    "DELTA_INTERVAL": "$DELTA_INTERVAL",
    "INSTRUMENTATION": "$INSTRUMENTATION",
    "AWS_ACCESS_KEY_ID": "$AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY": "$AWS_SECRET_ACCESS_KEY"
  },
  "labels": {
      "HAPROXY_GROUP":"external",
      "HAPROXY_0_VHOST":"$ELB_HOST"
  },
  "healthChecks": [
    {
      "path": "/api/health",
      "portIndex": 0,
      "protocol": "HTTP",
      "gracePeriodSeconds": 300,
      "intervalSeconds": 60,
      "timeoutSeconds": 20,
      "maxConsecutiveFailures": 3,
      "ignoreHttp1xx": false
    }
  ]
}
EOF

echo ">>> Removing lasp-server from Marathon"
curl -s -k -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X DELETE $DCOS/service/marathon/v2/apps/lasp-server > /dev/null
sleep 2

echo ">>> Adding lasp-server to Marathon"
curl -s -k -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X POST -d @lasp-server.json "$DCOS/service/marathon/v2/apps?force=true" > /dev/null
sleep 10

cat <<EOF > lasp-client.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "lasp-client",
  "dependencies": [],
  "cpus": $CPU,
  "mem": $MEMORY,
  "instances": $CLIENT_NUMBER,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "cmeiklejohn/lasp-hyparview-dcos",
      "network": "HOST",
      "forcePullImage": true,
      "parameters": [
        { "key": "oom-kill-disable", "value": "true" }
      ]
    }
  },
  "ports": [0, 0],
  "env": {
    "AD_COUNTER_SIM_CLIENT": "true",
    "DCOS": "$DCOS",
    "TOKEN": "$TOKEN",
    "EVALUATION_PASSPHRASE": "$EVALUATION_PASSPHRASE",
    "PEER_SERVICE": "$PEER_SERVICE",
    "MODE": "$MODE",
    "BROADCAST": "$BROADCAST",
    "SIMULATION": "$SIMULATION",
    "EVAL_ID": "$EVAL_ID",
    "EVAL_TIMESTAMP": "$EVAL_TIMESTAMP",
    "CLIENT_NUMBER": "$CLIENT_NUMBER",
    "HEAVY_CLIENTS": "$HEAVY_CLIENTS",
    "PARTITION_PROBABILITY": "$PARTITION_PROBABILITY",
    "AAE_INTERVAL": "$AAE_INTERVAL",
    "DELTA_INTERVAL": "$DELTA_INTERVAL",
    "INSTRUMENTATION": "$INSTRUMENTATION",
    "AWS_ACCESS_KEY_ID": "$AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY": "$AWS_SECRET_ACCESS_KEY"
  },
  "healthChecks": [
    {
      "path": "/api/health",
      "portIndex": 0,
      "protocol": "HTTP",
      "gracePeriodSeconds": 300,
      "intervalSeconds": 60,
      "timeoutSeconds": 20,
      "maxConsecutiveFailures": 3,
      "ignoreHttp1xx": false
    }
  ]
}
EOF

echo ">>> Removing lasp-client from Marathon"
curl -s -k -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X DELETE $DCOS/service/marathon/v2/apps/lasp-client > /dev/null
sleep 2

echo ">>> Adding lasp-client to Marathon"
curl -s -k -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X POST -d @lasp-client.json "$DCOS/service/marathon/v2/apps?force=true" > /dev/null
sleep 10
