#!/bin/bash

source helpers.sh

TIMESTAMP_FILE=/tmp/.LAST_TIMESTAMP

ENV_VARS=(
  LASP_BRANCH
  DCOS
  ELB_HOST
  PEER_SERVICE
  MODE
  BROADCAST
  SIMULATION
  EVAL_ID
  EVAL_TIMESTAMP
  CLIENT_NUMBER
  HEAVY_CLIENTS
  REACTIVE_SERVER
  PARTITION_PROBABILITY
  AAE_INTERVAL
  DELTA_INTERVAL
  INSTRUMENTATION
  LOGS
  EXTENDED_LOGGING
  MAILBOX_LOGGING
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

if [ -e "$TIMESTAMP_FILE" ]; then
  # Last timestamp used to deploy an experiment
  LAST_TIMESTAMP=$(cat $TIMESTAMP_FILE)

  echo ">>> Removing lasp-server-$LAST_TIMESTAMP from Marathon"
  curl -s -k -H 'Content-type: application/json' -X DELETE $DCOS/service/marathon/v2/apps/lasp-server-$LAST_TIMESTAMP > /dev/null
  sleep 2

  echo ">>> Removing lasp-client-$LAST_TIMESTAMP from Marathon"
  curl -s -k -H 'Content-type: application/json' -X DELETE $DCOS/service/marathon/v2/apps/lasp-client-$LAST_TIMESTAMP > /dev/null
  sleep 2

  echo ">>> Waiting for Mesos to kill all tasks."
  wait_for_completion $LAST_TIMESTAMP
else
  echo ">>> No apps to be removed from Marathon"
fi

echo ">>> Configuring Lasp"
cd /tmp

# Memory of each VM.
SERVER_MEMORY=4096.0
CLIENT_MEMORY=1024.0

# CPU of each VM.
SERVER_CPU=2
CLIENT_CPU=0.5

cat <<EOF > lasp-server.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "lasp-server-$EVAL_TIMESTAMP",
  "dependencies": [],
  "constraints": [["hostname", "UNIQUE", ""]],
  "cpus": $SERVER_CPU,
  "mem": $SERVER_MEMORY,
  "instances": 1,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "vitorenesduarte/lasp-dev",
      "network": "HOST",
      "forcePullImage": true,
      "parameters": [
        { "key": "oom-kill-disable", "value": "true" }
      ]
    }
  },
  "ports": [0, 0],
  "env": {
    "LASP_BRANCH": "$LASP_BRANCH",
    "SIMPLE_SIM_SERVER": "true",
    "DCOS": "$DCOS",
    "PEER_SERVICE": "$PEER_SERVICE",
    "MODE": "$MODE",
    "BROADCAST": "$BROADCAST",
    "SIMULATION": "$SIMULATION",
    "EVAL_ID": "$EVAL_ID",
    "EVAL_TIMESTAMP": "$EVAL_TIMESTAMP",
    "CLIENT_NUMBER": "$CLIENT_NUMBER",
    "HEAVY_CLIENTS": "$HEAVY_CLIENTS",
    "REACTIVE_SERVER": "$REACTIVE_SERVER",
    "PARTITION_PROBABILITY": "$PARTITION_PROBABILITY",
    "AAE_INTERVAL": "$AAE_INTERVAL",
    "DELTA_INTERVAL": "$DELTA_INTERVAL",
    "INSTRUMENTATION": "$INSTRUMENTATION",
    "LOGS": "$LOGS",
    "EXTENDED_LOGGING": "$EXTENDED_LOGGING",
    "MAILBOX_LOGGING": "$MAILBOX_LOGGING",
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

echo ">>> Adding lasp-server-$EVAL_TIMESTAMP to Marathon"
curl -s -k -H 'Content-type: application/json' -X POST -d @lasp-server.json "$DCOS/service/marathon/v2/apps?force=true" > /dev/null
sleep 10

cat <<EOF > lasp-client.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "lasp-client-$EVAL_TIMESTAMP",
  "dependencies": [],
  "cpus": $CLIENT_CPU,
  "mem": $CLIENT_MEMORY,
  "instances": $CLIENT_NUMBER,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "vitorenesduarte/lasp-dev",
      "network": "HOST",
      "forcePullImage": true,
      "parameters": [
        { "key": "oom-kill-disable", "value": "true" }
      ]
    }
  },
  "ports": [0, 0],
  "env": {
    "LASP_BRANCH": "$LASP_BRANCH",
    "SIMPLE_SIM_CLIENT": "true",
    "DCOS": "$DCOS",
    "PEER_SERVICE": "$PEER_SERVICE",
    "MODE": "$MODE",
    "BROADCAST": "$BROADCAST",
    "SIMULATION": "$SIMULATION",
    "EVAL_ID": "$EVAL_ID",
    "EVAL_TIMESTAMP": "$EVAL_TIMESTAMP",
    "CLIENT_NUMBER": "$CLIENT_NUMBER",
    "HEAVY_CLIENTS": "$HEAVY_CLIENTS",
    "REACTIVE_SERVER": "$REACTIVE_SERVER",
    "PARTITION_PROBABILITY": "$PARTITION_PROBABILITY",
    "AAE_INTERVAL": "$AAE_INTERVAL",
    "DELTA_INTERVAL": "$DELTA_INTERVAL",
    "INSTRUMENTATION": "$INSTRUMENTATION",
    "LOGS": "$LOGS",
    "EXTENDED_LOGGING": "$EXTENDED_LOGGING",
    "MAILBOX_LOGGING": "$MAILBOX_LOGGING",
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

echo ">>> Adding lasp-client-$EVAL_TIMESTAMP to Marathon"
curl -s -k -H 'Content-type: application/json' -X POST -d @lasp-client.json "$DCOS/service/marathon/v2/apps?force=true" > /dev/null
sleep 10

echo $EVAL_TIMESTAMP > $TIMESTAMP_FILE
