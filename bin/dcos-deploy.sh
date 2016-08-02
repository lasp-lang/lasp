#!/bin/bash

ENV_VARS=(
  DCOS
  TOKEN
  ELB_HOST
  EVALUATION_PASSPHRASE
  MODE
  BROADCAST
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

cat <<EOF > lasp-server.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "lasp-server",
  "dependencies": [],
  "constraints": [["hostname", "UNIQUE", ""]],
  "cpus": 1.0,
  "mem": 2048.0,
  "instances": 1,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "cmeiklejohn/lasp-dev",
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
    "AD_COUNTER_SIM_CLIENT": "true",
    "MODE": "$MODE",
    "BROADCAST": "$BROADCAST",
    "EVALUATION_PASSPHRASE": "$EVALUATION_PASSPHRASE",
    "DCOS": "$DCOS",
    "TOKEN": "$TOKEN"
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
curl -k -v -v -v -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X DELETE $DCOS/service/marathon/v2/apps/lasp-server
sleep 2

echo ">>> Adding lasp-server to Marathon"
curl -k -v -v -v -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X POST -d @lasp-server.json $DCOS/service/marathon/v2/apps
echo
sleep 10

cat <<EOF > lasp-client.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "lasp-client",
  "dependencies": [],
  "cpus": 1.0,
  "mem": 2048.0,
  "instances": 5,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "cmeiklejohn/lasp-dev",
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
    "MODE": "$MODE",
    "BROADCAST": "$BROADCAST",
    "EVALUATION_PASSPHRASE": "$EVALUATION_PASSPHRASE",
    "DCOS": "$DCOS",
    "TOKEN": "$TOKEN"
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
curl -k -v -v -v -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X DELETE $DCOS/service/marathon/v2/apps/lasp-client
sleep 2

echo ">>> Adding lasp-client to Marathon"
curl -k -v -v -v -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X POST -d @lasp-client.json $DCOS/service/marathon/v2/apps
echo
sleep 10
