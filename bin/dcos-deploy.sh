#!/bin/bash

if [ -z "$DCOS" ]; then
  echo ">>> DCOS is not configured; please export DCOS."
  exit 1
fi

if [ -z "$MODE" ]; then
  echo ">>> MODE is not configured; please export MODE."
  exit 1
fi

if [ -z "$BROADCAST" ]; then
  echo ">>> BROADCAST is not configured; please export BROADCAST."
  exit 1
fi

if [ -z "$TOKEN" ]; then
  echo ">>> TOKEN is not configured; please export TOKEN."
  exit 1
fi

if [ -z "$ELB_HOST" ]; then
  echo ">>> ELB_HOST is not configured; please export ELB_HOST."
  exit 1
fi

if [ -z "$EVALUATION_PASSPHRASE" ]; then
  echo ">>> EVALUATION_PASSPHRASE is not configured; please export EVALUATION_PASSPHRASE."
  exit 1
fi

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
