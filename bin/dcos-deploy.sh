#!/bin/bash

if [ -z "$DCOS" ]; then
  echo ">>> DCOS is not configured; please export DCOS."
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

echo ">>> Beginning deployment!"

echo ">>> Configuring Lasp"
cd /tmp

cat <<EOF > lasp-orchestrator.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "lasp-orchestrator",
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
      "forcePullImage": true
    }
  },
  "ports": [0, 0],
  "env": {
    "AD_COUNTER_SIM_SERVER": "true",
    "DCOS": "$DCOS",
    "TOKEN": "$TOKEN"
  },
  "labels":{
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

echo ">>> Removing orchestrator from Marathon"
curl -k -v -v -v -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X DELETE $DCOS/service/marathon/v2/apps/lasp-orchestrator
sleep 2

echo ">>> Adding orchestrator to Marathon"
curl -k -v -v -v -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X POST -d @lasp-orchestrator.json $DCOS/service/marathon/v2/apps
echo
sleep 10

cat <<EOF > lasp.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "lasp",
  "dependencies": [],
  "cpus": 1.0,
  "mem": 8192.0,
  "instances": 5,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "cmeiklejohn/lasp-dev",
      "network": "HOST",
      "forcePullImage": true
    }
  },
  "ports": [0, 0],
  "env": {
    "AD_COUNTER_SIM_CLIENT": "true",
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

echo ">>> Removing lasp from Marathon"
curl -k -v -v -v -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X DELETE $DCOS/service/marathon/v2/apps/lasp
sleep 2

echo ">>> Adding lasp to Marathon"
curl -k -v -v -v -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X POST -d @lasp.json $DCOS/service/marathon/v2/apps
echo
sleep 10
