#!/bin/bash

ENV_VARS=(
  DCOS
  TOKEN
  EVALUATION_PASSPHRASE
  ELB_HOST
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  CLIENT_NUMBER
)

for ENV_VAR in "${ENV_VARS[@]}"
do
  if [ -z "${!ENV_VAR}" ]; then
    echo ">>> ${ENV_VAR} is not configured; please export it."
    exit 1
  fi
done

echo ">>> Configuring Runner"
cd /tmp

# Memory of VM.
MEMORY=1024.0

# CPU of VM.
CPU=1

cat <<EOF > dcos-runner.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "dcos-runner",
  "dependencies": [],
  "constraints": [],
  "cpus": $CPU,
  "mem": $MEMORY,
  "instances": 1,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "vitorenesduarte/dcos-runner",
      "network": "HOST",
      "forcePullImage": true,
      "parameters": [
        { "key": "oom-kill-disable", "value": "true" }
      ]
    }
  },
  "ports": [0, 0],
  "env": {
    "DCOS": "$DCOS",
    "TOKEN": "$TOKEN",
    "EVALUATION_PASSPHRASE": "$EVALUATION_PASSPHRASE",
    "ELB_HOST": "$ELB_HOST",
    "AWS_ACCESS_KEY_ID": "$AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY": "$AWS_SECRET_ACCESS_KEY"
    "CLIENT_NUMBER": "$CLIENT_NUMBER"
  },
  "healthChecks": []
}
EOF

echo ">>> Adding dcos-runner to Marathon"
curl -s -k -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X POST -d @dcos-runner.json "$DCOS/service/marathon/v2/apps?force=true" > /dev/null
sleep 10

