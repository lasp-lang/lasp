#!/bin/bash

ENV_VARS=(
  DCOS
  TOKEN
  EVALUATION_PASSPHRASE
  ELB_HOST
)

for ENV_VAR in "${ENV_VARS[@]}"
do
  if [ -z "${!ENV_VAR}" ]; then
    echo ">>> ${ENV_VAR} is not configured; please export it."
    exit 1
  fi
done

PEER_SERVICE=partisan_client_server_peer_service_manager MODE=state_based BROADCAST=false SIMULATION=ad_counter EVAL_ID=client_server_state_based_with_aae EVAL_TIMESTAMP=12345678 CLIENT_NUMBER=3 HEAVY_CLIENTS=false PARTITION_PROBABILITY=0 AAE_INTERVAL=10000 DELTA_INTERVAL=10000 INSTRUMENTATION=true ./dcos-deploy.sh

DONE=0

## When pretty print json line count is 3, it means there are no apps running
## {
##    "apps": []
## }
while [ $DONE -ne 3 ]
do
  sleep 10
  DONE=$(curl -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X GET $DCOS/service/marathon/v2/apps | python -m json.tool | wc -l)
done

echo "SIMULATION FINISHED"
