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

EVAL_NUMBER=1
SIMULATION=ad_counter
CLIENT_NUMBER=3
HEAVY_CLIENTS=false
PARTITION_PROBABILITY=0
AAE_INTERVAL=10000
DELTA_INTERVAL=10000
INSTRUMENTATION=true

declare -A EVALUATIONS

## client_server_state_based_with_aae_test
EVALUATIONS["client_server_state_based_with_aae"]="partisan_client_server_peer_service_manager state_based false"

## client_server_state_based_with_aae_and_tree_test
EVALUATIONS["client_server_state_based_with_aae_and_tree"]="partisan_client_server_peer_service_manager state_based true"

## client_server_delta_based_with_aae_test
EVALUATIONS["client_server_delta_based_with_aae"]="partisan_client_server_peer_service_manager delta_based false"

## peer_to_peer_state_based_with_aae_test
EVALUATIONS["peer_to_peer_state_based_with_aae"]="partisan_hyparview_peer_service_manager state_based false"

## peer_to_peer_state_based_with_aae_and_tree_test
EVALUATIONS["peer_to_peer_state_based_with_aae_and_tree"]="partisan_hyparview_peer_service_manager state_based true"

## peer_to_peer_delta_based_with_aae_test
EVALUATIONS["peer_to_peer_delta_based_with_aae"]="partisan_hyparview_peer_service_manager delta_based false"

function wait_for_completion {
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
}

for i in $(seq 1 $EVAL_NUMBER)
do
  for EVAL_ID in "${!EVALUATIONS[@]}"
  do
    STR=${EVALUATIONS["$EVAL_ID"]}
    IFS=' ' read -a CONFIG <<< "$STR"
    PEER_SERVICE=${CONFIG[0]}
    MODE=${CONFIG[1]}
    BROADCAST=${CONFIG[2]}
    TIMESTAMP=$(date +%s)

    PEER_SERVICE=$PEER_SERVICE MODE=$MODE BROADCAST=$BROADCAST SIMULATION=$SIMULATION EVAL_ID=$EVAL_ID EVAL_TIMESTAMP=$TIMESTAMP CLIENT_NUMBER=$CLIENT_NUMBER HEAVY_CLIENTS=$HEAVY_CLIENTS PARTITION_PROBABILITY=$PARTITION_PROBABILITY AAE_INTERVAL=$AAE_INTERVAL DELTA_INTERVAL=$DELTA_INTERVAL INSTRUMENTATION=$INSTRUMENTATION ./dcos-deploy.sh

    wait_for_completion
  done
done
