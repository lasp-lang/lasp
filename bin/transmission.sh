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
declare -A CONFIG

## client_server_state_based_with_aae_test
CONFIG=(
  ["peer_service"]="partisan_client_server_peer_service_manager"
  ["mode"]="state_based"
  ["broadcast"]="false"
)
EVALUATIONS["client_server_state_based_with_aae"]=$CONFIG

## client_server_state_based_with_aae_and_tree_test
CONFIG=(
  ["peer_service"]="partisan_client_server_peer_service_manager"
  ["mode"]="state_based"
  ["broadcast"]="true"
)
EVALUATIONS["client_server_state_based_with_aae_and_tree"]=$CONFIG

## client_server_delta_based_with_aae_test
CONFIG=(
  ["peer_service"]="partisan_client_server_peer_service_manager"
  ["mode"]="delta_based"
  ["broadcast"]="false"
)
EVALUATIONS["client_server_delta_based_with_aae"]=$CONFIG

## peer_to_peer_state_based_with_aae_test
CONFIG=(
  ["peer_service"]="partisan_hyparview_peer_service_manager"
  ["mode"]="state_based"
  ["broadcast"]="false"
)
EVALUATIONS["peer_to_peer_state_based_with_aae"]=$CONFIG

## peer_to_peer_state_based_with_aae_and_tree_test
CONFIG=(
  ["peer_service"]="partisan_hyparview_peer_service_manager"
  ["mode"]="state_based"
  ["broadcast"]="true"
)
EVALUATIONS["peer_to_peer_state_based_with_aae_and_tree"]=$CONFIG

## peer_to_peer_delta_based_with_aae_test
CONFIG=(
  ["peer_service"]="partisan_hyparview_peer_service_manager"
  ["mode"]="delta_based"
  ["broadcast"]="false"
)
EVALUATIONS["peer_to_peer_delta_based_with_aae"]=$CONFIG

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
    CONFIG=${EVALUATIONS["$EVAL_ID"]}
    PEER_SERVICE=${CONFIG["peer_service"]}
    MODE=${CONFIG["mode"]}
    BROADCAST=${CONFIG["broadcast"]}
    TIMESTAMP=$(date +%s)

    PEER_SERVICE=$PEER_SERVICE MODE=$MODE BROADCAST=$BROADCAST SIMULATION=$SIMULATION EVAL_ID=$EVAL_ID EVAL_TIMESTAMP=$TIMESTAMP CLIENT_NUMBER=$CLIENT_NUMBER HEAVY_CLIENTS=$HEAVY_CLIENTS PARTITION_PROBABILITY=$PARTITION_PROBABILITY AAE_INTERVAL=$AAE_INTERVAL DELTA_INTERVAL=$DELTA_INTERVAL INSTRUMENTATION=$INSTRUMENTATION ./dcos-deploy.sh

    wait_for_completion
  done
done
