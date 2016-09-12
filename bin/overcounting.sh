#!/bin/bash

source helpers.sh

ENV_VARS=(
  DCOS
  TOKEN
  EVALUATION_PASSPHRASE
  ELB_HOST
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

EVAL_NUMBER=1
MIN_POW=2
MAX_POW=6
MODE=delta_based
BROADCAST=false
SIMULATION=ad_counter_overcounting
PARTITION_PROBABILITY=0
AAE_INTERVAL=10000
DELTA_INTERVAL=10000
INSTRUMENTATION=true
LOGS="s3"
EXTENDED_LOGGING=true
MAILBOX_LOGGING=true

declare -A EVALUATIONS

## client_server_overcounting_test
EVALUATIONS["client_server_overcounting"]="partisan_client_server_peer_service_manager false"

## peer_to_peer_overcounting_test
EVALUATIONS["peer_to_peer_overcounting"]="partisan_hyparview_peer_service_manager false"

## code_peer_to_peer_overcounting_test
EVALUATIONS["code_peer_to_peer_overcounting"]="partisan_hyparview_peer_service_manager true"

for i in $(seq 1 $EVAL_NUMBER)
do
  echo "Running evaluation $i of $EVAL_NUMBER"

  for POW in $(seq $MIN_POW $MAX_POW)
  do
    CLIENT_NUMBER=$(echo '2^$POW' | bc)

    for EVAL_ID in "${!EVALUATIONS[@]}"
    do
      STR=${EVALUATIONS["$EVAL_ID"]}
      IFS=' ' read -a CONFIG <<< "$STR"
      PEER_SERVICE=${CONFIG[0]}
      HEAVY_CLIENTS=${CONFIG[1]}
      TIMESTAMP=$(date +%s)
      REAL_EVAL_ID=$EVAL_ID"_"$CLIENT_NUMBER

      PEER_SERVICE=$PEER_SERVICE MODE=$MODE BROADCAST=$BROADCAST SIMULATION=$SIMULATION EVAL_ID=$REAL_EVAL_ID EVAL_TIMESTAMP=$TIMESTAMP CLIENT_NUMBER=$CLIENT_NUMBER HEAVY_CLIENTS=$HEAVY_CLIENTS PARTITION_PROBABILITY=$PARTITION_PROBABILITY AAE_INTERVAL=$AAE_INTERVAL DELTA_INTERVAL=$DELTA_INTERVAL INSTRUMENTATION=$INSTRUMENTATION LOGS=$LOGS AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY EXTENDED_LOGGING=$EXTENDED_LOGGING MAILBOX_LOGGING=$MAILBOX_LOGGING ./dcos-deploy.sh

      echo "Running $EVAL_ID with $CLIENT_NUMBER clients"

      wait_for_completion $TIMESTAMP
    done
  done

  echo "Evaluation $i of $EVAL_NUMBER completed!"
done
