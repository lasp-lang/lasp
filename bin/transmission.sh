#!/usr/bin/env bash

source helpers.sh

ENV_VARS=(
  LASP_BRANCH
  DCOS
  ELB_HOST
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  CLIENT_NUMBER
  PARTITION_PROBABILITY
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
AAE_INTERVAL=5000
DELTA_INTERVAL=5000
INSTRUMENTATION=true
LOGS="s3"
EXTENDED_LOGGING=false
MAILBOX_LOGGING=false

declare -A EVALUATIONS

# peer service | mode | broadcast (boolean) | heavy_clients (boolean) | reactive_server (boolean)
#EVALUATIONS["peer_to_peer_state_based_with_aae_test"]="partisan_hyparview_peer_service_manager state_based false false false"
#EVALUATIONS["peer_to_peer_state_based_with_aae_and_tree_test"]="partisan_hyparview_peer_service_manager state_based true false false"
EVALUATIONS["peer_to_peer_delta_based_with_aae_test"]="partisan_hyparview_peer_service_manager delta_based false false false"
#EVALUATIONS["client_server_state_based_with_aae_test"]="partisan_client_server_peer_service_manager state_based false false false"

for i in $(seq 1 $EVAL_NUMBER)
do
  echo "[$(date +%T)] Running evaluation $i of $EVAL_NUMBER"

  for EVAL_ID in "${!EVALUATIONS[@]}"
  do
    STR=${EVALUATIONS["$EVAL_ID"]}
    IFS=' ' read -a CONFIG <<< "$STR"
    PEER_SERVICE=${CONFIG[0]}
    MODE=${CONFIG[1]}
    BROADCAST=${CONFIG[2]}
    HEAVY_CLIENTS=${CONFIG[3]}
    REACTIVE_SERVER=${CONFIG[4]}
    # unix timestamp + nanoseconds
    TIMESTAMP=$(date +%s)$(date +%N)
    REAL_EVAL_ID=$EVAL_ID"_"$CLIENT_NUMBER"_"$PARTITION_PROBABILITY

    PEER_SERVICE=$PEER_SERVICE MODE=$MODE BROADCAST=$BROADCAST SIMULATION=$SIMULATION EVAL_ID=$REAL_EVAL_ID EVAL_TIMESTAMP=$TIMESTAMP HEAVY_CLIENTS=$HEAVY_CLIENTS REACTIVE_SERVER=$REACTIVE_SERVER AAE_INTERVAL=$AAE_INTERVAL DELTA_INTERVAL=$DELTA_INTERVAL INSTRUMENTATION=$INSTRUMENTATION LOGS=$LOGS EXTENDED_LOGGING=$EXTENDED_LOGGING MAILBOX_LOGGING=$MAILBOX_LOGGING ./dcos-deploy.sh

    echo "[$(date +%T)] Running $EVAL_ID with $CLIENT_NUMBER clients; $PARTITION_PROBABILITY % partitions; with configuration $STR"

    wait_for_completion $TIMESTAMP
    # Marathon may reply that no app is running but the resources may still be unavailable
    # Wait 10 minutes
    # sleep 600
  done

  echo "[$(date +%T)] Evaluation $i of $EVAL_NUMBER completed!"
done
