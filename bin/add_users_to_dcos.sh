#!/bin/bash

ENV_VARS=(
  DCOS
  TOKEN
)

for ENV_VAR in "${ENV_VARS[@]}"
do
  if [ -z "${!ENV_VAR}" ]; then
    echo ">>> ${ENV_VAR} is not configured; please export it."
    exit 1
  fi
done

EMAILS=(
  "christopher.meiklejohn@gmail.com"
  "junghun.yoo@cs.ox.ac.uk"
  "vitorenesduarte@gmail.com"
)

CREATOR=$(git config user.email)

for EMAIL in "${EMAILS[@]}"
do
  if [ ! "$EMAIL" == "$CREATOR" ]; then
    curl -v -v -v -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X PUT -d '{"creator_uid":"$CREATOR","cluster_url":"$DCOS"}' $DCOS/acs/api/v1/users/$EMAIL
  fi
done


