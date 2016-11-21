#!/bin/bash

function wait_for_completion {
  LAST_TIMESTAMP=$1
  DONE=0

  while [ ! "$DONE" == ""  ]
  do
    sleep 10
    DONE=$(curl -s -H 'Content-type: application/json' -X GET $DCOS/service/marathon/v2/apps | python -m json.tool | grep -E "lasp-client-$LAST_TIMESTAMP|lasp-server-$LAST_TIMESTAMP")
  done
}
