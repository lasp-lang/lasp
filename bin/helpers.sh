#!/bin/bash

function wait_for_completion {
  DONE=0

  while [ ! "$DONE" == ""  ]
  do
    sleep 10
    DONE=$(curl -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X GET $DCOS/service/marathon/v2/apps | python -m json.tool | grep -E "lasp-client|lasp-server")
  done
}

