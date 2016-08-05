#!/bin/bash

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

