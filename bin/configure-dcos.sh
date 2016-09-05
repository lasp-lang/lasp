#!/usr/bin/env bash

echo "Configuring DCOS URL."
export DCOS=`dcos config show core.dcos_url`

echo "Generating token."
dcos auth login

echo "Configuring DCOS token."
export TOKEN=`dcos config show core.dcos_acs_token`
