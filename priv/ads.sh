#!/bin/bash

echo "Removing old code."
rm -rf priv/evaluation

echo "Installing SSH key."
( cd priv ; ./install_ssh_key.sh )

GIT_SSH=ssh/wrapper
echo -n "Using the following wrapper: "
echo -n $GIT_SSH
echo

echo "Evaluating agent."
eval `ssh-agent -s`

echo "Authentication socket."
echo $SSH_AUTH_SOCK

echo "Cloning evaluation code."
( cd priv; SSH_AUTH_SOCK=$SSH_AUTH_SOCK GIT_SSH=$GIT_SSH git clone git@github.com:lasp-lang/evaluation.git )

echo "Terminating old instances."
pkill -9 beam.smp

echo "Running evaluation."
./rebar3 ct --readable=false --suite=test/lasp_advertisement_counter_SUITE

echo "Checking in results."
( cd priv/evaluation ; git add . )
( cd priv/evaluation ; git commit -m 'Adding evaluation data.' )
( cd priv/evaluation ; GIT_SSH=../$GIT_SSH git push -u origin)
