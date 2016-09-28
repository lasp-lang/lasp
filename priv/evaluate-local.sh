#!/bin/bash

./evaluate-mesos-setup.sh

echo "Terminating old instances."
pkill -9 beam.smp

echo "Running peer-to-peer ads evaluation."
./rebar3 ct --readable=false --suite=test/lasp_peer_to_peer_advertisement_counter_SUITE

echo "Running client-server ads evaluation."
./rebar3 ct --readable=false --suite=test/lasp_client_server_advertisement_counter_SUITE

echo "Generating plots."
( cd priv/evaluation ; make plots )

./evaluate-mesos-push.sh

