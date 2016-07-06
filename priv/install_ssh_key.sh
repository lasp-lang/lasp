#!/bin/bash

echo "Starting agent."
eval `ssh-agent -s`

echo "Current SSH_AUTH_SOCK:"
echo $SSH_AUTH_SOCK

echo "Adding key to agent."
./add_key.sh

echo "Now SSH_AUTH_SOCK:"
echo $SSH_AUTH_SOCK

echo "After adding key, agent now contains the following"
ssh-add -l
