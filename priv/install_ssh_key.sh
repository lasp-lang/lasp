#!/bin/bash

echo "Starting agent."
eval `ssh-agent -s`

echo "Removing existing key."
rm -f /tmp/evaluation_private_key

echo "Installing key in temporary directory."
echo $EVALUATION_PRIVATE_KEY > /tmp/evaluation_private_key

echo "Changing permissions."
chmod 400 /tmp/evaluation_private_key

echo "Adding key to agent."
./add_key.sh

echo "After adding key, agent now contains the following"
ssh-add -l
