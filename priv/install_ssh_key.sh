#!/bin/bash

echo "Starting agent."
eval `ssh-agent -s`

echo "Removing existing key."
rm -f /tmp/evaluation_private_key

echo "Installing key in temporary directory."
echo $EVALUATION_PRIVATE_KEY > /tmp/evaluation_private_key.base64

echo "Decoding."
openssl base64 -d -in /tmp/evaluation_private_key.base64 -out /tmp/evaluation_private_key

echo "Printing key."
cat /tmp/evaluation_private_key

echo "Changing permissions."
chmod 400 /tmp/evaluation_private_key

echo "Current SSH_AUTH_SOCK:"
echo $SSH_AUTH_SOCK

echo "Adding key to agent."
./add_key.sh

echo "Now SSH_AUTH_SOCK:"
echo $SSH_AUTH_SOCK

echo "After adding key, agent now contains the following"
ssh-add -l
