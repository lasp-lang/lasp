#!/bin/sh

echo "Launching Oregon."
aws cloudformation create-stack \
  --stack-name dcos \
  --template-body https://s3-us-west-2.amazonaws.com/downloads.dcos.io/dcos/stable/commit/e64024af95b62c632c90b9063ed06296fcf38ea5/cloudformation/multi-master.cloudformation.json \
  --capabilities CAPABILITY_IAM \
  --parameters ParameterKey=KeyName,ParameterValue=dcos \
               ParameterKey=PublicSlaveInstanceCount,ParameterValue=40 \
               ParameterKey=OAuthEnabled,ParameterValue=false \
               ParameterKey=SlaveInstanceCount,ParameterValue=0

echo "Waiting for stack creation."
aws cloudformation wait stack-create-complete --stack-name dcos

DCOS_URL=$(aws cloudformation describe-stacks --stack-name dcos --query 'Stacks[0].Outputs[0].OutputValue' | sed -e s/\"//g)

echo "Configuring DCOS url."
dcos config set core.dcos_url "http://${DCOS_URL}"

echo "Installing marathon-lb."
yes | dcos package install marathon-lb

# Determine platform.
platform='unknown'
unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
  platform='linux'
else
  platform='darwin'
fi

if [[ $platform == 'linux' ]]; then
  google-chrome ${DCOS_URL}
else
  open "http://${DCOS_URL}"
fi
