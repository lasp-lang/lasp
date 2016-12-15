#!/bin/sh

KEY_NAME=dcos
STACK_NAME=dcos-m3-2xlarge

echo "Launching Oregon."
aws cloudformation create-stack \
  --stack-name ${STACK_NAME} \
  --template-url https://s3-us-west-2.amazonaws.com/cf-templates-akuyolple9l-us-west-2/2016256mQk-template-m3.2xlarge840l39dhog1rubdph6j2it3xr \
  --capabilities CAPABILITY_IAM \
  --parameters ParameterKey=KeyName,ParameterValue=${KEY_NAME} \
               ParameterKey=PublicSlaveInstanceCount,ParameterValue=50 \
               ParameterKey=OAuthEnabled,ParameterValue=false \
               ParameterKey=SlaveInstanceCount,ParameterValue=0

echo "Waiting for stack creation."
aws cloudformation wait stack-create-complete --stack-name ${STACK_NAME}

DCOS_URL=$(aws cloudformation describe-stacks --stack-name ${STACK_NAME} --query 'Stacks[0].Outputs[0].OutputValue' | sed -e s/\"//g)

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
