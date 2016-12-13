#!/bin/sh

KEY_NAME=dcos
STACK_NAME=dcos-m4-4xlarge

echo "Launching Oregon."
aws cloudformation create-stack \
  --stack-name ${STACK_NAME} \
  --template-url https://s3-us-west-2.amazonaws.com/cf-templates-akuyolple9l-us-west-2/2016343Mok-m4.4xlargeqai5dkf1oduzm7ahch6ez5mi \
  --capabilities CAPABILITY_IAM \
  --parameters ParameterKey=KeyName,ParameterValue=${KEY_NAME} \
               ParameterKey=PublicSlaveInstanceCount,ParameterValue=70 \
               ParameterKey=OAuthEnabled,ParameterValue=false \
               ParameterKey=SlaveInstanceCount,ParameterValue=0

echo "Waiting for stack creation."
aws cloudformation wait stack-create-complete --stack-name ${STACK_NAME}

DCOS_URL=$(aws cloudformation describe-stacks --stack-name ${STACK_NAME} --query 'Stacks[0].Outputs[0].OutputValue' | sed -e s/\"//g)

echo "Configuring DCOS url."
dcos config set core.dcos_url "http://${DCOS_URL}"

echo "Installing marathon-lb."
yes | dcos package install marathon-lb

google-chrome ${DCOS_URL}
