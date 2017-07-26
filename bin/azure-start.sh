#!/usr/bin/env bash

## Configure the environment.
CLUSTER_NAME=lasp
RESOURCE_GROUP=lasp

## Create the resource group.
az group create --name ${RESOURCE_GROUP} --location eastus

## Create the Kubernetes cluster.
az acs create --orchestrator-type=kubernetes --resource-group ${RESOURCE_GROUP} --name=${CLUSTER_NAME} --generate-ssh-keys

## Get credentials.
az acs kubernetes get-credentials --resource-group=${RESOURCE_GROUP} --name=${CLUSTER_NAME}
