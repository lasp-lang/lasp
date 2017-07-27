#!/usr/bin/env bash

## Configure environment.
RESOURCE_GROUP=lasp

## Terminate resource group.
az group delete --name ${RESOURCE_GROUP} --yes --no-wait
