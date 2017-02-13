#!/usr/bin/env bash

gcloud beta container clusters create lasp
gcloud beta container clusters get-credentials lasp
