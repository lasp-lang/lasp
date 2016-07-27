#!/bin/bash

GIT_SSH=ssh/wrapper

cd priv/evaluation

echo "Configuring git."
git config user.email "christopher.meiklejohn+lasp-lang-evaluator@gmail.com"
git config user.name "Lasp Language Evaluation Bot"

echo "Checking in results."
git add .
git commit -m 'Adding evaluation data.'
GIT_SSH=../$GIT_SSH git push -u origin
