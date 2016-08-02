#!/bin/bash

GIT_SSH=ssh/wrapper

echo "Configuring git."
git config --global user.email "christopher.meiklejohn+lasp-lang-evaluator@gmail.com"
git config --global user.name "Lasp Language Evaluation Bot"

echo "Checking in results."
( cd priv/evaluation ; git add . )
( cd priv/evaluation ; git commit -m 'Adding evaluation data.' )
( cd priv/evaluation ; GIT_SSH=../$GIT_SSH git push -u origin)

