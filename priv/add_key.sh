#!/usr/bin/expect -f

spawn ssh-add "$env(PWD)/ssh/evaluation"
expect "Enter passphrase for $env(PWD)/ssh/evaluation:"
send "$env(EVALUATION_PASSPHRASE)\n";
interact
