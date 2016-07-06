#!/usr/bin/expect -f

spawn ssh-add /tmp/evaluation_private_key
expect "Enter passphrase for /tmp/evaluation_private_key:"
send "evaluation\n";
interact
