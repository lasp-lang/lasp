#!/usr/bin/expect -f

spawn ssh-add "/tmp/evaluation"
expect "Enter passphrase for /tmp/evaluation:"
send_user "$env(EVALUATION_PASSPHRASE)\r\n";
send "$env(EVALUATION_PASSPHRASE)\r\n";
expect eof
exit
