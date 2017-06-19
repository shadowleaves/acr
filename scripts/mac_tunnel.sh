#!/bin/sh
ssh -NR $2:localhost:22 $1 -o ExitOnForwardFailure=yes
# this is already set in the mac private/etc/sshd_config:
# -oServerAliveInterval=100
