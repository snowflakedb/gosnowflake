#!/bin/bash -ex
# Add local user
# Either use the LOCAL_USER_ID if passed in at runtime or
# fallback

USER_ID=${LOCAL_USER_ID:-9001}

echo "Starting with UID : $USER_ID"
adduser -s /bin/bash -u $USER_ID -h /home/user -D user
export HOME=/home/user
mkdir -p /home/user/.cache
chown user:user /home/user/.cache

exec gosu user "$@"

