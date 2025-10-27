#!/bin/bash

# Add local user
# Either use the LOCAL_USER_ID if passed in at runtime or
# fallback

USER_ID=${LOCAL_USER_ID:-9001}

echo "Starting with UID : $USER_ID"

# Create user (may already exist from Docker build)
useradd --shell /bin/bash -u $USER_ID -o -c "" -m user 2>/dev/null || true
export HOME=/home/user

# Set up user directories and permissions
echo "Setting up user permissions and directories..."
# Create all needed directories first
mkdir -p /home/user/go/{bin,src,pkg} 2>/dev/null || true
mkdir -p /home/user/.cache/go-build 2>/dev/null || true

# Only change ownership of directories we created, not the mounted workspace
chown user:user /home/user 2>/dev/null || true
chown -R user:user /home/user/go 2>/dev/null || true  
chown -R user:user /home/user/.cache 2>/dev/null || true

# Create .bashrc if it doesn't exist and set ownership
touch /home/user/.bashrc 2>/dev/null || true
chown user:user /home/user/.bashrc 2>/dev/null || true

# Set permissions on user's own directories (not mounted ones)
chmod 755 /home/user 2>/dev/null || true
chmod -R 755 /home/user/go 2>/dev/null || true
chmod -R 755 /home/user/.cache 2>/dev/null || true

/usr/local/bin/gosu user "$@"
