FROM artifactory.int.snowflakecomputing.com/development-chainguard-virtual/snowflake.com/go:1.24.0-dev

USER root

RUN apk update && apk add python3 python3-dev jq aws-cli gosu py3-pip
RUN python3 -m ensurepip
RUN pip install -U snowflake-connector-python

# workspace
RUN mkdir -p /home/user && \
    chmod 777 /home/user
WORKDIR /mnt/host

# entry point
COPY scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
