ARG BASE_IMAGE=rockylinux:9
FROM $BASE_IMAGE

# Get target architecture for multi-arch builds
ARG TARGETARCH

# Install minimal required packages + gcc for CGO (race detection)
RUN dnf install -y --allowerasing \
    gcc \
    glibc-devel \
    java-11-openjdk \
    python3 \
    curl \
    wget \
    jq \
    tar \
    gzip \
    && dnf clean all

# Set Java 11 as the default using environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Go versions 1.23, 1.24, and 1.25 (multi-arch aware)
ENV GO_VERSIONS="1.23.4 1.24.2 1.25.0"

# Download and install Go versions for the target architecture
RUN GOARCH=${TARGETARCH:-amd64} && \
    for version in $GO_VERSIONS; do \
        echo "Installing Go $version for ${GOARCH}..."; \
        wget -q https://golang.org/dl/go${version}.linux-${GOARCH}.tar.gz -O /tmp/go${version}.tar.gz; \
        mkdir -p /usr/local/go${version}; \
        tar -C /usr/local/go${version} --strip-components=1 -xzf /tmp/go${version}.tar.gz; \
        rm /tmp/go${version}.tar.gz; \
    done

# Create wrapper scripts for Go versions (simplified)
RUN for version in 1.23.4 1.24.2 1.25.0; do \
        short_version=$(echo $version | cut -d. -f1,2); \
        echo "#!/bin/bash" > /usr/local/bin/go${short_version}; \
        echo "export GOROOT=/usr/local/go${version}" >> /usr/local/bin/go${short_version}; \
        echo 'exec $GOROOT/bin/go "$@"' >> /usr/local/bin/go${short_version}; \
        chmod +x /usr/local/bin/go${short_version}; \
    done

# Ensure /usr/local/bin is in PATH (should be by default, but making sure)
ENV PATH="/usr/local/bin:${PATH}"

# This is to solve permission issue, read https://denibertovic.com/posts/handling-permissions-with-docker-volumes/
ARG GOSU_URL=https://github.com/tianon/gosu/releases/download/1.14/gosu-amd64
ENV GOSU_PATH=${GOSU_URL}
RUN GOARCH=${TARGETARCH:-amd64} && \
    GOSU_ARCH=$([ "$GOARCH" = "amd64" ] && echo "amd64" || echo "arm64") && \
    curl -o /usr/local/bin/gosu -SL https://github.com/tianon/gosu/releases/download/1.14/gosu-${GOSU_ARCH}
RUN chmod +x /usr/local/bin/gosu

# Copy entrypoint script
COPY scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh


WORKDIR /home/user
RUN chmod 777 /home/user

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
