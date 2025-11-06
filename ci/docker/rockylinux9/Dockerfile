ARG BASE_IMAGE=rockylinux:9
FROM $BASE_IMAGE

ARG TARGETARCH

# Update all packages first (including glibc) to get latest versions
RUN dnf update -y && dnf clean all

# Install glibc-devel - it should match the updated glibc version
# If there's still a mismatch, try installing an older compatible version
RUN dnf install -y --allowerasing --nobest glibc-devel || \
    (echo "Direct install failed, checking available versions..." && \
     dnf list available glibc-devel | head -5 && \
     CURRENT_GLIBC=$(rpm -q glibc --qf '%{VERSION}-%{RELEASE}\n') && \
     echo "Current glibc: $CURRENT_GLIBC" && \
     dnf install -y --allowerasing --nobest glibc-devel || true) && \
    dnf clean all

# Install minimal required packages + gcc for CGO (race detection)
RUN dnf install -y --allowerasing --nobest \
    gcc \
    java-11-openjdk \
    python3 \
    curl \
    wget \
    jq \
    tar \
    gzip \
    procps-ng \
    && dnf clean all

# Set Java 11 as the default using environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Accept full Go version as build argument (e.g., GO_VERSION=1.24.2)
ARG GO_VERSION

# Download and install Go version
RUN GOARCH=${TARGETARCH} && \
    GO_VERSION_SHORT=$(echo ${GO_VERSION} | cut -d. -f1,2) && \
    echo "Installing Go ${GO_VERSION} for ${GOARCH}..." && \
    wget -q https://golang.org/dl/go${GO_VERSION}.linux-${GOARCH}.tar.gz -O /tmp/go.tar.gz && \
    mkdir -p /usr/local/go${GO_VERSION_SHORT} && \
    tar -C /usr/local/go${GO_VERSION_SHORT} --strip-components=1 -xzf /tmp/go.tar.gz && \
    rm /tmp/go.tar.gz && \
    # Create wrapper script for short version (e.g., go1.24) \
    echo "#!/bin/bash" > /usr/local/bin/go${GO_VERSION_SHORT} && \
    echo "export GOROOT=/usr/local/go${GO_VERSION_SHORT}" >> /usr/local/bin/go${GO_VERSION_SHORT} && \
    echo 'exec $GOROOT/bin/go "$@"' >> /usr/local/bin/go${GO_VERSION_SHORT} && \
    chmod +x /usr/local/bin/go${GO_VERSION_SHORT}

# Ensure /usr/local/bin is in PATH (should be by default, but making sure)
ENV PATH="/usr/local/bin:${PATH}"

# Accept user ID as build argument to match host permissions
ARG USER_ID=1001
ARG GROUP_ID=1001

# Create user for proper permission testing
# Always create "user" user - use requested IDs if available, otherwise auto-assign
RUN if ! getent group user >/dev/null 2>&1; then \
        (groupadd -g ${GROUP_ID} user 2>/dev/null || groupadd user); \
    fi && \
    if ! getent passwd user >/dev/null 2>&1; then \
        (useradd -u ${USER_ID} -g user -m -s /bin/bash user 2>/dev/null || useradd -g user -m -s /bin/bash user); \
    fi && \
    mkdir -p /home/user/go && \
    chown -R user:user /home/user

USER user
WORKDIR /home/user/gosnowflake
