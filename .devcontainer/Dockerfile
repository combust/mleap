FROM --platform=linux/amd64 ubuntu:22.04

ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install necessary packages and tools
RUN DEBIAN_FRONTEND=noninteractive  \
    apt-get update && apt-get install -y \
    curl \
    openjdk-11-jdk \
    unzip \
    zip \
    git \
    software-properties-common \
    python3-pip  && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && apt-get install -y \
    python3.7 \
    python3.8 \
    python3.10  \
    tox \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1

# Install SDKMAN and Scala
SHELL ["/bin/bash", "-c"]

RUN curl -s "https://get.sdkman.io" | bash \
    && source "$HOME/.sdkman/bin/sdkman-init.sh" \
    && sdk install scala 2.12.18 \
    && sdk install sbt

# Set up your environment as needed
