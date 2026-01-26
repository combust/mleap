FROM --platform=linux/amd64 ubuntu:22.04

RUN ln -fs /usr/share/zoneinfo/UTC /etc/localtime


# Install necessary packages and tools
RUN DEBIAN_FRONTEND=noninteractive  \
    apt-get update && apt-get install -y \
    curl \
    openjdk-11-jdk \
    unzip \
    zip \
    git \
    gpg \
    software-properties-common \
    python3-pip && \ 
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && apt-get install -y \
    python3.7  \
    python3.8  \
    python3.9  \
    python3.10  \
    python3.11  \
    python3.12  \
    python3.7-distutils \
    python3.8-distutils \
    python3.9-distutils \
    python3-distutils  \
    tox \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1
RUN pip install twine
# ARG USERNAME=mleap
# ARG USER_UID=1001
# ARG USER_GID=$USER_UID

# # Create the user
# RUN (groupadd --gid $USER_GID $USERNAME || true) \
#     && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME --shell /usr/bin/bash

# USER $USERNAME

# Install SDKMAN and Scala
SHELL ["/bin/bash", "-c"]

RUN curl -s "https://get.sdkman.io" | bash \
    && source "$HOME/.sdkman/bin/sdkman-init.sh" \
    && sdk install scala 2.12.18 \
    && sdk install sbt
ENV PATH=/root/.sdkman/candidates/scala/current/bin:/root/.sdkman/candidates/sbt/current/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
