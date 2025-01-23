FROM ubuntu:18.04

RUN  apt-get update \
  && apt-get install -y wget \
     gnupg2

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.8 \
    python3-pip \
    python3.8-dev \
    libpython3.8 \
    libpython3.8-dev \
    jq \
    locales \
    locales-all \
    python3-setuptools \
    g++ \
    git \
    python3-dev \
    npm \
    curl \
    groff \
    less \
    unzip \
    zip \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3.8 get-pip.py

# Install last version of NodeJS
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash -
RUN apt-get install -y nodejs

WORKDIR /src

ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8

COPY requirements.txt /src/requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt --proxy=${HTTP_PROXY}
RUN npm install elasticdump -g

COPY . /src
