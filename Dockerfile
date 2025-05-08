# Use a base image with the required tools
FROM ubuntu:20.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    g++ \
    cmake \
    clangd \
    clang-format \
    clang-tidy \
    git \
    libboost-all-dev \
    googletest \
    libbenchmark-dev \
    libspdlog-dev \
    libfmt-dev \
    libprotobuf-dev \
    protobuf-compiler \
    python3 \
    python3-pip \
    && apt-get clean

# Install Python libraries
RUN pip3 install --no-cache-dir numpy pandas matplotlib

# Set the working directory
WORKDIR /app

# Copy the repository into the container
COPY . .
