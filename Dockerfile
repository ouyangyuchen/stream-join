# Use a base image with the required tools
FROM ubuntu:20.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    zsh \
    curl \
    vim \
    gdb \
    build-essential \
    g++ \
    cmake \
    clangd \
    clang-format \
    clang-tidy \
    git \
    libboost-all-dev \
    libspdlog-dev \
    libfmt-dev \
    python3 \
    python3-pip \
    && apt-get clean

# Install Python libraries
RUN pip3 install --no-cache-dir numpy pandas matplotlib

# Install oh-my-zsh, and set zsh as the default shell
RUN chsh -s $(which zsh) \
    && sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended
# Install additional zsh plugins
RUN git clone https://github.com/zsh-users/zsh-autosuggestions.git $ZSH_CUSTOM/plugins/zsh-autosuggestions
RUN git clone https://github.com/zsh-users/zsh-syntax-highlighting.git $ZSH_CUSTOM/plugins/zsh-syntax-highlighting

# Set the working directory
WORKDIR /app

# Copy the repository into the container
COPY . .

# Set the entry point for the container
ENTRYPOINT ["/bin/zsh"]
# Add a CMD to keep zsh running in detached mode
CMD ["-c", "tail -f /dev/null"]
