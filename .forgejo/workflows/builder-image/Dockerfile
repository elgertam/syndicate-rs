FROM rust:latest
RUN cargo install cross
RUN cargo install cargo-deb

# This is necessary for cross to be able to access /var/run/docker.sock
COPY --from=docker:dind /usr/local/bin/docker /usr/local/bin/

RUN curl -fsSL https://deb.nodesource.com/setup_20.x -o nodesource_setup.sh && bash nodesource_setup.sh && rm -f nodesource_setup.sh && apt -y install nodejs && apt clean
