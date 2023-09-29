FROM busybox
RUN mkdir /data
ARG TARGETARCH
COPY ./syndicate-server.$TARGETARCH /syndicate-server
CMD ["/syndicate-server", "-c", "/data", "-p", "1"]
