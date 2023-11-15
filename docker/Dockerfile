FROM busybox
RUN mkdir /data
ARG TARGETARCH
COPY ./syndicate-server.$TARGETARCH /syndicate-server
EXPOSE 1
CMD ["/syndicate-server", "-c", "/data", "-p", "1"]
