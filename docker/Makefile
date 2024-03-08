U=leastfixedpoint
I=syndicate-server
ARCHITECTURES:=amd64 arm arm64
SERVERS:=$(patsubst %,syndicate-server.%,$(ARCHITECTURES))
VERSION=$(shell ./syndicate-server.$(shell ./docker-architecture $$(uname -m)) --version | cut -d' ' -f2)

all:

.PHONY: all clean image push push-only

clean:
	rm -f syndicate-server.*
	-podman images -q $(U)/$(I) | sort -u | xargs podman rmi -f

image: $(SERVERS)
	for A in $(ARCHITECTURES); do set -x; \
		podman build --platform=linux/$$A \
			-t $(U)/$(I):$(VERSION)-$$A \
			-t $(U)/$(I):latest-$$A \
		.; \
	done
	rm -f tmp.image

push: image push-only

push-only:
	$(patsubst %,podman push $(U)/$(I):$(VERSION)-%;,$(ARCHITECTURES))
	$(patsubst %,podman push $(U)/$(I):latest-%;,$(ARCHITECTURES))
	podman rmi -f $(U)/$(I):$(VERSION) $(U)/$(I):latest
	podman manifest create $(U)/$(I):$(VERSION) $(patsubst %,$(U)/$(I):$(VERSION)-%,$(ARCHITECTURES))
	podman manifest create $(U)/$(I):latest $(patsubst %,$(U)/$(I):latest-%,$(ARCHITECTURES))
	podman manifest push $(U)/$(I):$(VERSION)
	podman manifest push $(U)/$(I):latest

syndicate-server.%:
	make -C .. $$(./alpine-architecture $*)-binary-release
	cp -a ../target/target.$$(./alpine-architecture $*)/$$(./alpine-architecture $*)-unknown-linux-musl*/release/syndicate-server $@
