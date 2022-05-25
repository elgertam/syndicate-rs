all:

.PHONY: all clean image push syndicate-server

clean:
	rm -f syndicate-server
	docker rmi leastfixedpoint/syndicate-server

image: syndicate-server
	docker build -t leastfixedpoint/$$(./syndicate-server --version | tr ' ' ':') -t leastfixedpoint/syndicate-server:latest .

push: image
	docker push leastfixedpoint/$$(./syndicate-server --version | tr ' ' ':')
	docker push leastfixedpoint/syndicate-server:latest

syndicate-server:
	make -C .. x86_64-binary-release
	cp -a ../target/x86_64-unknown-linux-musl/release/syndicate-server $@
