all:

clean:
	rm -f syndicate-server
	docker rmi syndicate-server

image: syndicate-server
	rm -rf emptydir
	mkdir emptydir
	docker build -t leastfixedpoint/$$(./syndicate-server --version | tr ' ' ':') -t leastfixedpoint/syndicate-server:latest .

push: image
	docker push leastfixedpoint/$$(./syndicate-server --version | tr ' ' ':')
	docker push leastfixedpoint/syndicate-server:latest

syndicate-server: ../target/x86_64-unknown-linux-musl/release/syndicate-server
	cp -a $< $@
