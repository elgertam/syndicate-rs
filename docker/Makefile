all:

clean:
	rm -f syndicate-server
	docker rmi syndicate-server

image: syndicate-server
	rm -rf emptydir
	mkdir emptydir
	docker build -t $$(./syndicate-server --version | tr ' ' ':') -t syndicate-server:latest .

syndicate-server: ../target/x86_64-unknown-linux-musl/release/syndicate-server
	cp -a $< $@
