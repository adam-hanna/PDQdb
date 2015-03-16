##########
default: build
##########

build: deps lint test
	gox -os="darwin linux" -output=".bin/{{.OS}}-{{.Arch}}/PDQdb" -verbose
	# gox -output=".bin/{{.OS}}-{{.Arch}}/PDQdb" -verbose

clean:
	rm -fr ./.bin

deps:
	godep go install -v -x ./...

errcheck:
	# errcheck github.com/adam-hanna/PDQdb

fmt:
	gofmt -e -s -w .

lint: fmt errcheck

test: lint
	# TODO(@jonathanmarvens): Add some damn tests to fix this.

.PHONY: default
.PHONY: build clean deps errcheck fmt lint test
