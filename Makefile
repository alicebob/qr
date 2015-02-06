.PHONY: short test install bench

short:
	go test -short
	go vet .
	golint .

test:
	go test

install: test
	go install

bench:
	go test -short -bench .
