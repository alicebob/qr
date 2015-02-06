.PHONY: all test install

all: test

test:
	go test

install:
	go install
