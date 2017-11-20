.PHONY: test deps

all: test

deps:
	@go list github.com/mjibson/esc || go get github.com/mjibson/esc/...
	@go list golang.org/x/tools/cmd/goimports || go get golang.org/x/tools/cmd/goimports
	go generate -x
	go get .

clean:
	-rm -rf bin

fmt:
	goimports -w .
	go vet .

test: fmt deps
	go test ./...

# build: fmt
# 	go build -o bin/`basename ${PWD}` cli/*.go
