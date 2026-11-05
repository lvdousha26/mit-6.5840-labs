.PHONY: all test race vet check fmt clean

GO ?= go
PKGS = ./...

all: test

test:
	$(GO) vet $(PKGS)
	$(GO) test $(PKGS) -count=1

race:
	$(GO) test -race $(PKGS) -count=1

vet:
	$(GO) vet $(PKGS)

fmt:
	$(GO) fmt $(PKGS)

# The same gate CI runs.
check: race

clean:
	$(GO) clean -testcache
	rm -f mr-* 2>/dev/null || true
