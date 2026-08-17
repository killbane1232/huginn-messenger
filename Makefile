GO ?= go
VERSION := $(shell tr -d '[:space:]' < VERSION)
GOARCH := $(shell $(GO) env GOARCH)
DIST_DIR ?= dist
LIBRARY_DIR := $(DIST_DIR)/linux_$(GOARCH)
LIBRARY_ARCHIVE := $(DIST_DIR)/huginn-messenger_$(VERSION)_linux_$(GOARCH).tar.gz

.PHONY: all library package-library test clean

all: library

library:
	mkdir -p $(LIBRARY_DIR)
	$(GO) build -ldflags='-checklinkname=0' -buildmode=c-shared \
		-o $(LIBRARY_DIR)/libhuginn_messenger.so .

package-library: library
	tar -C $(LIBRARY_DIR) -czf $(LIBRARY_ARCHIVE) \
		libhuginn_messenger.so libhuginn_messenger.h
	cd $(DIST_DIR) && sha256sum "$(notdir $(LIBRARY_ARCHIVE))" > SHA256SUMS

test:
	$(GO) test ./...

clean:
	rm -rf $(DIST_DIR)
