GO ?= go
CMAKE ?= cmake
LOCAL_BUILD_DIR ?= /tmp/zerokv-local-build
COVERAGE_FILE ?= /tmp/zerokv-core.cover

.PHONY: test test-race coverage bench daemon-build cpp-build cpp-test integration test-local

test:
	$(GO) test -count=1 ./...

test-race:
	$(GO) test -count=1 -race ./internal/storage ./internal/coordinator ./internal/transport ./internal/distributed

coverage:
	$(GO) test -count=1 -coverprofile=$(COVERAGE_FILE) ./internal/storage ./internal/coordinator ./internal/transport ./internal/distributed
	$(GO) tool cover -func=$(COVERAGE_FILE)

bench:
	$(GO) test -run '^$$' -bench 'Benchmark(HandlerLeaseAcquire|HandlerStreamRead|P2PTransportFetch)$$' -benchmem ./internal/storage ./internal/transport

cpp-build:
	$(CMAKE) -S csrc -B $(LOCAL_BUILD_DIR)/csrc
	$(CMAKE) --build $(LOCAL_BUILD_DIR)/csrc -j2

cpp-test: cpp-build
	ctest --test-dir $(LOCAL_BUILD_DIR)/csrc --output-on-failure

daemon-build:
	$(GO) build -o $(LOCAL_BUILD_DIR)/zerokv-daemon ./cmd

integration: daemon-build cpp-build
	ZEROKV_RUN_E2E=1 \
	ZEROKV_DAEMON_BIN=$(LOCAL_BUILD_DIR)/zerokv-daemon \
	ZEROKV_CLIENT_BIN=$(LOCAL_BUILD_DIR)/csrc/kvcache_client \
	$(GO) test -count=1 -run TestLocalTwoNodeCacheFill -v ./integration

test-local: test test-race cpp-test integration
