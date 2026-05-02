GO ?= go
BIN_DIR ?= plugins
PLUGINS := \
	nyanyabot-plugin-xbot-gn

.PHONY: build test fmt clean tidy \
	build-xbot-gn

build: $(addprefix $(BIN_DIR)/,$(PLUGINS))

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(BIN_DIR)/%: | $(BIN_DIR)
	$(GO) build -o $@ ./cmd/$*

build-xbot-gn: $(BIN_DIR)/nyanyabot-plugin-xbot-gn

test:
	$(GO) test ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BIN_DIR)
