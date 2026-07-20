GO_IMAGE ?= golang:1.26

.PHONY: test

test:
	docker run --rm \
		-v $(CURDIR):/app \
		-v go-mod-cache:/go/pkg/mod \
		-v go-build-cache:/root/.cache/go-build \
		-w /app \
		$(GO_IMAGE) \
		sh -c "go clean -testcache && go test -v -count=1 -race ./..."
