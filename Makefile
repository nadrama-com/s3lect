.PHONY: help fmt lint test tag

help:
	@echo "Available targets:"
	@echo "  fmt   - Format Go code"
	@echo "  lint  - Run golangci-lint"
	@echo "  test  - Run tests"
	@echo "  tag   - Tag a new release (usage: make tag VERSION=v1.0.0)"

fmt:
	go fmt ./...

lint:
	golangci-lint run

test:
	go test -v -race -coverprofile=coverage.out ./...

tag:
	@if [ -z "$(VERSION)" ]; then \
		echo "ERROR: VERSION is required. Usage: make tag VERSION=v1.0.0"; \
		exit 1; \
	fi
	git tag -a $(VERSION) -m "$(VERSION)"
	@echo "To push tag run:"
	@echo "  git push origin $(VERSION)"
