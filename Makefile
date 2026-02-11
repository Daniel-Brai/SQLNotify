UV := uv
CODECOV_CLI := codecovcli

.PHONY: test build publish release coverage


build:
	@echo "Building the project..."
	@$(UV) build
	@echo "Build completed"


publish:
	@echo "Publishing to PyPI..."
	@$(UV) publish -t $(PYPI_TOKEN)
	@echo "Published successfully"


GIT_SHA := $(shell git rev-parse HEAD 2>/dev/null || :)

coverage:
	@echo "Running coverage (inside Docker)..."
	@docker compose run --remove-orphans sqlnotify bash -c "$(UV) run pytest --cov=sqlnotify --cov-report=xml && if [ -n '$(GIT_SHA)' ]; then $(CODECOV_CLI) upload-process -t $(CODECOV_TOKEN) -f coverage.xml -C $(GIT_SHA); else echo 'Skipping codecov upload: missing git commit sha'; fi"
	@echo "Coverage report generated at coverage.xml"


release: build
	@echo "Running release script..."
	@bash scripts/release.sh $(ARGS)


test:
	@echo "Running all tests for sqlnotify..."
	@docker compose run --remove-orphans sqlnotify bash -c "$(UV) run pytest"
	@echo "All tests completed"
