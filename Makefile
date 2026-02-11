UV := uv
CODECOV_CLI := codecovcli

.PHONY: test build publish release


build:
	@echo "Building the project..."
	@$(UV) build
	@echo "Build completed"


publish:
	@echo "Publishing to PyPI..."
	@$(UV) publish -t $(PYPI_TOKEN)
	@echo "Published successfully"


release: build
	@echo "Running release script..."
	@bash scripts/release.sh $(ARGS)


test:
	@echo "Running all tests for sqlnotify..."
	@docker compose run --remove-orphans sqlnotify bash -c "$(UV) run pytest"
	@echo "All tests completed"
