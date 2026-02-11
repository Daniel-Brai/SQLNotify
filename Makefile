UV := uv

.PHONY: test test_unit test_sqlite test_postgres test_coverage test_all build publish release


build:
	@echo "Building the project..."
	@$(UV) build
	@echo "Build completed"


publish:
	@echo "Publishing to PyPI..."
	@$(UV) publish -t $(token)
	@echo "Published successfully"


release: build
	@echo "Running release script..."
	@bash scripts/release.sh $(ARGS)


test_all: test test_coverage
	@echo "All tests and coverage completed"

test_coverage:
	@echo "Running tests coverage for sqlnotify..."
	@docker compose run --remove-orphans sqlnotify bash -c "$(UV) run pytest --cov=sqlnotify --cov-report=xml"
	@echo "Test coverage report generated at coverage.xml"


test: test_unit test_postgres test_sqlite
	@echo "All tests completed"


test_unit:
	@echo "Running all unit tests for sqlnotify..."
	@docker compose run --remove-orphans sqlnotify bash -c "$(UV) run pytest tests/test_watcher.py"
	@echo "All Unit tests completed"


test_sqlite:
	@echo "Running tests for sqlnotify sqlite..."
	@docker compose run --remove-orphans sqlnotify bash -c "$(UV) run pytest tests/test_sqlite_*.py --db=sqlite"
	@echo "All SQLite tests completed"


test_postgres:
	@echo "Running tests for sqlnotify postgres..."
	@docker compose run --remove-orphans sqlnotify bash -c "$(UV) run pytest tests/test_postgres_*.py --db=postgresql"
	@echo "All PostgreSQL tests completed"
