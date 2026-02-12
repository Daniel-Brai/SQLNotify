# Contributing to SQLNotify

Thank you for your interest in contributing to SQLNotify. This document provides guidelines and instructions for contributions.

## Development Setup

### Prerequisites

- Python 3.10 or higher
- [uv](https://github.com/astral-sh/uv) package manager
- Docker and Docker Compose (for running tests with the supported databases)

### Setting Up Your Environment

1. **Fork and clone the repository:**

```bash
git clone https://github.com/Daniel-Brai/SQLNotify.git sqlnotify
cd sqlnotify
```

1. **Install dependencies**

```bash
# Using make
make install-dev

# Use uv
uv sync --all-groups --extra all
```

2. **Activate the virtual environment**

```bash
source .venv/bin/activate
```

3. **Set up pre-commit hooks**

I know you might be asking why. Just because it will save me time and effort and no I don't want to argue about it or run it in CI :)

```bash
uv run pre-commit install
```

## Running Tests

### Run all tests

```bash
# Using make (recommended - runs in Docker with all the supported databases)
make test

# Or directly with pytest
uv run pytest
```

### Run tests with coverage

```bash
# Using make
make test_cov

# Or directly with pytest
uv run pytest --cov=sqlnotify --cov-report=html
```

## Code Style

SQLNotify follows Python best practices and uses automated tools to maintain code quality:

- **Ruff** for linting and formatting
- **Isort** for import sorting
- **Black** for code formatting
- **Mypy** for type checking
- **Bandit** for security checks

Note when commit your changes pre-commit hooks run automatically.

## Project Structure

```text
sqlnotify/
├── src/sqlnotify/           # Main package
│   ├── notifiers/           # Notifier implementations
│   │   └── dialects/        # Database dialect implementations
│   │       ├── base.py      # Abstract base dialect
│   │       ├── postgresql.py # PostgreSQL dialect
│   │       └── sqlite.py    # SQLite dialect
│   ├── adapters/            # Framework adapters (ASGI, etc.)
│   ├── types.py             # Type definitions
│   └── watcher.py           # Watcher configuration
├── tests/                   # Test suite
│   ├── e2e/                 # End-to-end integration tests
│   └── test_*.py            # Unit tests
└── docs/                    # Documentation for specific features
```

## Documentation

When contributing, please update documentation as needed. This includes:

- **README.md** - For user-facing feature changes
- **docs/*.md** - For detailed documentation if necessary
- **Docstrings** - For all APIs it makes it easier for me to hover and just see what the function does without having to navigate to the function definition. This is especially important for public APIs and complex internal functions or methods.
- **Type hints** - For all function signatures

### Documentation Style

- Make sure your use clear and concise language
- Include code examples if applicable
- Also, document the parameters, return values, and exceptions of functions, methods or classes if applicable in your docstrings

### Before Submitting

1. Ensure all tests pass: `make test`
2. Update documentation if necessary
3. Add entries to relevant docs if needed
4. Write clear commit messages

### PR Process

1. **Create a feature branch:**

```bash
git checkout -b feature/my-new-feature
```

1. **Make your changes and commit:**

```bash
git add .
git commit -m "Add feature: description of feature"
```

1. **Push to your fork:**

```bash
git push origin feature/my-new-feature
```

1. **Open a Pull Request** on GitHub

2. **Address review feedback** if requested

### PR Title Format

Use clear, descriptive titles:

- `feat: Add support for MySQL dialect`
- `fix: Resolve memory leak in polling loop`
- `docs: Update SQLite installation instructions`
- `test: Add integration tests for overflow tables`
- `refactor: Simplify trigger SQL generation`

### PR Description

Follow the PR template and include:

- What changes were made and why
- How to test the changes
- Any breaking changes or migration notes
- Screenshots or examples if applicable

## Reporting Bugs

### Before Reporting

1. Check if the issue already exists
2. Verify it's not a configuration problem
3. Test with the latest version

### Bug Report Should Include

- Python version
- SQLNotify version
- Database and version (PostgreSQL 14, SQLite 3.40, etc.)
- Minimal code example reproducing the issue
- Expected vs actual behavior
- Full error traceback if applicable

## Feature Requests

We welcome feature requests

1. Please check if the feature has already been requested
2. Clearly describe the use case and problem it solves
3. Provide example(s) of how the feature would be used
4. Explain why existing functionality doesn't meet the need if necessary

## Code of Conduct

- Be respectful and considerate
- Communicate clearly and professionally
- Focus on constructive feedback

## Questions?

- Check existing issues and documentation

## License

By contributing to SQLNotify, you agree that your contributions will be licensed under the MIT License.
