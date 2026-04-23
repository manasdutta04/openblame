.PHONY: test install lint

install:
	pip install -e ".[dev]"

test:
	PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/ -v -p pytest_asyncio.plugin

lint:
	ruff check openblame/ tests/
