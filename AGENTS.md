# Repository Guidelines

## Project Structure & Module Organization
Keep runtime code in `src/` with package names that match the feature domain (for example, `src/conversation/agent.py`). Shared prompt templates belong in `src/prompts/`, while integration clients sit under `src/services/`. Place CLI helpers or one-off maintenance utilities in `scripts/`. Test data and reusable fixtures live in `tests/fixtures/`, and diagrams or proposal docs go to `docs/`.

## Build, Test, and Development Commands
Target Python 3.11+. Create an isolated environment via `python -m venv .venv && source .venv/bin/activate`. Install runtime dependencies with `pip install -r requirements.txt` and development tooling using `pip install -r requirements-dev.txt`. Run the main workflow locally with `python -m src.cli` once your entry point exists. Execute `pytest` for the automated suite, `pytest --maxfail=1 --ff` for focused debugging, `ruff check src tests` to lint, and `mypy src` to keep type safety intact.

## Coding Style & Naming Conventions
Adopt PEP 8 with 4-space indentation. Modules should use descriptive snake_case names such as `agent_registry.py`. Classes follow PascalCase, functions and variables remain snake_case, and constants are UPPER_SNAKE_CASE. Keep public APIs small; mark module-private helpers with a leading underscore. Format code with `ruff format` before committing and keep docstrings concise and imperative.

## Testing Guidelines
Write tests with Pytest and mirror the directory layout (`tests/conversation/test_agent.py`). Name tests after observable behaviour (`test_agent_handles_interruption`). Prefer fixtures for setup and factory functions for payload construction. Strive for 85%+ statement coverage and add regression tests for every bug fix. Run `pytest --cov=src --cov-report=term-missing` before opening a pull request.

## Commit & Pull Request Guidelines
Group commits by logical concerns and follow Conventional Commits (`feat:`, `fix:`, `chore:`). Keep subject lines under 72 characters and wrap bodies at 80. Reference issues in the footer (`Refs #123`). Pull requests must explain the motivation, outline the solution, link related issues, and include screenshots or CLI captures when behaviour changes. Confirm CI status and checklists before requesting review.
