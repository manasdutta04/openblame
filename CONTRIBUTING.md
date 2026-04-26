# Contributing to OpenBlame

First off, thank you for considering contributing to OpenBlame! It's people like you who make the open-source community such an amazing place to learn, inspire, and create.

## 🚀 How Can I Contribute?

### Reporting Bugs
If you find a bug, please open an issue on GitHub. Include:
- Your OS and Python version.
- Steps to reproduce the bug.
- Expected vs. actual behavior.
- Any relevant logs (run with `DEBUG=1` if possible).

### Suggesting Enhancements
We love new ideas! If you have a feature request, please open an issue to discuss it before starting work.

### Pull Requests
1. **Fork the repo** and create your branch from `main`.
2. **Install dev dependencies**:
   ```bash
   pip install -e ".[dev]"
   ```
3. **Write tests** for your changes.
4. **Run tests** and linting:
   ```bash
   # On Windows
   .\test.ps1
   
   # On Linux/Mac
   make test
   ```
5. **Format your code**: We use `ruff` for formatting and linting.
6. **Issue a Pull Request**: Provide a clear description of your changes.

## 🛠 Development Workflow

- **Backend**: Python 3.11+
- **CLI**: Typer + Rich
- **Settings**: Pydantic-Settings
- **AI**: Ollama (local-first)
- **Metadata**: OpenMetadata REST API

### Coding Standards
- Follow PEP 8.
- Use type hints wherever possible.
- Keep components focused and modular.
- Ensure any new metadata tool fails gracefully (returns an error key instead of raising).

## 📝 License
By contributing to OpenBlame, you agree that your contributions will be licensed under the project's MIT License.
