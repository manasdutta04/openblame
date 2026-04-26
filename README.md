# OpenBlame

```text
                         _     _                      
                        | |   | |                     
   ___  _ __   ___ _ __ | |__ | | __ _ _ __ ___   ___ 
  / _ \| '_ \ / _ \ '_ \| '_ \| |/ _` | '_ ` _ \ / _ \
 | (_) | |_) |  __/ | | | |_) | | (_| | | | | | |  __/
  \___/| .__/ \___|_| |_|_.__/|_|\__,_|_| |_| |_|\___|
       | |                                            
       |_|                                            
```

**OpenBlame** is a local-first AI investigation CLI for data pipelines running on **OpenMetadata**. Point it to a table, and it will trace lineage, inspect recent quality failures, parse schema-changes, surface governance gaps, and draft a root-cause narrative.

The reasoning layer runs entirely on your local **Ollama** models, ensuring your metadata stays inside your environment.

---

## Features

- **Autonomous Investigation**: ReAct-style agent loop that plans and executes metadata research.
- **Lineage & Blast Radius**: Automatic upstream/downstream analysis to find affected entities.
- **Schema Drift**: Instant diffing of column changes, type updates, and renames.
- **Governance Aware**: Surfaces missing owners, tiers, and domain gaps as operational risks.
- **Local-First AI**: Powered by Ollama (qwen2.5, llama3, etc.). No external API keys needed for reasoning.
- **MCP Ready**: Built-in Model Context Protocol server to integrate with AI IDEs like Cursor and Claude Desktop.

---

## Quick Start

### 1. Install
```bash
pip install openblame
```

### 2. Configure (CLI-only)
No need to manually edit files. Just run the interactive setup:
```bash
openblame configure
```
*It will help you set your OpenMetadata Host, Token, and even auto-detect your installed Ollama models.*

### 3. Investigate
```bash
openblame investigate "ecommerce.analytics.customer_churn"
```

---

## CLI Commands

### `investigate`
The core command. Runs the full AI reasoning loop.
```bash
openblame investigate <table_fqn> [OPTIONS]

Options:
  --depth INTEGER     Lineage depth (default: 3)
  --days INTEGER      Lookback days for changes (default: 7)
  --output PATH       Save report to markdown file
  --model TEXT        Override default Ollama model
  --host TEXT         Override OpenMetadata host
  --token TEXT        Override OpenMetadata JWT token
```

### `configure`
Interactive setup for your environment. Sets up `.env` for you.
```bash
openblame configure
```

### `list-models`
Show all models available in your local Ollama instance and see which one is active.
```bash
openblame list-models
```

### `diff`
Check for schema changes in a table without running the full agent.
```bash
openblame diff <table_fqn> --days 7
```

### `lineage`
Visualize upstream and downstream lineage as a tree.
```bash
openblame lineage <table_fqn> --depth 3
```

### `mcp-server`
Start the MCP server for integration with Claude/Cursor.
```bash
openblame mcp-server
```

---

## MCP Integration (Cursor / Claude)

Add OpenBlame to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "openblame": {
      "command": "openblame",
      "args": ["mcp-server"]
    }
  }
}
```

---

## Development

```bash
# Clone and install
git clone https://github.com/manasdutta04/openblame
cd openblame
pip install -e ".[dev]"

# Run Tests (Windows)
.\test.ps1

# Run Tests (Linux/Mac)
make test
```

---

## License
OpenBlame is released under the **MIT License**. See [LICENSE](LICENSE) for details.

---

## Contributing
We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
Check out our [Code of Conduct](CODE_OF_CONDUCT.md) and [Security Policy](SECURITY.md).
