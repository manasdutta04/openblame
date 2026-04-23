import asyncio
import json
import os
import sys
from unittest.mock import MagicMock, patch

import respx
from httpx import Response
from typer.testing import CliRunner

# Add the current directory to sys.path to ensure we import the local package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from openblame.cli import app

runner = CliRunner()

@respx.mock
def test_all_commands():
    print("Starting End-to-End Mock Testing for OpenBlame\n")

    # 1. Mock OpenMetadata Endpoints
    host = "http://localhost:8585"
    
    # Owners and tags (table metadata)
    respx.get(f"{host}/api/v1/tables/name/test.table").mock(return_value=Response(200, json={
        "id": "123",
        "name": "test_table",
        "fullyQualifiedName": "test.table",
        "description": "A test table",
        "owners": [{"name": "data_eng", "type": "team"}],
        "tags": [{"tagFQN": "Tier.Tier1"}],
        "columns": [{"name": "id", "dataType": "INT"}]
    }))

    # Lineage
    respx.get(f"{host}/api/v1/lineage/table/name/test.table").mock(return_value=Response(200, json={
        "entity": {"id": "123", "type": "table", "name": "test_table"},
        "nodes": [
            {"id": "456", "type": "table", "name": "upstream_table", "fullyQualifiedName": "test.upstream"},
            {"id": "789", "type": "table", "name": "downstream_table", "fullyQualifiedName": "test.downstream"}
        ],
        "upstreamEdges": [{"fromEntity": "456", "toEntity": "123"}],
        "downstreamEdges": [{"fromEntity": "123", "toEntity": "789"}]
    }))

    # Quality tests
    respx.get(f"{host}/api/v1/dataQuality/testCases").mock(return_value=Response(200, json={
        "data": [
            {
                "name": "column_not_null",
                "entityLink": "<#E::table::test.table::columns::id>",
                "testCaseResult": {
                    "timestamp": 1776902400000, # Milliseconds
                    "testCaseStatus": "Failed",
                    "result": "Found 5 null values"
                }
            }
        ]
    }))

    # Schema diff (feed)
    respx.get(f"{host}/api/v1/feed").mock(return_value=Response(200, json={
        "data": [
            {
                "about": "<#E::table::test.table>",
                "type": "EntityUpdated",
                "changeDescription": {
                    "fieldsUpdated": [
                        {
                            "name": "columns/id", # Specific column path
                            "oldValue": {"name": "id", "dataType": "INT"},
                            "newValue": {"name": "id", "dataType": "STRING"}
                        }
                    ]
                },
                "updatedAt": 1776902400000 # Milliseconds
            }
        ]
    }))

    # 2. Mock Ollama Endpoints
    ollama_host = "http://localhost:11434"
    
    # List (connection test)
    respx.get(f"{ollama_host}/api/tags").mock(return_value=Response(200, json={"models": [{"name": "llama3"}]}))
    
    # Chat (plan and reason)
    # We'll use a side effect to handle multiple calls if needed, but for now simple mock is fine
    respx.post(f"{ollama_host}/api/chat").mock(return_value=Response(200, json={
        "message": {
            "role": "assistant",
            "content": '["Check upstream lineage", "Analyze quality failure"]' # For plan()
        }
    }))

    # ---------------------------------------------------------
    # COMMAND: investigate
    # ---------------------------------------------------------
    print("--- Testing 'investigate' command ---")
    # We need to mock OllamaClient.chat again for reasoning because the first mock is consumed or simple
    # Actually OllamaClient.chat is called twice: plan() then reason()
    # Let's use a side effect for chat
    chat_calls = 0
    def chat_side_effect(request):
        nonlocal chat_calls
        chat_calls += 1
        if chat_calls == 1:
            return Response(200, json={"message": {"role": "assistant", "content": '["Check upstream lineage", "Analyze quality failure"]'}})
        else:
            return Response(200, json={"message": {"role": "assistant", "content": '## Severity: HIGH\n\n**Root Cause**\nColumn type changed from INT to STRING.\n\n**Impact**\nDownstream test.downstream is affected.\n\n**Evidence**\n5 null values found in id column.'}})

    respx.post(f"{ollama_host}/api/chat").mock(side_effect=chat_side_effect)

    result = runner.invoke(app, ["investigate", "test.table", "--days", "7"])
    print(result.stdout)
    if result.exit_code == 0:
        print("✅ 'investigate' command passed\n")
    else:
        print(f"❌ 'investigate' command failed with exit code {result.exit_code}\n")

    # ---------------------------------------------------------
    # COMMAND: diff
    # ---------------------------------------------------------
    print("--- Testing 'diff' command ---")
    result = runner.invoke(app, ["diff", "test.table"])
    print(result.stdout)
    if result.exit_code == 0:
        print("✅ 'diff' command passed\n")
    else:
        print(f"❌ 'diff' command failed with exit code {result.exit_code}\n")

    # ---------------------------------------------------------
    # COMMAND: lineage
    # ---------------------------------------------------------
    print("--- Testing 'lineage' command ---")
    result = runner.invoke(app, ["lineage", "test.table"])
    print(result.stdout)
    if result.exit_code == 0:
        print("✅ 'lineage' command passed\n")
    else:
        print(f"❌ 'lineage' command failed with exit code {result.exit_code}\n")

    # ---------------------------------------------------------
    # COMMAND: mcp-server
    # ---------------------------------------------------------
    # Testing mcp-server is harder because it's a blocking server call.
    # We'll just check if we can import the run function.
    print("--- Testing 'mcp-server' initialization ---")
    try:
        from openblame.mcp_server import run
        print("✅ 'mcp-server' module is importable\n")
    except ImportError as e:
        print(f"❌ 'mcp-server' import failed: {e}\n")

if __name__ == "__main__":
    test_all_commands()
