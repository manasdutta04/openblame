# Security Policy

## Supported Versions
We currently support and provide security updates for the following versions of OpenBlame:

| Version | Supported |
| ------- | --------- |
| v0.1.x  | Yes       |
| < v0.1  | No        |

## Reporting a Vulnerability
We take the security of OpenBlame seriously. If you believe you have found a security vulnerability, please do NOT open a public issue. Instead, please report it via the following steps:

1. Send an email to the maintainers (refer to the GitHub profile or repository metadata).
2. Include a detailed description of the vulnerability.
3. Provide steps to reproduce the issue.

We will acknowledge your report within 48 hours and provide a timeline for a fix.

## Local-First Philosophy
OpenBlame is designed to be **local-first**.
- Metadata is pulled directly from your OpenMetadata instance to your local machine.
- Reasoning is performed by a **local Ollama instance**.
- No data is sent to external AI providers (OpenAI, Anthropic, etc.) unless you explicitly configure a proxy or a different LLM client.

Please ensure your local environment and Ollama instance are secured according to your organization's policies.
