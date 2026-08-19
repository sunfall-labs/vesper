# Security

Vesper is pre-1.0 software and should be evaluated in a threat model suited to
the deployment. In particular, the workspace driver's local shell is not a
sandbox, raw conversation recording can contain prompts and tool data, and
MCP servers receive the authority granted by their configured handlers.

Please do not disclose a suspected vulnerability in a public issue. Report it
privately through [GitHub Security Advisories](https://github.com/sunfall-labs/vesper/security/advisories/new)
with a description, affected version, reproduction, and any suggested
mitigation. If that channel is unavailable, contact the repository maintainer
through the GitHub profile and request a private channel.

For ordinary usage questions and non-sensitive bugs, use the public
[issue tracker](https://github.com/sunfall-labs/vesper/issues). Do not include
API keys, customer data, conversation contents, or other secrets in issues or
logs.
