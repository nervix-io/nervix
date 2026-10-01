# Nervix

Nervix is a realtime relay processing system. It runs a graph of runtime nodes across one or more cluster members, keeps control-plane state strongly consistent, and processes data in a high-performance relaying runtime with selective snapshot-style state persistence and replication.

## Project Status

Nervix is experimental software in active development. It is intended for evaluation, local testing, and design exploration. It is not suitable for real production workloads.

## Documentation

Detailed documentation lives in **The Nervix Book** at
[docs.nervix.io](https://docs.nervix.io/). A downloadable
[PDF edition](https://docs.nervix.io/nervix.pdf) follows the same latest snapshot.

## Quick Start

Start local broker dependencies:

```bash
just deps
```

Run the dashboard:

```bash
just cluster-dashboard
```

The local dashboard uses the `default` user with password `nervix`.

## Pull Request Reviews

The `Codex PR Review` workflow posts an advisory GitHub review when a pull request
opens, receives new commits, reopens, or becomes ready for review. It reviews the
PR's changes using `AGENTS.md` and the relevant architecture documentation.
Drafts, fork PRs, and events triggered by bot accounts are skipped. The Codex
action restricts triggers to repository collaborators with write access.

Add an OpenAI API key as the `OPENAI_API_KEY` repository secret before enabling
the workflow on the default branch:

```bash
gh secret set OPENAI_API_KEY --repo nervix-io/nervix
```

The command prompts for the key. Reviews use the OpenAI API and its billing. Codex
runs with read-only permissions; a separate job publishes its feedback for the
reviewed revision. See the [official Codex GitHub Action documentation](https://learn.chatgpt.com/docs/github-action)
for the action's configuration.
