# NSPL Agent Skill

Nervix publishes a portable [Agent Skills](https://agentskills.io) package that teaches compatible
AI coding agents how to design, explain, review, and troubleshoot Nervix configurations written in
NSPL. The maintained source is
[`nervix-io/nervix/.agents/skills/nspl`](https://github.com/nervix-io/nervix/tree/main/.agents/skills/nspl).

The skill contains guidance and references, not a Nervix client or server. Review its contents
before installing it, just as you would review any third-party instructions supplied to an agent.
It is distributed under the repository's
[FCL-1.0-ALv2 license](https://github.com/nervix-io/nervix/blob/main/LICENSE.md).

You do not need to clone the Nervix repository. The installer downloads the skill directly from
the public GitHub repository, and the installed skill uses the public Nervix documentation.

## Quick Start

### 1. Preview And Install The Skill

The skill becomes installable directly from the public repository as soon as it is merged into
the default branch. A separate package upload, repository clone, or GitHub release is not required.

GitHub CLI 2.90.0 or newer provides the preview `gh skill` commands. Inspect the complete skill
before installing it:

```bash
gh skill preview nervix-io/nervix nspl --allow-hidden-dirs
```

Install it for one agent at user scope so it is available in every project. This Codex example can
use `claude-code`, `kimi-cli`, or another value listed by `gh skill install --help` instead:

```bash
gh skill install nervix-io/nervix nspl \
  --allow-hidden-dirs \
  --agent codex \
  --scope user
```

The `--allow-hidden-dirs` option is required because the maintained source lives under
`.agents/skills/nspl`. GitHub CLI records the repository source with the installation so it can be
updated later.

#### Cross-Agent Installer

The [`skills` CLI](https://github.com/vercel-labs/skills) is an alternative that can install into
multiple supported agents at once, including agents not currently supported by `gh skill`. It
requires Node.js and can run directly through `npx`.

Install the NSPL skill for your user account so it is available in every project:

```bash
npx skills add nervix-io/nervix --skill nspl --global
```

The installer detects supported coding agents and asks which ones should receive the skill. It can
install one canonical copy and create the agent-specific links needed by Claude Code, Codex,
Cursor, Kimi Code CLI, GitHub Copilot, Gemini CLI, OpenCode, Grok Build, and other supported agents.

For a non-interactive installation into every agent supported by the installer:

```bash
npx skills add nervix-io/nervix --skill nspl --global --agent '*' --yes
```

Use `--agent '*'` only when all of those integrations are intentional. For a smaller installation,
repeat `--agent` with the desired identifiers:

```bash
npx skills add nervix-io/nervix \
  --skill nspl \
  --global \
  --agent claude-code \
  --agent codex \
  --agent kimi-code-cli \
  --agent grok
```

Both installers download only the skill content needed by the selected agents. Neither command
clones Nervix into the current directory.

### 2. Start A New Agent Session

Open the selected coding agent in your own project, configuration repository, or any other working
directory. Start a new session after installation so the agent discovers the newly installed
skill.

### 3. Ask The Agent To Use NSPL

The most portable invocation is to name the skill in the request:

```text
Use the nspl skill to design a Nervix configuration that reads JSON orders from Kafka,
deduplicates them by order_id for 10 minutes, and publishes valid orders to another Kafka topic.
Use placeholders for broker addresses and credentials.
```

Agents may also expose installed skills through a skill picker or command. For example, use
`$nspl` or `/nspl` when the selected agent supports that form:

```text
$nspl Review this Nervix configuration for invalid types, missing flush policies, and branch
isolation problems: <paste NSPL here>
```

The agent can also select the skill automatically from an ordinary request about authoring,
explaining, reviewing, or troubleshooting a Nervix configuration. Naming `nspl` explicitly is
useful when you want to guarantee that the installed guidance is loaded.

## Describe What You Need

Give the agent as much of this deployment contract as you know:

- the input system, external topic, queue, stream, table, or other entity, and a sample payload;
- the required internal fields and exact types, when known;
- whether records are unbranched or isolated by fields such as tenant, device, or customer;
- filtering, transformation, deduplication, windows, correlation, inference, lookup, or custom
  WASM processing;
- the output system and desired payload shape;
- delivery, ordering, batching, flush, error-handling, TLS, and credential requirements; and
- whether you want a new graph, an explanation, a review, or help diagnosing an error.

Use this copyable template when starting from a new graph:

```text
Use the nspl skill to produce a complete Nervix configuration.

Input system and external entity:
Sample input payload:
Required processing:
Branch key, or unbranched:
Output system and external entity:
Delivery, flush, and error requirements:
Deployment-specific values that should remain placeholders:

Return assumptions, external prerequisites, ordered NSPL command phases, and verification
commands. Do not invent undocumented syntax.
```

If details are missing, the skill directs the agent to ask only questions that materially change
the graph or to use conspicuous placeholders and state its assumptions.

## Use The Result

A generated configuration should contain:

1. assumptions and external prerequisites;
2. separate client-local and transactional server command phases;
3. complete NSPL with deployment placeholders called out; and
4. relevant `SHOW`, `DESCRIBE`, lookup, or subscription commands for verification.

The skill does not connect to or configure a running Nervix deployment by itself. Review the
result, replace placeholders, provision required external entities such as Kafka topics, and then
submit the NSPL through your normal Nervix client workflow. See [Running Nervix](running-locally.md)
and the [NSPL Overview](nspl-overview.md) for the current client and language documentation.

## Install For One Project

With GitHub CLI, change the scope from `user` to `project`:

```bash
gh skill install nervix-io/nervix nspl \
  --allow-hidden-dirs \
  --agent codex \
  --scope project
```

With the cross-agent installer, omit `--global`:

```bash
npx skills add nervix-io/nervix --skill nspl
```

Run the command from the target project's root. Review the generated agent directories before
committing them because project-scoped installations become part of that project rather than the
Nervix repository.

## Verify The Installation

List skills installed by GitHub CLI:

```bash
gh skill list
```

List globally installed skills:

```bash
npx skills list --global
```

For a project-scoped installation, run `npx skills list` from the project root. Confirm that `nspl`
appears for the expected agents.

## Update The Skill

Update an installation managed by GitHub CLI:

```bash
gh skill update nspl
```

Update a global installation to the latest version from the default branch of
`nervix-io/nervix`:

```bash
npx skills update nspl --global
```

Update a project-scoped installation from that project's root:

```bash
npx skills update nspl --project
```

Start a new agent session after updating when an existing session has already loaded the previous
skill contents. Update the skill whenever upgrading Nervix or when the public NSPL surface changes.

For stricter reproducibility, install a reviewed commit instead of following repository updates:

```bash
gh skill install nervix-io/nervix nspl \
  --allow-hidden-dirs \
  --agent codex \
  --scope user \
  --pin <commit-sha>
```

Pinned installations do not update until they are reinstalled with a different commit.

## Trust And Safety

Keeping the skill in the Nervix repository makes its changes visible in the same version history
and code review as NSPL and its documentation. It does not make installed skills inherently
trusted: a skill supplies instructions to an agent, and future updates can change those
instructions.

- Preview the skill before installing or updating it.
- Review changes to `SKILL.md` and every bundled reference as carefully as code changes.
- Pin a reviewed commit when reproducibility matters more than automatic updates.
- Never add credentials or deployment secrets to a skill directory.
- Remember that an agent follows a skill with the permissions already available to that agent; the
  skill itself grants no additional access.

The current NSPL skill contains instructions, public-documentation references, and display
metadata. It contains no executable scripts.

## Manual Installation

If Node.js or the `skills` CLI is unavailable, download the complete
[`nspl` skill directory](https://github.com/nervix-io/nervix/tree/main/.agents/skills/nspl) and copy
it into a skill location supported by the target agent. Keep `SKILL.md`, `references/`, and any
future bundled resources together.

Common project-level locations include:

- `.agents/skills/nspl/` for Codex, Kimi Code CLI, GitHub Copilot, Cursor, Gemini CLI,
  Antigravity, and OpenCode
- `.claude/skills/nspl/` for Claude Code
- `.grok/skills/nspl/` for Grok Build

For a manual update, replace the entire installed `nspl` directory with the latest directory from
the repository. Do not update only `SKILL.md`, because its bundled references are part of the same
versioned skill.
