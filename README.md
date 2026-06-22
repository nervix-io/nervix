# Nervix

Nervix is a realtime relay processing system. It runs a graph of runtime nodes across one or more cluster members, keeps control-plane state strongly consistent, and processes data in a high-performance relaying runtime with selective snapshot-style state persistence and replication.

## Project Status

Nervix is experimental software in active development. It is intended for evaluation, local testing, and design exploration. It is not suitable for real production workloads.

## Documentation

Detailed documentation lives in **The Nervix Book** under [https://docs.nervix.io/](https://docs.nervix.io/).

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
