# Paced Simulation

Two runnable application drivers for a paced sensor simulation. Each one attaches to the domain
clock, submits sensor readings stamped with the tick centers the clock reaches through a client
ingestor, and applies and acknowledges the readings the graph constructs through attached client
emitters, all on one session.

| File | What it is |
| --- | --- |
| `paced_simulation.nspl` | The graph: a paced domain, client ingestors with `TIMESTAMP AT` and `TIMESTAMP NOW`, a branched relay, and attached client emitters for readings and rejection notices |
| `rust/` | The Rust driver, `nervix-paced-simulation`, built on the Rust client library |
| `python/paced_simulation.py` | The Python driver, built on the shared C binding through `ctypes` and the standard library alone |

Both drivers take the same options, print the same report, and write the same ledger and effect
store. [Paced Simulation Drivers](../../docs/src/paced-simulation-drivers.md) explains the graph,
every option and report line, and what the drivers do when the clock, the session or the domain
changes under them.

## Run It

Load the graph into a running cluster, then start its clock at the pace the simulation should run
at. The file's domain bootstrap and `USE` are separate phases; load its graph transaction with
the CLI's domain selection. `TIME RATE 4.0` runs domain time four times faster than wall time:

```bash
nervix-cli --command "CREATE PACED DOMAIN paced_simulation WITH PERIOD 100ms SKEW 100ms;"
nervix-cli --domain paced_simulation --command \
  "$(sed -n '/^BEGIN;/,$p' examples/paced-simulation/paced_simulation.nspl)"
nervix-cli --domain paced_simulation --command "START AT NOW TIME RATE 4.0;"
```

Check the printed command dispositions; ordinary `--command` currently prints a refusal without
a nonzero exit status. The [architecture chapter](../../docs/src/client-io-architecture.md)
explains the graph's endpoint ownership, ACK, credit and restoration boundaries.

Run the Rust driver:

```bash
just paced-simulation --username default --password "$NERVIX_PASSWORD" --ticks 100 --sensors 3
```

Run the Python driver, which builds the shared binding first:

```bash
just paced-simulation-python --username default --password "$NERVIX_PASSWORD" --ticks 100
```

Both connect to `http://127.0.0.1:47391` unless `--server` names another node; any live node
serves them. Each writes `paced-simulation-ledger.jsonl` and `paced-simulation-effects.jsonl` in
the working directory unless `--ledger` and `--effects` name other files. Interrupt a run with
Ctrl-C: the driver stops submitting, waits for the outcomes it awaits, closes its endpoints and
prints its summary.

The public scenarios in `tests/features/runtime/paced_simulation.feature` run both programs as
published against this graph.

With `--follow-generations`, both drivers retry a stopped-domain producer open within their
existing 30-second budget after observing a later paced START generation. An initial open still
requires a running domain. `just test-paced-simulation` checks both drivers' open policies.
