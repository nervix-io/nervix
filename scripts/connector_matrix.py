#!/usr/bin/env python3
"""Render Plexus connector reference regions from the compiled vocabulary.

The JSON is a one-way documentation projection, never a product configuration. Explanatory
prose lives in docs/connector-matrix.toml; it cannot add or remove a capability. No Rust source
or Markdown table is parsed to infer a capability.
"""

import argparse
import difflib
import json
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def export_declarations(path):
    """Publish only a complete compiled export, preserving the configured compiler wrapper."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            subprocess.run(["cargo", "run", "--quiet", "--package", "nervix-models",
                            "--example", "connector_capabilities"], cwd=ROOT, stdout=stream, check=True)
            stream.close()
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def cell(value):
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("|", "\\|").replace("\n", "<br>")


def identity(entity):
    return entity.lower().replace(" ", "-")


def table(rows, headings=("Capability", "Declaration")):
    lines = ["| " + " | ".join(headings) + " |", "| " + " | ".join("---" for _ in headings) + " |"]
    lines.extend("| " + " | ".join(cell(value) for value in row) + " |" for row in rows)
    return "\n".join(lines) + "\n"


def option_text(options):
    values = []
    for option in options:
        forms = "; ".join(f"`{form['example']}` ({form['type']})" for form in option["forms"])
        values.append(f"{forms}; {option['presence']}")
    return "\n".join(values) or "No entity-specific options."


def required_notes(surface):
    needed = {}
    for family in ("ingest", "emit", "clients"):
        if not surface[family]:
            raise ValueError(f"{family}: declaration export is empty")
    clients = [client["kind"] for client in surface["clients"]]
    if len(clients) != len(set(clients)):
        raise ValueError("duplicate client kind")
    for side in ("ingest", "emit"):
        seen = set()
        for row in surface[side]:
            entity = row["entity"]
            if entity in seen:
                raise ValueError(f"duplicate {side} entity {entity}")
            seen.add(entity)
            fields = needed.setdefault(entity, set())
            if row["locator"] is None:
                fields.add(f"{side}_locator")
            if not row["headers"]:
                fields.add(f"{side}_headers")
            if side == "ingest" and not row["metadata"]:
                fields.add("metadata")
            if side == "emit":
                for field in ("envelope", "properties"):
                    if not row[field]:
                        fields.add(field)
            fields.add("mirror")
    return needed


def validate_notes(surface, notes):
    needed = required_notes(surface)
    if set(needed) != set(notes):
        raise ValueError(f"entity explanations differ: missing {sorted(set(needed) - set(notes))}; stale {sorted(set(notes) - set(needed))}")
    for entity, fields in needed.items():
        if fields != set(notes[entity]):
            raise ValueError(f"{entity} explanations differ: missing {sorted(fields - set(notes[entity]))}; stale {sorted(set(notes[entity]) - fields)}")
        for field in fields:
            if not isinstance(notes[entity][field], str) or not notes[entity][field].strip():
                raise ValueError(f"{entity} {field} requires a transport reason")


def absent(notes, row, field):
    return f"Not offered: {notes[row['entity']][field]}"


def mirror(surface, side, row, notes):
    opposite = "emit" if side == "ingest" else "ingest"
    peers = [peer for peer in surface[opposite] if peer["connector"] == row["connector"]]
    reason = notes[row["entity"]]["mirror"]
    if not peers:
        return f"No {opposite} entity is declared. {reason}"
    names = ", ".join(f"`{peer['entity']}`" for peer in peers)
    differences = []
    for field in ("locator", "options", "modes", "headers", "reference"):
        if any(peer[field] != row[field] for peer in peers):
            differences.append(field)
    if side == "ingest" and row["metadata"]:
        differences.append("ingest metadata")
    if side == "emit" and (row["envelope"] or row["properties"]):
        differences.append("emit envelope/properties")
    compared = ", ".join(differences) or "the same shared declaration fields"
    return f"Compare {names}: {compared}. {reason}"


def bounds_rows(bounds):
    """The rows that state what an entity's connector holds and how long it waits."""
    buffers = "\n".join(f"{buffer['holds']}: {buffer['bound']}" for buffer in bounds["buffers"])
    waits = "\n".join(f"{wait['wait']}: {wait['bound']}" for wait in bounds["waits"])
    checks = "\n".join(f"`STALLED PEER {key}`" for key in bounds["deadline_keys"])
    return [
        ("Buffers", buffers or "The connector holds no buffer between its transport and the host."),
        ("Network waits", waits or "The connector waits on no network peer of its own."),
        ("Driver limits", "\n".join(bounds["driver_limits"]) or "None declared."),
        ("Stalled-peer checks", checks or "None: no client deadline key bounds a wait."),
    ]


def header_label(model):
    """A declared header model as `DESCRIBE` names it: its layout and names, then what carries each
    typed value where its values are typed."""
    label = f"`{model['layout']}`, {model['names']} names"
    if model["values"]:
        label += f"; {model['values']}"
    return label


def header_meaning(model):
    """What a declared header model means for the headers of one message."""
    meaning = f"{model['layout_meaning']}; {model['names_meaning']}"
    if model["values"]:
        meaning += "; a value's exact type decides the type it travels as"
    return meaning


def header_model(model):
    """A declared header model as the manuals state it: its label, then its meaning."""
    return f"{header_label(model)}: {header_meaning(model)}"


def entity_rows(row, side, notes):
    location = row["locator"]
    if location is None:
        location = absent(notes, row, f"{side}_locator")
    functions = "`read_header`, `read_headers`" if side == "ingest" else "`write_header`"
    if row["headers"]:
        headers = f"{functions}; {header_model(row['headers'])}"
    else:
        headers = absent(notes, row, f"{side}_headers")
    rows = [("Reference", row["reference"]), ("Locator and validation rule", location),
            ("Modes (complete bodies after MODE)", "\n".join(row["modes"])),
            ("Options in statement order", option_text(row["options"])), ("Headers", headers)]
    if side == "ingest":
        rows.extend([
            ("Quiesce (complete bodies after ON QUIESCE)", "\n".join(row["quiesce"])),
            ("Metadata", "\n".join(row["metadata"]) or absent(notes, row, "metadata")),
            ("Every live node", "yes" if row["every_node"] else "no; scheduled owner"),
            ("Position class", row["position"]), ("Acknowledgement confirmation", row["confirmation"]),
        ])
        rows.extend(bounds_rows(row["bounds"]))
    else:
        rows.extend([
            ("Body", row["body"]), ("BATCH", row["batch"]),
            ("Protocol batch maximum", row["batch_maximum"] or "No additional protocol maximum is declared; general BATCH limits still apply."),
            ("Batch transformation required", "yes" if row["batch_transformation"] else "no"),
            ("Envelope", "\n".join(row["envelope"]) or absent(notes, row, "envelope")),
            ("PROPERTIES keys and exact types", "\n".join(row["properties"]) or absent(notes, row, "properties")),
        ])
        rows.extend(bounds_rows(row["bounds"]))
        contract = row["value_contract"]
        if contract is not None:
            rows.extend([
                ("VALUES fields, exact types and constraints", "\n".join(contract["fields"])),
                ("ATTRIBUTES values", contract["attributes"]),
                ("RESOURCE values", contract["resource"]),
                ("Unsupported OTLP features and reasons", "\n".join(contract["unsupported"])),
            ])
    return rows


def render_matrix(surface, notes):
    validate_notes(surface, notes)
    lines = ["# Connector Capabilities\n", "This reference is generated from the current declarations in Plexus, the Nervix graph definition language. Regenerate it with `just connector-matrix`; `just validate-connector-matrix` rejects drift in this chapter and the generated manual regions.\n",
             "The tables describe the implemented surface. A connector Epic can extend a declaration and regenerate these tables in the same change. Modes below are complete bodies after `MODE`; options retain their declared order, exact value types, presence and defaults. Locator rule names identify the vocabulary's validator; see [External Names](ingestors.md#external-names).\n",
             "Ingest and emit are separate contracts: consuming a broker position and publishing a record need different acknowledgements. Each mirror column compares every declared entity of the same connector on the other side and explains transport differences. An absent capability requires reviewed explanatory prose; stale or missing explanations fail validation.\n",
             "Client CONFIG keys below come from the same declaration used by validation, completion and public inspection. Keys are lowercase and case-sensitive; every key occurs at most once. Required keys, Nervix-owned value types and TLS identity pairs validate before storage. Driver property namespaces and address query options remain intentional pass-through; undeclared driver properties are confidential in public inspection. Kafka properties and Postgres, MySQL, Redis, RabbitMQ and NATS addresses also pass network-free driver checks before commit. Authentication, connectivity and checks requiring a remote service wait for initialization. Nervix-owned file values accept resource mount templates and must resolve to readable files; driver-owned file properties retain their native interpretation. HTTPS retains the driver's built-in trust roots; MQTT requires tls_ca_file for mqtts, as its key's conditional presence declares. tls_ca_file follows the connector's documented augmentation or replacement policy, and a client certificate requires its private key.\n"]
    for side, title in (("ingest", "Ingest"), ("emit", "Emit")):
        lines.append(f"## {title}\n")
        for row in surface[side]:
            lines.append(f"### {row['entity']} {title.lower()}\n")
            rows = [("Entity", row["entity"], mirror(surface, side, row, notes))]
            rows.extend((name, value, "") for name, value in entity_rows(row, side, notes))
            lines.append(table(rows, ("Capability", "Declaration", "Mirror and transport reason")))
    lines.append("## Clients\n")
    for client in surface["clients"]:
        lines.append(f"### {client['kind']} client\n")
        lines.append(table([("Kind", client["kind"]), ("Connection pooling", client["pool"]),
                            ("CONFIG owner class", client["config_owner"]),
                            ("Statement options", option_text(client["options"]))]))
        lines.append(table([
            (f"`{key['name']}`", key["presence"], key["type"], key["owner"],
             "yes" if key["confidential"] else "no",
             key["file"])
            for key in client["keys"]
        ], ("CONFIG key", "Presence", "Value", "Owner", "Confidential", "File role")))
        namespaces = ", ".join(client["namespaces"]) or "none; only declared keys are accepted"
        reserved = ", ".join(f"`{key}`" for key in client["reserved"]) or "none"
        lines.append(f"Delegated property namespaces: {namespaces}. Reserved keys: {reserved}.\n")
        if client["constraints"]:
            lines.append(table([("Additional declared constraints", "\n".join(client["constraints"]))]))
    return "\n".join(lines)


def region(text, name, content):
    start = f"<!-- connector-matrix:{name}:start -->"
    end = f"<!-- connector-matrix:{name}:end -->"
    if text.count(start) != 1 or text.count(end) != 1:
        raise ValueError(f"{name}: expected exactly one start/end marker pair")
    before, rest = text.split(start)
    _, after = rest.split(end)
    return before + start + "\n" + content.rstrip() + "\n" + end + after


def manual(row, side, notes):
    link = f"connector-capabilities.md#{identity(row['entity'])}-{side}"
    choices = "Choose one complete mode from the table."
    if side == "ingest":
        choices += " Choose a compatible quiesce form and satisfy its option requirements."
    return (f"```plexus,ignore\n{row['template']}\n```\n\n"
            f"Angle brackets and alternatives form a synopsis; option values shown are declared examples. {choices}\n\n"
            + table(entity_rows(row, side, notes))
            + f"\nSee the [{row['entity']} capability and mirror reference]({link}).\n")


def classified_table(surface, field, explanations):
    groups = {}
    for row in surface["ingest"]:
        groups.setdefault(row[field], []).append(row["entity"])
    if set(groups) != set(explanations):
        raise ValueError(f"{field}: explanation classes do not match the declarations")
    label = "Position class" if field == "position" else "Acknowledgement confirmation"
    return table([(f"`{key}`", ", ".join(f"`{entity}`" for entity in entities), explanations[key])
                  for key, entities in sorted(groups.items())], (label, "Entities", "Meaning"))


def header_models_table(surface):
    """Every declared header model with the entities that declare it on each side."""
    groups = {}
    for side in ("ingest", "emit"):
        for row in surface[side]:
            model = row["headers"]
            if not model:
                continue
            key = (model["layout"], model["names"], model["values"] or "")
            group = groups.setdefault(key, {"ingest": [], "emit": [], "meaning": model})
            group[side].append(f"`{row['entity']}`")
    rows = []
    for _, group in sorted(groups.items()):
        model = group["meaning"]
        rows.append((header_label(model), ", ".join(group["ingest"]) or "None",
                     ", ".join(group["emit"]) or "None", header_meaning(model)))
    return table(rows, ("Header model", "Ingest", "Emit", "Meaning"))


def side_headers(surface, side):
    """One side's header models entity by entity, and the entities of that side that carry none."""
    rows = []
    without = []
    for row in surface[side]:
        model = row["headers"]
        if not model:
            without.append(f"`{row['entity']}`")
            continue
        rows.append((f"`{row['entity']}`", header_label(model), header_meaning(model)))
    if side == "ingest":
        text = table(rows, ("Ingestor", "Header model", "What `read_header` and `read_headers` find"))
        closing = "carry no transport headers and refuse `read_header` and `read_headers`"
    else:
        text = table(rows, ("Emitter", "Header model", "What a record's `write_header` calls become"))
        closing = "write no transport headers and refuse `write_header`"
    if without:
        text += f"\n{', '.join(without)} {closing}.\n"
    return text


def documents(root, surface, notes, policy):
    output = {"docs/src/connector-capabilities.md": render_matrix(surface, notes)}
    for side, path in (("ingest", "docs/src/ingestors.md"), ("emit", "docs/src/emitters.md")):
        text = (root / path).read_text()
        for row in surface[side]:
            text = region(text, f"{side}-{identity(row['entity'])}", manual(row, side, notes))
        if side == "ingest":
            rows = []
            for row in surface[side]:
                for option in row["options"]:
                    if option["keywords"] == "INSTANCES":
                        rows.append((row["entity"], option["presence"]))
            text = region(text, "instancing", table(rows, ("Entity", "INSTANCES")))
        text = region(text, f"{side}-headers", side_headers(surface, side))
        output[path] = text
    path = "docs/src/connector-contract.md"
    text = (root / path).read_text()
    for field in ("position", "confirmation"):
        text = region(text, field, classified_table(surface, field, policy[field]))
    text = region(text, "headers", header_models_table(surface))
    output[path] = text
    return output


def publish(root, output, check):
    clean = True
    for path, content in output.items():
        target = root / path
        current = target.read_text() if target.exists() else ""
        if current == content:
            continue
        if check:
            clean = False
            print("".join(difflib.unified_diff(current.splitlines(True), content.splitlines(True),
                                             fromfile=path, tofile=path + " (generated)")), file=sys.stderr)
        else:
            target.write_text(content)
    return clean


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--declarations", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--export", action="store_true", help="compile the current declarations before rendering")
    parser.add_argument("--export-only", action="store_true", help="publish only the compiled declaration input")
    args = parser.parse_args(argv)
    try:
        if args.export or args.export_only:
            export_declarations(args.declarations)
        if args.export_only:
            return 0
        surface = json.loads(args.declarations.read_text())
        policy = tomllib.loads((ROOT / "docs/connector-matrix.toml").read_text())
        output = documents(ROOT, surface, policy["entity"], policy)
        clean = publish(ROOT, output, args.check)
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f"connector matrix: {error}", file=sys.stderr)
        return 1
    if not clean:
        print("connector matrix is stale; run just connector-matrix", file=sys.stderr)
        return 1
    print(f"connector matrix: {len(surface['ingest'])} ingest entities, {len(surface['emit'])} emit entities, {len(surface['clients'])} client kinds; all generated regions match")
    return 0


if __name__ == "__main__":
    sys.exit(main())
