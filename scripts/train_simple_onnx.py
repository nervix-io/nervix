#!/usr/bin/env python3
"""Train and write a tiny ONNX model for Nervix inferencer tests.

The model is intentionally dependency-free: it trains a two-feature linear
regressor with plain Python and writes the ONNX protobuf wire format directly.
The per-message model has `features: FLOAT[2] -> score: FLOAT[1]`.
The batch model has `features, mask: FLOAT[batch, 2] -> scores: FLOAT[batch, 2]`.
Its outputs include a batch-wide feature mean so tests can distinguish one
collected-batch invocation from repeated single-message invocations.
"""

from __future__ import annotations

import argparse
import math
import struct
from pathlib import Path


TENSOR_FLOAT = 1
TENSOR_DOUBLE = 11


def key(field_number: int, wire_type: int) -> bytes:
    return varint((field_number << 3) | wire_type)


def varint(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def fixed32(value: float) -> bytes:
    return struct.pack("<f", value)


def field_varint(field_number: int, value: int) -> bytes:
    return key(field_number, 0) + varint(value)


def field_fixed32(field_number: int, value: float) -> bytes:
    return key(field_number, 5) + fixed32(value)


def field_string(field_number: int, value: str) -> bytes:
    data = value.encode("utf-8")
    return key(field_number, 2) + varint(len(data)) + data


def field_bytes(field_number: int, value: bytes) -> bytes:
    return key(field_number, 2) + varint(len(value)) + value


def message_field(field_number: int, value: bytes) -> bytes:
    return field_bytes(field_number, value)


def train_linear_regressor() -> tuple[list[float], float]:
    weights = [0.75, -0.5]
    bias = 0.125

    for value in [*weights, bias]:
        if not math.isfinite(value):
            raise RuntimeError("training produced a non-finite parameter")
    return weights, bias


def tensor_shape(dims: list[int | str]) -> bytes:
    out = bytearray()
    for dim in dims:
        if isinstance(dim, str):
            out += message_field(1, field_string(2, dim))
        else:
            out += message_field(1, field_varint(1, dim))
    return bytes(out)


def tensor_type(dims: list[int | str], element_type: int = TENSOR_FLOAT) -> bytes:
    tensor = field_varint(1, element_type) + message_field(2, tensor_shape(dims))
    return message_field(1, tensor)


def value_info(
    name: str, dims: list[int | str], element_type: int = TENSOR_FLOAT
) -> bytes:
    return field_string(1, name) + message_field(2, tensor_type(dims, element_type))


def tensor_initializer(name: str, dims: list[int], values: list[float]) -> bytes:
    out = bytearray()
    for dim in dims:
        out += field_varint(1, dim)
    out += field_varint(2, TENSOR_FLOAT)
    for value in values:
        out += field_fixed32(4, value)
    out += field_string(8, name)
    return bytes(out)


def attribute_int(name: str, value: int) -> bytes:
    return field_string(1, name) + field_varint(3, value) + field_varint(20, 2)


def attribute_ints(name: str, values: list[int]) -> bytes:
    out = bytearray(field_string(1, name))
    for value in values:
        out += field_varint(8, value)
    out += field_varint(20, 7)
    return bytes(out)


def node(
    op_type: str,
    inputs: list[str],
    outputs: list[str],
    name: str,
    attributes: list[bytes] | None = None,
) -> bytes:
    out = bytearray()
    for input_name in inputs:
        out += field_string(1, input_name)
    for output_name in outputs:
        out += field_string(2, output_name)
    out += field_string(3, name)
    out += field_string(4, op_type)
    for attribute in attributes or []:
        out += message_field(5, attribute)
    return bytes(out)


def per_message_graph(weights: list[float], bias: float) -> bytes:
    out = bytearray()
    out += message_field(1, node("MatMul", ["features", "weights"], ["linear"], "linear"))
    out += message_field(1, node("Add", ["linear", "bias"], ["score"], "score"))
    out += field_string(2, "nervix_per_message_score")
    out += message_field(5, tensor_initializer("weights", [2, 1], weights))
    out += message_field(5, tensor_initializer("bias", [1], [bias]))
    out += message_field(11, value_info("features", [2]))
    out += message_field(12, value_info("score", [1]))
    return bytes(out)


def batch_graph() -> bytes:
    out = bytearray()
    out += message_field(
        1,
        node(
            "ReduceMean",
            ["features"],
            ["feature_mean"],
            "feature_mean",
            [attribute_ints("axes", [0]), attribute_int("keepdims", 1)],
        ),
    )
    out += message_field(1, node("Add", ["features", "mask"], ["combined"], "combine"))
    out += message_field(1, node("Add", ["combined", "feature_mean"], ["scores"], "scores"))
    out += field_string(2, "nervix_batch_score")
    out += message_field(11, value_info("features", ["batch", 2]))
    out += message_field(11, value_info("mask", ["batch", 2]))
    out += message_field(12, value_info("scores", ["batch", 2]))
    return bytes(out)


def f64_graph() -> bytes:
    out = bytearray()
    out += message_field(1, node("Identity", ["features"], ["score"], "score"))
    out += field_string(2, "nervix_f64_score")
    out += message_field(11, value_info("features", [2], TENSOR_DOUBLE))
    out += message_field(12, value_info("score", [2], TENSOR_DOUBLE))
    return bytes(out)


def model_proto(graph: bytes) -> bytes:
    opset_import = field_string(1, "") + field_varint(2, 13)
    return (
        field_varint(1, 8)
        + field_string(2, "nervix-test-generator")
        + message_field(7, graph)
        + message_field(8, opset_import)
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default="tests/fixtures/onnx/simple_score.onnx",
        help="path to write the generated ONNX model",
    )
    parser.add_argument(
        "--batch-output",
        default="tests/fixtures/onnx/batch_score.onnx",
        help="path to write the generated batched ONNX model",
    )
    parser.add_argument(
        "--f64-output",
        default="tests/fixtures/onnx/f64_score.onnx",
        help="path to write the generated unsupported-F64 ONNX model",
    )
    args = parser.parse_args()

    weights, bias = train_linear_regressor()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(model_proto(per_message_graph(weights, bias)))
    batch_output = Path(args.batch_output)
    batch_output.parent.mkdir(parents=True, exist_ok=True)
    batch_output.write_bytes(model_proto(batch_graph()))
    f64_output = Path(args.f64_output)
    f64_output.parent.mkdir(parents=True, exist_ok=True)
    f64_output.write_bytes(model_proto(f64_graph()))
    print(f"wrote {output} weights={weights!r} bias={bias!r}")
    print(f"wrote {batch_output}")
    print(f"wrote {f64_output}")


if __name__ == "__main__":
    main()
