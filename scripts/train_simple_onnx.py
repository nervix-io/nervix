#!/usr/bin/env python3
"""Train and write a tiny ONNX model for Nervix inferencer tests.

The model is intentionally dependency-free: it trains a two-feature linear
regressor with plain Python and writes the ONNX protobuf wire format directly.
Inputs:
  features: FLOAT tensor [batch, 2]
Outputs:
  score: FLOAT tensor [batch, 1]
"""

from __future__ import annotations

import argparse
import math
import struct
from pathlib import Path


TENSOR_FLOAT = 1


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
    samples = [
        ([0.0, 0.0], 0.10),
        ([1.0, 0.0], 0.85),
        ([0.0, 1.0], -0.40),
        ([1.0, 1.0], 0.35),
        ([2.0, 1.0], 1.10),
        ([1.0, 2.0], -0.15),
        ([2.0, 2.0], 0.60),
        ([3.0, 1.0], 1.85),
    ]
    weights = [0.0, 0.0]
    bias = 0.0
    learning_rate = 0.04

    for _ in range(1200):
        grad_w = [0.0, 0.0]
        grad_b = 0.0
        for features, expected in samples:
            predicted = weights[0] * features[0] + weights[1] * features[1] + bias
            error = predicted - expected
            grad_w[0] += error * features[0]
            grad_w[1] += error * features[1]
            grad_b += error
        scale = 2.0 / len(samples)
        weights[0] -= learning_rate * scale * grad_w[0]
        weights[1] -= learning_rate * scale * grad_w[1]
        bias -= learning_rate * scale * grad_b

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


def tensor_type(dims: list[int | str]) -> bytes:
    tensor = field_varint(1, TENSOR_FLOAT) + message_field(2, tensor_shape(dims))
    return message_field(1, tensor)


def value_info(name: str, dims: list[int | str]) -> bytes:
    return field_string(1, name) + message_field(2, tensor_type(dims))


def tensor_initializer(name: str, dims: list[int], values: list[float]) -> bytes:
    out = bytearray()
    for dim in dims:
        out += field_varint(1, dim)
    out += field_varint(2, TENSOR_FLOAT)
    for value in values:
        out += field_fixed32(4, value)
    out += field_string(8, name)
    return bytes(out)


def node(op_type: str, inputs: list[str], outputs: list[str], name: str) -> bytes:
    out = bytearray()
    for input_name in inputs:
        out += field_string(1, input_name)
    for output_name in outputs:
        out += field_string(2, output_name)
    out += field_string(3, name)
    out += field_string(4, op_type)
    return bytes(out)


def graph(weights: list[float], bias: float) -> bytes:
    out = bytearray()
    out += message_field(1, node("MatMul", ["features", "weights"], ["linear"], "linear"))
    out += message_field(1, node("Add", ["linear", "bias"], ["score"], "score"))
    out += field_string(2, "nervix_simple_score")
    out += message_field(5, tensor_initializer("weights", [2, 1], weights))
    out += message_field(5, tensor_initializer("bias", [1], [bias]))
    out += message_field(11, value_info("features", ["batch", 2]))
    out += message_field(12, value_info("score", ["batch", 1]))
    return bytes(out)


def model_proto(weights: list[float], bias: float) -> bytes:
    opset_import = field_string(1, "") + field_varint(2, 13)
    return (
        field_varint(1, 8)
        + field_string(2, "nervix-test-generator")
        + message_field(7, graph(weights, bias))
        + message_field(8, opset_import)
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default="tests/fixtures/onnx/simple_score.onnx",
        help="path to write the generated ONNX model",
    )
    args = parser.parse_args()

    weights, bias = train_linear_regressor()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(model_proto(weights, bias))
    print(f"wrote {output} weights={weights!r} bias={bias!r}")


if __name__ == "__main__":
    main()
