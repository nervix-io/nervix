# ONNX inference examples

These examples show the two ONNX inferencer execution modes. They intentionally
do not create a model implicitly. Before applying either graph, place the model
directory at `examples/onnx-inference/models`:

- `score.onnx` must expose `features: F32[128]` and `scores: F32[10]`.
- `batch-score.onnx` must expose `features: F32[dynamic, 128]`,
  `mask: F32[dynamic, 128]`, and `scores: F32[dynamic, 10]`.

Each NSPL file explicitly creates and uploads the resource, schemas, branch, and
relays before creating the inferencer. The per-message graph invokes ONNX once
per message. The batched graph invokes ONNX once per collected flush batch and
preserves row order when assigning output slices back to messages.
