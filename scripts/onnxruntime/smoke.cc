// Link and execute the public C API against the packaged archive and a generated fixture.
#include "onnxruntime_c_api.h"
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

static const OrtApi* api;

static void check(OrtStatus* status) {
  if (status) {
    std::fprintf(stderr, "%s\n", api->GetErrorMessage(status));
    api->ReleaseStatus(status);
    std::exit(1);
  }
}

int main(int argc, char** argv) {
  if (argc != 4) return 2;
  const bool cuda = std::strcmp(argv[3], "cuda") == 0;
  const bool load_cuda = cuda || std::strcmp(argv[3], "cuda-load") == 0;
  if (!load_cuda && std::strcmp(argv[3], "cpu") != 0) return 2;
#if !defined(__linux__)
  if (load_cuda) return 2;
#endif
  const auto* base = OrtGetApiBase();
  if (std::strcmp(base->GetVersionString(), argv[2]) != 0) return 3;
  api = base->GetApi(ORT_API_VERSION);
  if (!api) return 4;
  OrtEnv* environment;
  OrtSessionOptions* options;
  OrtSession* session;
  OrtMemoryInfo* memory;
  OrtValue* input;
  OrtValue* output = nullptr;
  check(api->CreateEnv(ORT_LOGGING_LEVEL_WARNING, "nervix-build", &environment));
  check(api->CreateSessionOptions(&options));
  if (load_cuda) {
    OrtCUDAProviderOptionsV2* cuda_options;
    check(api->CreateCUDAProviderOptions(&cuda_options));
    OrtAllocator* allocator;
    char* options_string;
    check(api->GetAllocatorWithDefaultOptions(&allocator));
    // Loading through the C API initializes the provider bridge before the CUDA module.
    // Inspecting options loads its registry without creating a GPU execution provider.
    check(api->GetCUDAProviderOptionsAsString(cuda_options, allocator, &options_string));
    check(api->AllocatorFree(allocator, options_string));
    if (cuda) {
      check(api->SessionOptionsAppendExecutionProvider_CUDA_V2(options, cuda_options));
      check(api->AddSessionConfigEntry(options, "session.disable_cpu_ep_fallback", "1"));
    }
    api->ReleaseCUDAProviderOptions(cuda_options);
  }
  check(api->CreateSession(environment, argv[1], options, &session));
  check(api->CreateCpuMemoryInfo(OrtArenaAllocator, OrtMemTypeDefault, &memory));
  float features[] = {1.0f, 2.0f};
  int64_t shape[] = {2};
  check(api->CreateTensorWithDataAsOrtValue(memory, features, sizeof(features), shape, 1,
                                          ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT, &input));
  const char* inputs[] = {"features"};
  const char* outputs[] = {"score"};
  check(api->Run(session, nullptr, inputs, &input, 1, outputs, 1, &output));
  void* values;
  check(api->GetTensorMutableData(output, &values));
  const bool correct = std::fabs(static_cast<float*>(values)[0] + 0.125f) < 0.000001f;
  api->ReleaseValue(output);
  api->ReleaseValue(input);
  api->ReleaseMemoryInfo(memory);
  api->ReleaseSession(session);
  api->ReleaseSessionOptions(options);
  api->ReleaseEnv(environment);
  if (correct) {
    std::fprintf(stderr, "%s passed\n", cuda ? "CUDA inference with CPU fallback disabled" :
                                         load_cuda ? "CUDA provider loading and CPU inference" : "CPU inference");
  }
  return correct ? 0 : 5;
}
