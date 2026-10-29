"""Deploy the shipped Kubernetes operator against the server image being qualified.

The operator's main-branch chart and example remain the deployment owners. This check replaces
only the example's server image, records both resolved images, observes initialization, three
ready pods and public Raft membership, and collects diagnostics before deleting its own profile.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shlex
import signal
import subprocess
import time
import uuid


INDEX_MEDIA_TYPES = frozenset({
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
})


def repository_name(requested: str) -> str:
    repository = requested.partition("@")[0]
    if ":" in repository.rsplit("/", 1)[-1]:
        repository = repository.rsplit(":", 1)[0]
    return repository


def platform_manifest(document: dict, architecture: str) -> str:
    # A per-architecture tag names an index of the platform's manifest and its provenance
    # attestation. Its own manifest's digest makes every pull select the tested architecture, also
    # on a node of another architecture, where the index would resolve to the node's platform.
    if document["mediaType"] not in INDEX_MEDIA_TYPES:
        # A single manifest names its platform only in its configuration, which the inspected
        # image's architecture check reads after the pull.
        return document["digest"]
    selected = []
    for manifest in document["manifests"]:
        platform = manifest.get("platform", {})
        if platform.get("os") == "linux" and platform.get("architecture") == architecture:
            selected.append(manifest["digest"])
    if len(selected) != 1:
        raise ValueError(f"image index must hold exactly one linux/{architecture} manifest")
    return selected[0]


def image_reference(metadata: dict, requested: str, pull_output: str) -> str:
    # Cached tags can leave several registry digests on one image. The pull identifies which
    # digest the requested reference selected; its position in RepoDigests has no meaning.
    digests = re.findall(r"^Digest: (sha256:[0-9a-f]{64})$", pull_output, re.M)
    if len(digests) != 1:
        raise ValueError("image pull must report exactly one registry digest")
    _, marker, specified = requested.partition("@")
    if marker and specified != digests[0]:
        raise ValueError("pulled registry digest does not match the requested image digest")
    reference = f"{repository_name(requested)}@{digests[0]}"
    if reference not in metadata["RepoDigests"]:
        raise ValueError("pulled registry digest is missing from the inspected image")
    return reference


def server_identity(images: list[dict], revision: str, architecture: str, requested: str, pull_output: str) -> str:
    image, = images
    actual = image["Config"]["Labels"].get("org.opencontainers.image.revision")
    if actual != revision or image["Architecture"] != architecture:
        raise ValueError(f"server image is {actual}/{image['Architecture']}, expected {revision}/{architecture}")
    return image_reference(image, requested, pull_output)


def with_server_image(documents: str, image: str) -> dict:
    # kubectl's client dry run prints each resource as its own JSON document. Construct a List
    # only after consuming that complete stream; every original resource and field stays intact.
    decoder = json.JSONDecoder()
    items = []
    remaining = documents.lstrip()
    while remaining:
        item, end = decoder.raw_decode(remaining)
        items.append(item)
        remaining = remaining[end:].lstrip()
    clusters = [item for item in items if item["kind"] == "NervixCluster"]
    if len(clusters) != 1:
        raise ValueError("the shipped example must contain exactly one NervixCluster")
    clusters[0]["spec"]["image"] = image
    return {"apiVersion": "v1", "kind": "List", "items": items}


def pods_ready(pods: dict, image: str) -> bool:
    items = pods["items"]
    if len(items) != 3 or {pod["metadata"]["name"] for pod in items} != {"nervix-0", "nervix-1", "nervix-2"}:
        return False
    for pod in items:
        containers = pod["spec"]["containers"]
        statuses = pod.get("status", {}).get("containerStatuses", [])
        conditions = pod.get("status", {}).get("conditions", [])
        if len(containers) != 1 or containers[0]["image"] != image:
            return False
        if len(statuses) != 1 or not statuses[0].get("ready") or not statuses[0].get("imageID"):
            return False
        if not any(condition["type"] == "Ready" and condition["status"] == "True" for condition in conditions):
            return False
    return True


def cluster_ready(status: str) -> bool:
    raft = re.search(r"\[raft\]\n(.*?)\n\[interconnect\]", status, re.S)
    if raft is None:
        return False
    text = raft.group(1)
    leader = re.search(r"^raft.current_leader: (\S+)$", text, re.M)
    voters = re.findall(r"^- (\S+) \[voter\] \S+$", text, re.M)
    return leader is not None and len(set(voters)) == 3 and leader.group(1) in voters


class DeploymentCheck:
    def __init__(self, image: str, revision: str, architecture: str, source: Path, output: Path):
        self.image = image
        self.revision = revision
        self.architecture = architecture
        self.source = source.resolve()
        self.output = output.resolve()
        self.profile = f"nervix-operator-{uuid.uuid4().hex[:12]}"
        self.started = False
        self.cli_container = f"{self.profile}-cli"

    def command(self, *args: str, timeout: int = 120, required: bool = True) -> subprocess.CompletedProcess:
        print(shlex.join(args), flush=True)
        with (self.output / "commands.log").open("a") as log:
            log.write(f"\n$ {shlex.join(args)}\n")
            try:
                result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
            except subprocess.TimeoutExpired as error:
                log.write(f"timed out after {timeout}s\n{error.stdout!r}\n{error.stderr!r}\n")
                raise
            log.write(result.stdout)
            log.write(result.stderr)
            log.write(f"\nexit: {result.returncode}\n")
        if required:
            result.check_returncode()
        return result

    def kubectl(self, *args: str, **kwargs) -> subprocess.CompletedProcess:
        return self.command("kubectl", "--context", self.profile, "--request-timeout=30s", *args, **kwargs)

    def save(self, name: str, value: object) -> None:
        (self.output / name).write_text(json.dumps(value, indent=2) + "\n")

    def wait(self, description: str, observation, timeout: int) -> None:
        deadline = time.monotonic() + timeout
        while True:
            if observation():
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{description} did not complete within {timeout}s; see {self.output}")
            time.sleep(2)

    def initialized(self) -> bool:
        cluster = json.loads(self.kubectl("-n", "nervix", "get", "nervixcluster/nervix", "-o", "json").stdout)
        self.save("cluster.json", cluster)
        return cluster.get("status", {}).get("initializationPhase") == "initialized"

    def ready(self, image: str) -> bool:
        pods = json.loads(self.kubectl("-n", "nervix", "get", "pods", "-o", "json").stdout)
        self.save("pods.json", pods)
        return pods_ready(pods, image)

    def public_status(self, image: str, endpoint: str) -> bool:
        status = self.command(
            "docker", "run", "--rm", "--name", self.cli_container, "--network", "host",
            "--platform", f"linux/{self.architecture}",
            "--entrypoint", "/usr/local/bin/nervix-cli", image, "--server", endpoint,
            "--password", "nervix", "--command", "SHOW CLUSTER STATUS;", timeout=45, required=False,
        )
        (self.output / "cluster-status.txt").write_text(status.stdout + status.stderr)
        return status.returncode == 0 and cluster_ready(status.stdout)

    def deploy(self) -> None:
        index = self.command("docker", "buildx", "imagetools", "inspect", self.image,
                             "--format", "{{json .Manifest}}", timeout=120)
        digest = platform_manifest(json.loads(index.stdout), self.architecture)
        manifest = f"{repository_name(self.image)}@{digest}"
        server_pull = self.command("docker", "pull", "--platform", f"linux/{self.architecture}", manifest,
                                   timeout=600)
        server = json.loads(self.command("docker", "image", "inspect", manifest).stdout)
        image = server_identity(server, self.revision, self.architecture, manifest, server_pull.stdout)
        operator = "ghcr.io/nervix-io/nervix-k8s-operator:latest"
        operator_pull = self.command("docker", "pull", operator, timeout=600)
        operator_metadata = json.loads(self.command("docker", "image", "inspect", operator).stdout)
        operator_digest = image_reference(operator_metadata[0], operator, operator_pull.stdout)
        repository, digest = operator_digest.split("@")
        source_revision = self.command("git", "-C", str(self.source), "rev-parse", "HEAD").stdout.strip()
        self.save("images.json", {
            "server": image, "server_revision": self.revision, "architecture": self.architecture,
            "operator": operator_digest, "operator_source_revision": source_revision,
            "server_metadata": server, "operator_metadata": operator_metadata,
        })
        # The profile has a random name and is owned from the first start attempt, including a
        # partially failed start. Every Kubernetes command explicitly names this context.
        self.started = True
        # The node's Docker runtime pulls a manifest of another architecture, which containerd,
        # minikube's default, refuses; the host's QEMU registration then runs an arm64 image on an
        # amd64 node.
        self.command("minikube", "start", "-p", self.profile, "--driver=docker", "--container-runtime=docker",
                     "--keep-context", "--kubernetes-version=v1.34.0", "--cpus=4", "--memory=10240", timeout=600)
        self.command("helm", "upgrade", "--install", "nervix-k8s-operator",
                     str(self.source / "charts/nervix-k8s-operator"), "--kube-context", self.profile,
                     "--namespace", "nervix-system", "--create-namespace", "--wait", "--timeout", "180s",
                     "--set-string", f"image.repository={repository}",
                     "--set-string", f"image.tag=latest@{digest}", timeout=210)
        example = self.kubectl("create", "--dry-run=client", "--validate=false", "-f",
                               str(self.source / "examples/nervix-cluster.yaml"), "-o", "json")
        manifest = with_server_image(example.stdout, image)
        self.save("deployment.json", manifest)
        self.kubectl("apply", "-f", str(self.output / "deployment.json"))
        self.wait("operator initialization", self.initialized, 300)
        self.kubectl("-n", "nervix", "rollout", "status", "statefulset/nervix", "--timeout=300s", timeout=330)
        self.wait("three ready pods on the tested image", lambda: self.ready(image), 120)
        address = self.command("minikube", "-p", self.profile, "ip").stdout.strip()
        self.wait("public three-voter cluster status", lambda: self.public_status(image, f"http://{address}:31390"), 120)

    def diagnostics(self) -> None:
        if not self.started:
            return
        for namespace in ("nervix", "nervix-system"):
            result = self.kubectl("-n", namespace, "get", "pods,svc,statefulsets,pvc,nervixclusters,events",
                                  "-o", "yaml", required=False)
            (self.output / f"{namespace}-state.yaml").write_text(result.stdout + result.stderr)
            pods = self.kubectl("-n", namespace, "get", "pods", "-o", "name", required=False)
            for pod in pods.stdout.splitlines():
                for previous in (False, True):
                    args = ["-n", namespace, "logs", pod, "--all-containers=true", "--tail=1000"]
                    if previous:
                        args.append("--previous")
                    logs = self.kubectl(*args, required=False)
                    suffix = "-previous" if previous else ""
                    name = pod.replace("/", "-")
                    (self.output / f"{namespace}-{name}{suffix}.log").write_text(logs.stdout + logs.stderr)

    def cleanup(self) -> None:
        self.command("docker", "rm", "-f", self.cli_container, required=False)
        if self.started:
            self.command("minikube", "delete", "-p", self.profile, timeout=180)

    def execute(self) -> None:
        self.output.mkdir(parents=True, exist_ok=False)
        result = {"result": "failed", "profile": self.profile, "revision": self.revision}
        try:
            self.deploy()
            result["result"] = "passed"
        finally:
            try:
                self.diagnostics()
            except BaseException:
                result["result"] = "failed"
                raise
            finally:
                try:
                    self.cleanup()
                except BaseException:
                    result["result"] = "failed"
                    raise
                finally:
                    self.save("result.json", result)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--architecture", required=True, choices=("amd64", "arm64"))
    parser.add_argument("--operator-source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    def interrupted(signum, _frame):
        raise InterruptedError(f"deployment check received signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    DeploymentCheck(args.image, args.revision, args.architecture, args.operator_source, args.output).execute()


if __name__ == "__main__":
    main()
