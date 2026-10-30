"""Qualify the server-image deployment gate and its public observation oracles."""

from __future__ import annotations

import copy
import json
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import operator_deployment as deployment
from scripts.tests.test_release_evidence import workflow_job

ROOT = Path(__file__).resolve().parents[2]
REVISION = "0123456789abcdef0123456789abcdef01234567"
IMAGE = "ghcr.io/nervix-io/nervix@sha256:" + "a" * 64
SERVER_TAG = "ghcr.io/nervix-io/nervix:build"
OPERATOR_IMAGE = "ghcr.io/nervix-io/nervix-k8s-operator@sha256:" + "b" * 64


class ObservationTests(unittest.TestCase):
    def test_image_identity_requires_the_tested_revision_and_native_architecture(self) -> None:
        image = {
            "Architecture": "amd64", "RepoDigests": ["ghcr.io/nervix-io/nervix@sha256:" + "c" * 64, IMAGE],
            "Config": {"Labels": {"org.opencontainers.image.revision": REVISION}},
        }
        pull_output = "Digest: " + IMAGE.partition("@")[2] + "\n"
        self.assertEqual(deployment.server_identity([image], REVISION, "amd64", SERVER_TAG, pull_output), IMAGE)
        for revision, architecture in (("b" * 40, "amd64"), (REVISION, "arm64")):
            with self.subTest(revision=revision, architecture=architecture):
                with self.assertRaises(ValueError):
                    deployment.server_identity([image], revision, architecture, SERVER_TAG, pull_output)
        image["RepoDigests"] = []
        with self.assertRaisesRegex(ValueError, "digest"):
            deployment.server_identity([image], REVISION, "amd64", SERVER_TAG, pull_output)

    def test_the_pull_selects_its_registry_digest_among_cached_references(self) -> None:
        metadata = {"RepoDigests": ["ghcr.io/nervix-io/nervix@sha256:" + "c" * 64, IMAGE]}
        pull_output = "Digest: " + IMAGE.partition("@")[2] + "\n"
        for requested in (SERVER_TAG, IMAGE):
            self.assertEqual(deployment.image_reference(metadata, requested, pull_output), IMAGE)
        reference = "registry.example:5443/nervix@sha256:" + "a" * 64
        self.assertEqual(deployment.image_reference({"RepoDigests": [reference]},
                                                   "registry.example:5443/nervix:build", pull_output), reference)
        for output in ("pull incomplete", pull_output * 2, "Digest: sha256:" + "b" * 64 + "\n"):
            with self.subTest(output=output), self.assertRaisesRegex(ValueError, "digest"):
                deployment.image_reference(metadata, SERVER_TAG, output)
        with self.assertRaisesRegex(ValueError, "requested image digest"):
            deployment.image_reference(metadata, "ghcr.io/nervix-io/nervix@sha256:" + "b" * 64, pull_output)

    def test_the_shipped_example_keeps_every_field_except_the_tested_image(self) -> None:
        manifest = {"kind": "List", "items": [
            {"kind": "Namespace", "metadata": {"name": "nervix"}},
            {"kind": "Secret", "stringData": {"password": "nervix"}},
            {"kind": "NervixCluster", "spec": {
                "image": "ghcr.io/nervix-io/nervix:debian-latest", "replicas": 3,
                "storage": "20Gi", "resources": {"memoryLimit": "3Gi"},
                "localAccess": {"enabled": True, "bootstrapGrpcNodePort": 31390},
            }},
        ]}
        expected = copy.deepcopy(manifest)
        expected["apiVersion"] = "v1"
        expected["items"][2]["spec"]["image"] = IMAGE
        documents = "\n".join(json.dumps(item) for item in manifest["items"])
        self.assertEqual(deployment.with_server_image(documents, IMAGE), expected)
        for items in ([], manifest["items"] * 2):
            with self.subTest(items=items):
                with self.assertRaisesRegex(ValueError, "exactly one NervixCluster"):
                    deployment.with_server_image("\n".join(json.dumps(item) for item in items), IMAGE)

    def test_all_three_pods_must_be_ready_on_the_exact_image(self) -> None:
        pods = {"items": [{
            "metadata": {"name": f"nervix-{i}"},
            "spec": {"containers": [{"name": "nervix", "image": IMAGE}]},
            "status": {"conditions": [{"type": "Ready", "status": "True"}],
                       "containerStatuses": [{"name": "nervix", "ready": True, "imageID": IMAGE}]},
        } for i in range(3)]}
        self.assertTrue(deployment.pods_ready(pods, IMAGE))
        changes = [
            lambda p: p["items"].pop(),
            lambda p: p["items"][1]["spec"]["containers"][0].update(image="different"),
            lambda p: p["items"][1]["status"]["conditions"][0].update(status="False"),
            lambda p: p["items"][1]["status"]["containerStatuses"][0].update(ready=False),
            lambda p: p["items"][1]["status"]["containerStatuses"][0].update(imageID=""),
        ]
        for change in changes:
            changed = copy.deepcopy(pods)
            change(changed)
            self.assertFalse(deployment.pods_ready(changed, IMAGE))

    def test_cluster_status_requires_a_leader_and_three_voters(self) -> None:
        status = "[raft]\nraft.current_leader: node-1\nraft.membership:\n"
        status += "\n".join(f"- node-{i} [voter] endpoint-{i}" for i in range(1, 4))
        status += "\n[interconnect]\n"
        self.assertTrue(deployment.cluster_ready(status))
        self.assertFalse(deployment.cluster_ready(status.replace("node-1\n", "(none)\n")))
        self.assertFalse(deployment.cluster_ready(status.replace("node-3 [voter]", "node-3 [learner]")))
        self.assertFalse(deployment.cluster_ready("command failed"))

    def test_failure_keeps_diagnostics_and_deletes_only_the_owned_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            check = deployment.DeploymentCheck(IMAGE, REVISION, "amd64", Path(directory), Path(directory) / "report")
            with mock.patch.object(check, "deploy", side_effect=RuntimeError("rollout failed")), \
                 mock.patch.object(check, "diagnostics") as diagnostics, \
                 mock.patch.object(check, "cleanup") as cleanup:
                with self.assertRaisesRegex(RuntimeError, "rollout failed"):
                    check.execute()
            diagnostics.assert_called_once()
            cleanup.assert_called_once()
            self.assertEqual(json.loads((check.output / "result.json").read_text())["result"], "failed")


class WorkflowTests(unittest.TestCase):
    def test_publication_requires_the_operator_check_for_every_built_architecture(self) -> None:
        workflow = (ROOT / ".github/workflows/docker-build.yaml").read_text()
        build = workflow_job(workflow, "build-arch")
        check = workflow_job(workflow, "operator-deployment")
        publish = workflow_job(workflow, "publish-manifest")
        built = re.findall(r"^\s+arch: (\S+)$", build, re.M)
        tested = re.findall(r"^\s+- arch: (\S+)$", check, re.M)
        self.assertEqual(sorted(built), sorted(tested))
        self.assertIn("needs: [meta, build-arch, operator-deployment]", publish)
        self.assertIn("needs: [meta, build-arch]", check)
        self.assertIn("ghcr.io/nervix-io/nervix:${{ needs.meta.outputs.build_id }}-debian-${{ matrix.arch }}", check)
        self.assertIn("nervix-io/nervix-k8s-operator", check)
        self.assertIn("ref: main", check)
        self.assertIn("just operator-deployment-check", check)
        version = re.search(r"^\s+minikube-version: (\S+)$", check, re.M)
        self.assertIsNotNone(version)
        self.assertRegex(version[1], r"^\d+\.\d+\.\d+$")
        self.assertNotIn("continue-on-error", check)
        self.assertIn("if: always()", check)
        self.assertIn("./.github/actions/ci-artifact-upload", check)


class ControllerTests(unittest.TestCase):
    def test_the_controller_deploys_the_resolved_image_and_observes_public_membership(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            check = deployment.DeploymentCheck(SERVER_TAG, REVISION, "amd64", Path(directory), Path(directory) / "report")
            calls = []

            def command(*args, **_kwargs):
                calls.append(args)
                output = ""
                if args[:2] == ("docker", "pull"):
                    selected = IMAGE if args[2] == SERVER_TAG else OPERATOR_IMAGE
                    output = "Digest: " + selected.partition("@")[2] + "\n"
                elif args[:3] == ("docker", "image", "inspect"):
                    metadata = {"RepoDigests": [IMAGE], "Architecture": "amd64",
                                "Config": {"Labels": {"org.opencontainers.image.revision": REVISION}}}
                    if args[3] != SERVER_TAG:
                        metadata["RepoDigests"] = [
                            "ghcr.io/nervix-io/nervix-k8s-operator@sha256:" + "c" * 64,
                            "ghcr.io/nervix-io/nervix-k8s-operator@sha256:" + "d" * 64,
                            OPERATOR_IMAGE,
                        ]
                    output = json.dumps([metadata])
                elif args[0] == "git":
                    output = "c" * 40
                elif "--dry-run=client" in args:
                    output = json.dumps({"kind": "NervixCluster", "spec": {"image": "server:current"}})
                elif "nervixcluster/nervix" in args:
                    output = json.dumps({"status": {"initializationPhase": "initialized"}})
                elif "pods" in args and args[-1] == "json":
                    output = json.dumps({"items": [{
                        "metadata": {"name": f"nervix-{i}"},
                        "spec": {"containers": [{"image": IMAGE}]},
                        "status": {"conditions": [{"type": "Ready", "status": "True"}],
                                   "containerStatuses": [{"ready": True, "imageID": IMAGE}]},
                    } for i in range(3)]})
                elif args[-1] == "ip":
                    output = "192.168.49.2\n"
                elif args[:2] == ("docker", "run"):
                    output = "[raft]\nraft.current_leader: node-1\n"
                    output += "\n".join(f"- node-{i} [voter] endpoint-{i}" for i in range(1, 4))
                    output += "\n[interconnect]\n"
                return subprocess.CompletedProcess(args, 0, output, "")

            def observed(description, observation, _timeout):
                self.assertTrue(observation(), description)

            with mock.patch.object(check, "command", side_effect=command), \
                 mock.patch.object(check, "wait", side_effect=observed):
                check.execute()
            self.assertEqual(json.loads((check.output / "result.json").read_text())["result"], "passed")
            self.assertEqual(json.loads((check.output / "deployment.json").read_text())["items"][0]["spec"]["image"], IMAGE)
            identity = json.loads((check.output / "images.json").read_text())
            self.assertEqual(identity["operator_source_revision"], "c" * 40)
            self.assertEqual(identity["server"], IMAGE)
            self.assertEqual(identity["operator"], OPERATOR_IMAGE)
            cli, = [args for args in calls if args[:2] == ("docker", "run")]
            self.assertIn(IMAGE, cli)
            self.assertIn("/usr/local/bin/nervix-cli", cli)
            self.assertIn("http://192.168.49.2:31390", cli)
            self.assertEqual(calls[-1], ("minikube", "delete", "-p", check.profile))
            helm, = [args for args in calls if args[0] == "helm"]
            self.assertIn("image.tag=latest@sha256:" + "b" * 64, helm)
            start, = [args for args in calls if args[:2] == ("minikube", "start")]
            self.assertIn("--keep-context", start)
            self.assertTrue(all(check.profile in args for args in calls if args[0] == "kubectl"))

    def test_cleanup_and_diagnostic_failures_cannot_leave_a_passing_result(self) -> None:
        for owner in ("diagnostics", "cleanup"):
            with self.subTest(owner=owner), tempfile.TemporaryDirectory() as directory:
                check = deployment.DeploymentCheck(IMAGE, REVISION, "amd64", Path(directory), Path(directory) / "report")
                with mock.patch.object(check, "deploy"), mock.patch.object(check, "diagnostics"), \
                     mock.patch.object(check, "cleanup"), \
                     mock.patch.object(check, owner, side_effect=RuntimeError("collection failed")):
                    with self.assertRaisesRegex(RuntimeError, "collection failed"):
                        check.execute()
                self.assertEqual(json.loads((check.output / "result.json").read_text())["result"], "failed")

    def test_subprocess_failures_and_timeouts_retain_their_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            check = deployment.DeploymentCheck(IMAGE, REVISION, "amd64", Path(directory), Path(directory))
            failure = subprocess.CompletedProcess(["helm"], 1, "observed output", "rollout failed")
            with mock.patch("subprocess.run", return_value=failure):
                with self.assertRaises(subprocess.CalledProcessError):
                    check.command("helm", "install")
                self.assertEqual(check.command("helm", "status", required=False).returncode, 1)
            with mock.patch("subprocess.run", side_effect=subprocess.TimeoutExpired("helm", 2, "partial output")):
                with self.assertRaises(subprocess.TimeoutExpired):
                    check.command("helm", "install", timeout=2)
            log = (check.output / "commands.log").read_text()
            self.assertIn("observed output", log)
            self.assertIn("rollout failed", log)
            self.assertIn("partial output", log)

    def test_pending_observations_end_at_the_harness_bound(self) -> None:
        check = deployment.DeploymentCheck(IMAGE, REVISION, "amd64", Path("."), Path("target/report"))
        with mock.patch("time.monotonic", side_effect=[0, 1, 2]), mock.patch("time.sleep"):
            with self.assertRaisesRegex(TimeoutError, "initialization did not complete"):
                check.wait("initialization", lambda: False, 2)


if __name__ == "__main__":
    unittest.main()
