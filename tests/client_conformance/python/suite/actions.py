"""The host suite's bounded request/confirmation channel to its scenario.

Requests share stdout with result lines; confirmations arrive on stdin. Only the scenario's
declared actions may be requested. A confirmation is sent after the action has been performed.
"""

from __future__ import annotations

import os
import selectors
import time


class SuitePipes:
    """Borrowed pipe descriptors whose copies the cluster runner owns until pytest ends."""

    def __init__(self, input_fd: int, output_fd: int) -> None:
        self.input_fd = input_fd
        self.output_fd = output_fd


class Harness:
    """One runner's sequential requests, numbered from one for its entire cluster tier."""

    def __init__(self, *, within: float, input_fd: int = 0, output_fd: int = 1) -> None:
        self.input_fd = input_fd
        self.output_fd = output_fd
        self.within = within
        self.sequence = 0

    def refuse_next_subscription_opening(self) -> None:
        """Arms every scenario node to refuse its next subscription opening as unavailable."""

        self._perform("refuse-next-subscription-opening")

    def clear_subscription_opening_refusals(self) -> None:
        """Clears unused refusals so the fault cannot reach another test."""

        self._perform("clear-subscription-opening-refusals")

    def _perform(self, action: str) -> None:
        self.sequence += 1
        # A framework may have left progress characters on the current output line.
        request = f"\nNERVIX-SUITE-ACTION {self.sequence} {action}\n".encode("ascii")
        expected = f"NERVIX-SUITE-CONFIRMED {self.sequence} {action}\n".encode("ascii")
        if os.write(self.output_fd, request) != len(request):
            raise AssertionError("the scenario action request was not written completely")
        ends = time.monotonic() + self.within
        received = bytearray()
        with selectors.DefaultSelector() as ready:
            try:
                ready.register(self.input_fd, selectors.EVENT_READ)
            except OSError as error:
                raise AssertionError("the scenario action channel is unavailable") from error
            while b"\n" not in received:
                remaining = ends - time.monotonic()
                if remaining <= 0 or not ready.select(remaining):
                    raise AssertionError(f"the scenario did not confirm {action} within {self.within}s")
                chunk = os.read(self.input_fd, 256)
                if not chunk:
                    raise AssertionError("the scenario closed its action channel without confirmation")
                received.extend(chunk)
                if len(received) > 256:
                    raise AssertionError("the scenario's action confirmation exceeds 256 bytes")
        if received != expected:
            raise AssertionError(f"unexpected scenario action confirmation: {bytes(received)!r}")
