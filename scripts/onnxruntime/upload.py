"""Report R2 transfer progress without contributing to runtime build inputs."""

from pathlib import Path
import sys
import threading


def upload_archive(client: object, archive: Path, bucket: str, key: str,
                   checksum: str, platform: str) -> None:
    from tqdm import tqdm

    with tqdm(total=archive.stat().st_size, desc=f"Uploading {platform}",
              unit="B", unit_scale=True, unit_divisor=1024,
              file=sys.stderr, dynamic_ncols=True,
              mininterval=0.2 if sys.stderr.isatty() else 5) as progress:
        mutex = threading.Lock()

        def transferred(size: int) -> None:
            # Multipart workers report byte deltas concurrently, including retry rollbacks.
            with mutex:
                progress.update(size)

        client.upload_file(str(archive), bucket, key,
                           ExtraArgs={"Metadata": {"sha256": checksum}},
                           Callback=transferred)
