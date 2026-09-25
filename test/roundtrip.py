#!/usr/bin/env python3
"""Downloads a medium-sized Git repo, archives it, restores it, and compares the two file trees"""

import hashlib
import os
import shutil
import socket
import stat
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig

TEST_REPO = "https://github.com/postgres/postgres"
TEST_REPO_REF = "REL_18_6"

S3_HOST = "127.0.0.1"
S3_PORT = 5000
S3_ENDPOINT = f"http://{S3_HOST}:{S3_PORT}"
S3_BUCKET = "cubist"

WORK_DIR = Path("/tmp/cubist-roundtrip")
SRC_DIR = WORK_DIR / "src"
RESTORED_DIR = WORK_DIR / "src-restored"

SHA256_CHUNK_SIZE = 1024 * 1024


def main() -> None:
    os.environ.update(
        {
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_REGION": "us-east-1",
            "AWS_REQUEST_CHECKSUM_CALCULATION": "when_required",
            "AWS_RESPONSE_CHECKSUM_VALIDATION": "when_required",
            "CUBIST_BUCKET": S3_BUCKET,
            "S3_IGNORE_SUBDOMAIN_BUCKETNAME": "true",
        }
    )

    WORK_DIR.mkdir(parents=True)

    print("Starting S3 mock...")
    moto = subprocess.Popen(
        ["moto_server", "-H", S3_HOST, "-p", str(S3_PORT)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        wait_for_port()
        create_bucket()

        print("Cloning test repo...")
        clone_test_repo()

        print("Creating archive...")
        archive = backup(SRC_DIR)

        print("Restoring archive...")
        restore(archive, RESTORED_DIR)

        print("Comparing files...")
        compare(SRC_DIR, RESTORED_DIR)
    finally:
        moto.terminate()

        try:
            moto.wait(timeout=5)
        except subprocess.TimeoutExpired:
            moto.kill()


def wait_for_port() -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", 5000), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)

    sys.exit("moto did not start listening on port 5000")


def create_bucket() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name="us-east-1",
        config=BotoConfig(s3={"addressing_style": "path"}),
    )
    client.create_bucket(Bucket=S3_BUCKET)


def clone_test_repo() -> None:
    subprocess.run(
        [
            "git",
            "clone",
            TEST_REPO,
            str(SRC_DIR),
            "--branch",
            TEST_REPO_REF,
            "--quiet",
            "--depth",
            "1",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    shutil.rmtree(SRC_DIR / ".git")


def backup(path: Path) -> str:
    output = cubist("backup", "--color", "never", str(path))
    marker = "created archive "
    for line in output.splitlines():
        if marker in line:
            return line.removeprefix(marker).strip()

    sys.exit("backup did not report an archive hash")


def restore(archive: str, destination: Path) -> None:
    destination.mkdir()
    cubist("restore", "--color", "never", archive, cwd=destination)


def cubist(*args: str, cwd: Path | None = None) -> str:
    result = subprocess.run(
        ["cubist", *args],
        cwd=cwd,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        sys.exit(result.stdout)

    return result.stdout


def compare(source: Path, restored: Path) -> None:
    source_entries = manifest(source)
    restored_entries = manifest(restored)
    source_paths = set(source_entries)
    restored_paths = set(restored_entries)
    missing = sorted(source_paths - restored_paths)
    extra = sorted(restored_paths - source_paths)
    if missing or extra:
        sys.exit(
            "path mismatch\n"
            f"missing from restore ({len(missing)}): {missing[:20]}\n"
            f"extra in restore ({len(extra)}): {extra[:20]}"
        )

    mismatches = []
    for path, original in source_entries.items():
        if original != restored_entries[path]:
            mismatches.append(
                f"{path}\n  source:   {original}\n  restored: {restored_entries[path]}"
            )
            if len(mismatches) >= 20:
                break

    if mismatches:
        sys.exit("metadata or content mismatch\n" + "\n".join(mismatches))

    print(f"OK: {len(source_entries)} paths match")


def manifest(root: Path) -> dict[str, str]:
    entries = {}

    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        current = Path(dirpath)
        for name in dirnames + filenames:
            path = current / name
            relative = path.relative_to(root).as_posix()
            entries[relative] = describe(path)

        dirnames[:] = [name for name in dirnames if not (current / name).is_symlink()]

    return entries


def describe(path: Path) -> str:
    info = path.lstat()
    kind = stat.S_IFMT(info.st_mode)
    mode = stat.S_IMODE(info.st_mode)
    fields = [
        f"type={kind:o}",
        f"mode={mode:04o}",
        f"uid={info.st_uid}",
        f"gid={info.st_gid}",
    ]

    if stat.S_ISLNK(info.st_mode):
        fields.append(f"target={os.readlink(path)}")
    elif stat.S_ISREG(info.st_mode):
        fields.append(f"sha256={sha256(path)}")

    return " ".join(fields)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while chunk := file.read(SHA256_CHUNK_SIZE):
            digest.update(chunk)

    return digest.hexdigest()


if __name__ == "__main__":
    main()
