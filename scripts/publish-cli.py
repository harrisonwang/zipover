#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

REPOSITORY = "harrisonwang/zipover"
TARGETS = {
    "aarch64-apple-darwin": "tar.gz",
    "x86_64-apple-darwin": "tar.gz",
    "x86_64-unknown-linux-gnu": "tar.gz",
    "x86_64-pc-windows-msvc": "exe",
}


def gh(*args, payload=None):
    return subprocess.run(
        ["gh", *args], input=json.dumps(payload) if payload is not None else None,
        text=True, check=True, capture_output=True,
    ).stdout


def digest(path):
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            checksum.update(chunk)
    return checksum.hexdigest()


def main():
    tag = os.environ["GITHUB_REF_NAME"]
    if not re.fullmatch(r"v\d+\.\d+\.\d+", tag):
        raise ValueError("Expected a stable vX.Y.Z release tag")
    directory = Path("artifacts")
    expected = {"LICENSE"} | {
        f"zipover_{tag}_{target}.{extension}" for target, extension in TARGETS.items()
    } | {f"THIRD-PARTY-NOTICES_{target}.txt" for target in TARGETS}
    if {path.name for path in directory.iterdir()} - {"SHA256SUMS.txt"} != expected:
        raise ValueError("Release assets do not match the CLI build targets")
    if any(not path.is_file() or path.is_symlink() for path in directory.iterdir()):
        raise ValueError("Release assets must be regular files")
    hashes = {name: digest(directory / name) for name in sorted(expected)}
    (directory / "SHA256SUMS.txt").write_text("".join(
        f"{checksum}  {name}\n" for name, checksum in hashes.items()
    ))
    expected.add("SHA256SUMS.txt")
    pages = json.loads(gh("api", "--paginate", "--slurp", f"repos/{REPOSITORY}/releases"))
    release = next((r for page in pages for r in page if r["tag_name"] == tag), None)
    if release is None:
        gh("api", f"repos/{REPOSITORY}/git/ref/tags/{tag}")
        release = json.loads(gh("api", "--method", "POST", f"repos/{REPOSITORY}/releases",
                                "--input", "-", payload={
                                    "tag_name": tag, "draft": True,
                                    "name": f"Zipover CLI {tag}", "body": f"Zipover CLI {tag}.",
                                }))
    existing = {asset["name"] for asset in release["assets"]}
    if existing - expected:
        raise ValueError("Existing release contains unexpected assets")
    with tempfile.TemporaryDirectory() as temp:
        for name in sorted(expected):
            if name not in existing:
                if not release["draft"]:
                    raise ValueError(f"Published release is missing {name}")
                gh("release", "upload", tag, str(directory / name), "--repo", REPOSITORY)
            gh("release", "download", tag, "--repo", REPOSITORY, "--pattern", name, "--dir", temp)
            if digest(directory / name) != digest(Path(temp) / name):
                raise ValueError(f"Release asset differs; refusing replacement: {name}")
    versions = [tuple(map(int, item["tag_name"][1:].split(".")))
                for page in pages for item in page
                if not item["draft"] and re.fullmatch(r"v\d+\.\d+\.\d+", item["tag_name"])]
    current = tuple(map(int, tag[1:].split(".")))
    latest = not versions or current >= max(versions)
    gh("release", "edit", tag, "--repo", REPOSITORY, "--draft=false",
       f"--latest={str(latest).lower()}")
    if "GITHUB_OUTPUT" in os.environ:
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            output.write(f"latest={str(latest).lower()}\n")


if __name__ == "__main__":
    main()
