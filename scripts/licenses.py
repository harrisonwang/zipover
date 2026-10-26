#!/usr/bin/env python3
import json
from pathlib import Path
import subprocess
import sys
import urllib.error
import urllib.request


def upstream_license(package, directory):
    repository = package["repository"]
    repository = (repository or "").removesuffix(".git").rstrip("/")
    vcs = directory / ".cargo_vcs_info.json"
    if not repository.startswith("https://github.com/") or not vcs.exists():
        return None
    revision = json.loads(vcs.read_text(encoding="utf-8"))["git"]["sha1"]
    base = repository.replace("https://github.com/", "https://raw.githubusercontent.com/")
    for filename in ("LICENSE", "LICENSE.md", "LICENSE-MIT", "COPYING"):
        try:
            with urllib.request.urlopen(f"{base}/{revision}/{filename}", timeout=30) as response:
                return response.read().decode()
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
    return None


def main():
    target, output = sys.argv[1:]
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--locked", "--format-version", "1", "--filter-platform", target,
    ]))
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    root = next(key for key, package in packages.items() if package["name"] == "zipover-cli")
    pending, included = [root], set()
    while pending:
        package_id = pending.pop()
        if package_id not in included:
            included.add(package_id)
            pending.extend(dependency["pkg"] for dependency in nodes[package_id]["deps"]
                           if any(kind["kind"] != "dev" for kind in dependency["dep_kinds"]))
    blocks, missing = [], []
    for package in sorted((packages[key] for key in included), key=lambda item: item["name"]):
        if package["source"] is None:
            continue
        directory = Path(package["manifest_path"]).parent
        files = set()
        if package["license_file"]:
            files.add(directory / package["license_file"])
        for parent in (directory, directory.parent, directory.parent.parent):
            files.update(path for path in parent.iterdir() if path.is_file()
                         and path.name.upper().startswith(("LICENSE", "LICENCE", "COPYING", "NOTICE")))
            if files or package["source"].startswith("registry+"):
                break
        license_text = "\n".join(path.read_text(encoding="utf-8", errors="replace") for path in sorted(files))
        if not license_text:
            license_text = upstream_license(package, directory)
        if not license_text:
            missing.append(f"{package['name']} {package['version']}")
            continue
        header = f"{package['name']} {package['version']} ({package['license'] or 'see below'})"
        blocks.append(header + "\n" + "=" * len(header) + "\n" +
                      license_text)
    if missing:
        raise SystemExit("Missing dependency license files: " + ", ".join(missing))
    Path(output).write_text("\n\n".join(blocks) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
