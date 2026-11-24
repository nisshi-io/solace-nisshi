#!/usr/bin/env python3
"""Fails when a Cargo.lock moves a package or a dependency edge to a lower version.

`cargo update -p <crate>` can re-point crates that it was not asked to update at older
copies already in the lockfile, and it reports none of it
(https://github.com/rust-lang/cargo/issues/14446). Dependabot runs the same command.

Usage: check-lock-downgrade.py BASE_LOCK HEAD_LOCK
       check-lock-downgrade.py --restore BASE_LOCK HEAD_LOCK
Env:   PR_BODY  the pull request description. Each `allow-lock-downgrade: <crate>` line in
                it accepts every downgrade of that crate.
Exit:  0 no unacknowledged downgrade, 1 at least one, 2 the check could not run.

The check prints one line per unacknowledged downgrade on stdout, and nothing else.
`--restore` rewrites HEAD_LOCK: it points each downgraded edge back at the base version, if
HEAD_LOCK still holds that copy. A downgrade whose base copy is gone needs a person.
Test:  python3 -I .github/scripts/check-lock-downgrade.test.py
"""

import os
import re
import sys
from collections import defaultdict

try:
    import tomllib
except ImportError:
    # Python exits 1 on an uncaught error, and 1 means "downgrade found".
    print("::error::check-lock-downgrade.py needs Python 3.11 or later", file=sys.stderr)
    sys.exit(2)

ALLOW = re.compile(r"^[ \t]*allow-lock-downgrade:[ \t]*([A-Za-z0-9_-]+)[ \t]*$", re.M)


def version_key(version):
    """Returns a sort key that orders versions by semver precedence."""
    core, _, pre = version.split("+")[0].partition("-")
    numbers = tuple(int(n) for n in core.split("."))
    if not pre:
        # A release sorts above each of its pre-releases.
        return numbers, (1,)
    return numbers, (0, tuple((0, int(p), "") if p.isdigit() else (1, 0, p) for p in pre.split(".")))


def semver_line(version):
    """Returns the semver-compatible line: 1.4.2 is in 1, 0.61.2 in 0.61, 0.0.3 in 0.0.3."""
    major, minor, patch = version.split("+")[0].split("-")[0].split(".")
    if major != "0":
        return major
    return f"0.{minor}" if minor != "0" else f"0.0.{patch}"


def owner_key(package):
    """Returns the key that matches one package in the base and the head lockfile.

    Cargo allows one version per semver line and source, so the key is unique in a lockfile
    and stays the same when the package's version moves within its line.
    """
    return package["name"], semver_line(package["version"]), package.get("source", "")


def load(path):
    """Returns the lockfile's versions of each crate, and the dependency edges of each package."""
    with open(path, "rb") as f:
        packages = tomllib.load(f).get("package", [])
    versions = defaultdict(set)
    for package in packages:
        versions[package["name"]].add(package["version"])
    edges = {}
    for package in packages:
        deps = defaultdict(list)
        for dep in package.get("dependencies", []):
            name, _, rest = dep.partition(" ")
            if rest:
                deps[name].append(rest.split(" ")[0])
            else:
                # Cargo writes a bare name when the lockfile holds one version of the crate.
                (only,) = versions[name]
                deps[name].append(only)
        edges[owner_key(package)] = deps
    return versions, edges


def newest(versions):
    return max(versions, key=version_key)


def downgrades(base, head):
    """Returns (crate, message) for each downgrade from the base to the head lockfile."""
    (base_versions, base_edges), (head_versions, head_edges) = base, head
    found = set()
    for name in base_versions.keys() & head_versions.keys():
        before, after = newest(base_versions[name]), newest(head_versions[name])
        if version_key(after) < version_key(before):
            found.add((name, f"{name}: {before} -> {after}"))
            continue
        lines = {semver_line(v) for v in base_versions[name]} & {semver_line(v) for v in head_versions[name]}
        for line in lines:
            before = newest(v for v in base_versions[name] if semver_line(v) == line)
            after = newest(v for v in head_versions[name] if semver_line(v) == line)
            if version_key(after) < version_key(before):
                found.add((name, f"{name}: {before} -> {after}"))
    for owner in base_edges.keys() & head_edges.keys():
        for dep, before, after in edge_downgrades(base_edges[owner], head_edges[owner]):
            found.add((dep, f"{dep}: {before} -> {after} (edge from {owner[0]})"))
    return sorted(found, key=lambda f: f[1])


def edge_downgrades(base_deps, head_deps):
    """Yields (crate, base version, head version) for each edge of one package that moved down.

    A package can depend on two versions of one crate under two names. Its edges to that crate
    pair up in version order. When their count changed, only the newest edges compare.
    """
    for dep in sorted(base_deps.keys() & head_deps.keys()):
        before = sorted(base_deps[dep], key=version_key)
        after = sorted(head_deps[dep], key=version_key)
        pairs = zip(before, after) if len(before) == len(after) else [(before[-1], after[-1])]
        for b, h in pairs:
            if version_key(h) < version_key(b):
                yield dep, b, h


def restore(base_path, head_path):
    """Points each downgraded edge in the head lockfile back at the base version."""
    _, base_edges = load(base_path)
    head_versions, head_edges = load(head_path)
    with open(head_path, newline="") as f:
        text = f.read()
    # The edit works on the text, not on parsed TOML, so that every other byte stays as cargo
    # wrote it.
    blocks = text.split("\n[[package]]\n")
    for i, block in enumerate(blocks[1:], 1):
        package = tomllib.loads(block)
        owner = owner_key(package)
        if owner not in base_edges:
            continue
        for dep, before, after in edge_downgrades(base_edges[owner], head_edges[owner]):
            if before in head_versions[dep]:
                edge = re.compile(rf'^ "{re.escape(dep)} {re.escape(after)}( \([^)]*\))?",$', re.M)
                block = edge.sub(lambda m: f' "{dep} {before}{m.group(1) or ""}",', block, count=1)
        blocks[i] = block
    with open(head_path, "w", newline="") as f:
        f.write("\n[[package]]\n".join(blocks))


def main(argv):
    if len(argv) == 4 and argv[1] == "--restore":
        try:
            restore(argv[2], argv[3])
        except Exception as e:
            print(f"::error::check-lock-downgrade.py cannot restore the edges: {e!r}", file=sys.stderr)
            return 2
        return 0
    if len(argv) != 3:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    try:
        found = downgrades(load(argv[1]), load(argv[2]))
    except Exception as e:
        # Any failure is "could not run" (2), never "downgrade found" (1).
        print(f"::error::check-lock-downgrade.py cannot compare the lockfiles: {e!r}", file=sys.stderr)
        return 2
    allowed = set(ALLOW.findall(os.environ.get("PR_BODY", "").replace("\r", "")))
    refused = [message for crate, message in found if crate not in allowed]
    for message in refused:
        print(message)
    return 1 if refused else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
