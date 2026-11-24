#!/usr/bin/env python3
"""Fails when a Cargo.lock moves a package or a dependency edge to a lower version.

`cargo update -p <crate>` can re-point crates that it was not asked to update at older
copies already in the lockfile, and it reports none of it
(https://github.com/rust-lang/cargo/issues/14446). Dependabot runs the same command.

Usage: check-lock-downgrade.py BASE_LOCK HEAD_LOCK
Exit:  0 no downgrade, 1 at least one, 2 the check could not run.

The check prints one line per downgrade on stdout, and nothing else.
Test:  python3 -I .github/scripts/check-lock-downgrade.test.py
"""

import sys
from collections import defaultdict
import re

try:
    import tomllib
except ImportError:
    # Python exits 1 on an uncaught error, and 1 means "downgrade found".
    print("::error::check-lock-downgrade.py needs Python 3.11 or later", file=sys.stderr)
    sys.exit(2)

# The check prints names and versions from the head lockfile, which the pull request
# controls. These patterns reject any other character, so that a printed line cannot hold a
# line break and reach the runner as a workflow command.
NAME = re.compile(r"[A-Za-z0-9_-]+")
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?")


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


def checked(pattern, value):
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValueError(f"invalid name or version in the lockfile: {value!r}")
    return value


def load(path):
    """Returns the lockfile's versions of each crate, and the dependency edges of each package."""
    with open(path, "rb") as f:
        packages = tomllib.load(f).get("package", [])
    versions = defaultdict(set)
    for package in packages:
        versions[checked(NAME, package["name"])].add(checked(VERSION, package["version"]))
    edges = {}
    for package in packages:
        deps = defaultdict(list)
        for dep in package.get("dependencies", []):
            name, _, rest = dep.partition(" ")
            checked(NAME, name)
            if rest:
                deps[name].append(checked(VERSION, rest.split(" ")[0]))
            else:
                # Cargo writes a bare name when the lockfile holds one version of the crate.
                (only,) = versions[name]
                deps[name].append(only)
        edges[owner_key(package)] = deps
    return versions, edges


def newest(versions):
    return max(versions, key=version_key)


def downgrades(base, head):
    """Returns one sorted message for each downgrade from the base to the head lockfile."""
    (base_versions, base_edges), (head_versions, head_edges) = base, head
    found = set()
    for name in base_versions.keys() & head_versions.keys():
        before, after = newest(base_versions[name]), newest(head_versions[name])
        if version_key(after) < version_key(before):
            found.add(f"{name}: {before} -> {after}")
            continue
        for before, after in by_line(base_versions[name], head_versions[name]):
            if version_key(after) < version_key(before):
                found.add(f"{name}: {before} -> {after}")
    for owner in base_edges.keys() & head_edges.keys():
        for dep, before, after in edge_downgrades(base_edges[owner], head_edges[owner]):
            found.add(f"{dep}: {before} -> {after} (edge from {owner[0]})")
    return sorted(found)


def by_line(before, after):
    """Yields (newest base version, newest head version) for each semver line that both hold."""
    for line in {semver_line(v) for v in before} & {semver_line(v) for v in after}:
        yield (newest(v for v in before if semver_line(v) == line),
               newest(v for v in after if semver_line(v) == line))


def edge_downgrades(base_deps, head_deps):
    """Yields (crate, base version, head version) for each edge of one package that moved down.

    A package can depend on two versions of one crate under two names. Its edges to that crate
    pair up in version order.
    """
    for dep in sorted(base_deps.keys() & head_deps.keys()):
        before = sorted(base_deps[dep], key=version_key)
        after = sorted(head_deps[dep], key=version_key)
        if len(before) == len(after):
            pairs = zip(before, after)
        else:
            # The package added or dropped an edge to the crate, so version order does not
            # pair the edges. They compare within each semver line instead, so that a
            # dropped edge does not read as a downgrade of the edge below it.
            pairs = by_line(before, after)
        for b, h in pairs:
            if version_key(h) < version_key(b):
                yield dep, b, h


def main(argv):
    if len(argv) != 3:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    try:
        found = downgrades(load(argv[1]), load(argv[2]))
    except Exception as e:
        # Any failure is "could not run" (2), never "downgrade found" (1).
        print(f"::error::check-lock-downgrade.py cannot read the lockfiles: {e!r}", file=sys.stderr)
        return 2
    for message in found:
        print(message)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
