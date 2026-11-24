"""Tests for check-lock-downgrade.py.

Each test writes a base and a head lockfile, runs the check as CI runs it, and compares its
exit code and its stdout lines, because those two are the check's contract.

Usage: python3 -I .github/scripts/check-lock-downgrade.test.py
"""

import os
import subprocess
import sys
import tempfile
import unittest

CHECK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "check-lock-downgrade.py")
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"


def lock(*packages):
    """Returns a version 4 lockfile with one entry per (name, version, [dependencies])."""
    out = ["version = 4", ""]
    for package in packages:
        name, version, deps = package[:3]
        out += ["[[package]]", f'name = "{name}"', f'version = "{version}"']
        if len(package) > 3:
            out.append(f'source = "{package[3]}"')
        if deps:
            out += ["dependencies = ["] + [f' "{d}",' for d in deps] + ["]"]
        out.append("")
    return "\n".join(out)


WINDOWS_SYS = [("windows-sys", "0.52.0", []), ("windows-sys", "0.61.2", [])]
REPOINTED_BASE = lock(*WINDOWS_SYS, ("rustix", "1.1.6", ["windows-sys 0.61.2"]))
REPOINTED_HEAD = lock(*WINDOWS_SYS, ("rustix", "1.1.6", ["windows-sys 0.52.0"]))
REPOINTED_LINE = "windows-sys: 0.61.2 -> 0.52.0 (edge from rustix)"


def setUpModule():
    # Python exits 2 when it cannot open a script, which is also the exit code that the
    # could-not-run tests expect.
    if not os.path.isfile(CHECK):
        raise FileNotFoundError(CHECK)


def write(directory, name, text):
    path = os.path.join(directory, name)
    with open(path, "w", newline="") as f:
        f.write(text)
    return path


def run(*args, body=""):
    process = subprocess.run(
        [sys.executable, "-I", CHECK, *args], capture_output=True, text=True, env={"PR_BODY": body}
    )
    return process.returncode, process.stdout.splitlines()


def check(base, head, body=""):
    with tempfile.TemporaryDirectory() as d:
        return run(write(d, "base.lock", base), write(d, "head.lock", head), body=body)


def restored(base, head):
    """Returns the head lockfile after `--restore`, and the exit code of that run."""
    with tempfile.TemporaryDirectory() as d:
        base_path, head_path = write(d, "base.lock", base), write(d, "head.lock", head)
        code, _ = run("--restore", base_path, head_path)
        with open(head_path, newline="") as f:
            return code, f.read()


class Refusals(unittest.TestCase):
    def test_newest_version_goes_down(self):
        base = lock(("serde", "1.0.200", []), ("app", "0.1.0", ["serde"]))
        head = lock(("serde", "1.0.199", []), ("app", "0.1.0", ["serde"]))
        self.assertEqual(
            check(base, head),
            (1, ["serde: 1.0.200 -> 1.0.199", "serde: 1.0.200 -> 1.0.199 (edge from app)"]),
        )

    def test_newest_copy_removed(self):
        base = lock(
            *WINDOWS_SYS, ("a", "1.0.0", ["windows-sys 0.52.0"]), ("b", "1.0.0", ["windows-sys 0.61.2"])
        )
        head = lock(("windows-sys", "0.52.0", []), ("a", "1.0.0", ["windows-sys"]), ("b", "1.0.0", ["windows-sys"]))
        self.assertEqual(
            check(base, head),
            (1, ["windows-sys: 0.61.2 -> 0.52.0", "windows-sys: 0.61.2 -> 0.52.0 (edge from b)"]),
        )

    def test_release_back_to_its_prerelease(self):
        self.assertEqual(
            check(lock(("x", "1.0.0", [])), lock(("x", "1.0.0-rc.1", []))), (1, ["x: 1.0.0 -> 1.0.0-rc.1"])
        )

    def test_older_semver_line_goes_down_while_the_newest_stays(self):
        base = lock(("x", "1.4.2", []), ("x", "2.0.0", []))
        head = lock(("x", "1.4.1", []), ("x", "2.0.0", []))
        self.assertEqual(check(base, head), (1, ["x: 1.4.2 -> 1.4.1"]))

    def test_edge_repointed_to_an_older_copy(self):
        self.assertEqual(check(REPOINTED_BASE, REPOINTED_HEAD), (1, [REPOINTED_LINE]))

    def test_bare_edge_repointed_to_a_new_older_copy(self):
        base = lock(("windows-sys", "0.61.2", []), ("rustix", "1.1.6", ["windows-sys"]))
        self.assertEqual(check(base, REPOINTED_HEAD), (1, [REPOINTED_LINE]))

    def test_lower_of_two_renamed_copies_repointed(self):
        # serde_with depends on indexmap 1 and indexmap 2 under two names, so its edges
        # pair up in version order. Only the lower edge moves.
        copies = [("indexmap", "1.9.2", []), ("indexmap", "1.9.3", []), ("indexmap", "2.14.2", [])]
        base = lock(*copies, ("serde_with", "3.24.0", ["indexmap 1.9.3", "indexmap 2.14.2"]))
        head = lock(*copies, ("serde_with", "3.24.0", ["indexmap 1.9.2", "indexmap 2.14.2"]))
        self.assertEqual(check(base, head), (1, ["indexmap: 1.9.3 -> 1.9.2 (edge from serde_with)"]))

    def test_edge_from_one_of_two_sources_of_one_version(self):
        # The lockfile holds foo 1.0.0 from the registry and from git. Only the registry
        # copy's edge moves, and the git copy comes after it in the file.
        def foo(windows_sys_version):
            return [("foo", "1.0.0", [f"windows-sys {windows_sys_version}"], REGISTRY),
                    ("foo", "1.0.0", ["windows-sys 0.61.2"], "git+https://example.com/foo#0123abc")]
        base = lock(*WINDOWS_SYS, *foo("0.61.2"))
        head = lock(*WINDOWS_SYS, *foo("0.52.0"))
        self.assertEqual(check(base, head), (1, ["windows-sys: 0.61.2 -> 0.52.0 (edge from foo)"]))

    def test_acknowledgement_of_another_crate(self):
        self.assertEqual(
            check(REPOINTED_BASE, REPOINTED_HEAD, "allow-lock-downgrade: windows"), (1, [REPOINTED_LINE])
        )

    def test_acknowledgement_inside_a_line(self):
        body = "Do not write this: allow-lock-downgrade: windows-sys"
        self.assertEqual(check(REPOINTED_BASE, REPOINTED_HEAD, body), (1, [REPOINTED_LINE]))


class Passes(unittest.TestCase):
    def test_acknowledged_downgrade_with_crlf_line_ends(self):
        body = "Why.\r\n\r\nallow-lock-downgrade: windows-sys\r\n"
        self.assertEqual(check(REPOINTED_BASE, REPOINTED_HEAD, body), (0, []))

    def test_clean_upgrade(self):
        base = lock(("tokio", "1.53.3", []), ("app", "0.1.0", ["tokio"]))
        head = lock(("tokio", "1.54.1", []), ("app", "0.1.0", ["tokio"]))
        self.assertEqual(check(base, head), (0, []))

    def test_prerelease_to_release(self):
        self.assertEqual(check(lock(("x", "1.0.0-rc.1", [])), lock(("x", "1.0.0", []))), (0, []))

    def test_added_and_removed_crates(self):
        self.assertEqual(check(lock(("old", "1.0.0", [])), lock(("new", "0.1.0", []))), (0, []))

    def test_two_renamed_copies_both_unchanged(self):
        copies = [("indexmap", "1.9.3", []), ("indexmap", "2.14.2", [])]
        base = lock(*copies, ("serde_with", "3.24.0", ["indexmap 1.9.3", "indexmap 2.14.2"]))
        self.assertEqual(check(base, base), (0, []))

    def test_edge_repointed_up_to_another_line(self):
        # The check flags only a move down. A move up to another semver line is a known
        # gap that a reviewer sees in the diff.
        self.assertEqual(check(REPOINTED_HEAD, REPOINTED_BASE), (0, []))


class CouldNotRun(unittest.TestCase):
    def test_invalid_lockfile(self):
        self.assertEqual(check("not = [toml", lock())[0], 2)

    def test_missing_lockfile(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(run(os.path.join(d, "absent.lock"), write(d, "head.lock", lock()))[0], 2)

    def test_package_without_a_version(self):
        self.assertEqual(check('version = 4\n\n[[package]]\nname = "x"\n', lock())[0], 2)

    def test_wrong_argument_count(self):
        self.assertEqual(run("only-one.lock")[0], 2)

    def test_restore_with_an_invalid_lockfile(self):
        self.assertEqual(restored("not = [toml", lock())[0], 2)


class Restore(unittest.TestCase):
    def test_puts_each_downgraded_edge_back(self):
        base = lock(*WINDOWS_SYS, ("rustix", "1.1.6", ["windows-sys 0.61.2"]), ("tokio", "1.53.3", []))
        head = lock(*WINDOWS_SYS, ("rustix", "1.1.6", ["windows-sys 0.52.0"]), ("tokio", "1.54.1", []))
        want = lock(*WINDOWS_SYS, ("rustix", "1.1.6", ["windows-sys 0.61.2"]), ("tokio", "1.54.1", []))
        self.assertEqual(restored(base, head), (0, want))

    def test_keeps_an_edge_whose_base_copy_is_gone(self):
        base = lock(*WINDOWS_SYS, ("rustix", "1.1.6", ["windows-sys 0.61.2"]))
        head = lock(("windows-sys", "0.52.0", []), ("rustix", "1.1.6", ["windows-sys"]))
        self.assertEqual(restored(base, head), (0, head))

    def test_leaves_the_lower_of_two_renamed_copies_alone(self):
        copies = [("indexmap", "1.9.3", []), ("indexmap", "2.14.2", [])]
        base = lock(*copies, ("serde_with", "3.24.0", ["indexmap 1.9.3", "indexmap 2.14.2"]))
        self.assertEqual(restored(base, base), (0, base))

    def test_restored_lockfile_passes_the_check(self):
        _, head = restored(REPOINTED_BASE, REPOINTED_HEAD)
        self.assertEqual(check(REPOINTED_BASE, head), (0, []))


if __name__ == "__main__":
    unittest.main()
