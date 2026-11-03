import copy
import datetime
import importlib.util
import os
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "check_upstream_parity.py"
spec = importlib.util.spec_from_file_location("check_upstream_parity", SCRIPT)
parity = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = parity
spec.loader.exec_module(parity)

TODAY = datetime.date(2026, 11, 2)
HEAD = "f" * 40
U_SYNC = "1" * 40
U_BUMP2 = "2" * 40
U_MASQUE = "3" * 40
U_TIMEOUT = "4" * 40
U_BUMP3 = "5" * 40
F_SYNC = "a" * 40
F_TIMEOUT = "b" * 40
F_ELSEWHERE = "c" * 40
F_DIVERGE = "e" * 40
U_DEV = "d" * 40
V_LINES = ['-version = "0.9.1"', '+version = "0.9.2"', '-__version__ = "0.9.1"', '+__version__ = "0.9.2"']
V3_LINES = ['-version = "0.9.2"', '+version = "0.9.3"', "-Version: 0.9.2", "+Version: 0.9.3"]


class FakeGit:
    def __init__(self):
        self.history = {F_SYNC, F_TIMEOUT, F_DIVERGE}
        self.commits = {F_SYNC, F_TIMEOUT, F_ELSEWHERE, F_DIVERGE}
        self.patch_ids = {U_SYNC: "p-sync", F_SYNC: "p-sync", U_TIMEOUT: "p-timeout", F_TIMEOUT: "p-timeout"}
        self.messages = {F_SYNC: "fix: sync\n", F_TIMEOUT: "fix: timeout\n", F_ELSEWHERE: "x\n"}
        self.diffs = {U_BUMP2: V_LINES, U_BUMP3: V3_LINES}
        self.reverts = {}
        self.added = {}
        self.deleted = {}
        self.paths = {}
        self.files = {}

    def is_shallow(self):
        return False

    def has_commit(self, sha):
        return sha in self.commits

    def is_ancestor(self, sha, head):
        return head == HEAD and sha in self.history

    def patch_id(self, sha):
        return self.patch_ids.get(sha)

    def message(self, sha):
        return self.messages.get(sha, "")

    def diff_lines(self, sha):
        return self.diffs.get(sha, ["+code = 1"])

    def reverted(self, head):
        return dict(self.reverts)

    def is_empty(self, sha):
        return not self.diff_lines(sha)

    def added_lines(self, sha):
        return self.added.get(sha, {})

    def deleted_lines(self, sha):
        return self.deleted.get(sha, {})

    def changed_paths(self, sha):
        if sha in self.paths:
            return set(self.paths[sha])
        return set(self.added.get(sha, {})) | set(self.deleted.get(sha, {}))

    def file_lines(self, head, path):
        return self.files.get(path)


def make_snapshot():
    return parity.Snapshot(
        releases={"v0.9.1": "0" * 40, "v0.9.2": U_BUMP2, "v0.9.3": U_BUMP3},
        non_final_tags={},
        branches={"main": U_BUMP3, "dev": U_DEV},
        ranges={
            "v0.9.2": [parity.Commit(U_BUMP2, "bump to v0.9.2"), parity.Commit(U_SYNC, "fix sync")],
            "v0.9.3": [
                parity.Commit(U_BUMP3, "bump to v0.9.3"),
                parity.Commit(U_TIMEOUT, "fix timeout"),
                parity.Commit(U_MASQUE, "feat masque"),
            ],
        },
        dev_only={"main": [], "dev": [parity.Commit(U_DEV, "wip")]},
        published={"PyPI": {"0.9.1", "0.9.2", "0.9.3"}, "GitHub releases": {"v0.9.2", "v0.9.3"}},
    )


def make_ledger():
    return {
        "upstream": {"repository": "PasarGuard/node_bridge_py", "pypi": "pasarguard-node-bridge", "floor": "v0.9.2"},
        "release": [{"tag": "v0.9.2", "commit": U_BUMP2}, {"tag": "v0.9.3", "commit": U_BUMP3}],
        "commit": [
            {"upstream": U_SYNC, "release": "v0.9.2", "status": "ported", "fork": [F_SYNC], "match": "patch-id"},
            {"upstream": U_BUMP2, "release": "v0.9.2", "status": "version-only"},
            {"upstream": U_TIMEOUT, "release": "v0.9.3", "status": "ported", "fork": [F_TIMEOUT], "match": "patch-id"},
            {"upstream": U_BUMP3, "release": "v0.9.3", "status": "version-only"},
            {
                "upstream": U_MASQUE,
                "release": "v0.9.3",
                "status": "deferred",
                "owner": "BRIDGE",
                "until": "2026-11-05",
                "reason": "field clash",
            },
        ],
    }


def entry(ledger, sha):
    return next(item for item in ledger["commit"] if item["upstream"] == sha)


class UpstreamParityTests(unittest.TestCase):
    def run_check(self, ledger=None, snapshot=None, git=None, versions=None, today=TODAY):
        return parity.evaluate(
            ledger or make_ledger(),
            snapshot or make_snapshot(),
            git or FakeGit(),
            HEAD,
            versions or {"pyproject.toml": "0.9.2+singbox.1", "PasarGuardNodeBridge/__init__.py": "0.9.2+singbox.1"},
            today,
        )

    def assertFailsWith(self, report, fragment):
        self.assertTrue(
            any(fragment in failure for failure in report.failures),
            f"expected a failure containing {fragment!r}, got {report.failures}",
        )

    def test_complete_ledger_passes_and_reports_dev_only_commits(self):
        report = self.run_check()
        self.assertEqual(report.failures, [])
        self.assertTrue(any("dev-only on upstream dev" in note and U_DEV[:10] in note for note in report.notes))
        self.assertTrue(any("deferred until 2026-11-05" in note for note in report.notes))

    def test_unknown_release_tag_fails(self):
        snapshot = make_snapshot()
        snapshot.releases["v0.9.4"] = "9" * 40
        snapshot.ranges["v0.9.4"] = []
        self.assertFailsWith(self.run_check(snapshot=snapshot), "upstream release v0.9.4")

    def test_moved_release_tag_fails(self):
        snapshot = make_snapshot()
        snapshot.releases["v0.9.3"] = "9" * 40
        self.assertFailsWith(self.run_check(snapshot=snapshot), "v0.9.3 now points at")

    def test_unknown_commit_in_release_range_fails(self):
        snapshot = make_snapshot()
        snapshot.ranges["v0.9.3"].append(parity.Commit("e" * 40, "fix: silent"))
        self.assertFailsWith(self.run_check(snapshot=snapshot), "unknown upstream commit v0.9.3 eeeeeeeeee")

    def test_entry_filed_under_the_wrong_release_fails(self):
        ledger = make_ledger()
        entry(ledger, U_TIMEOUT)["release"] = "v0.9.2"
        self.assertFailsWith(self.run_check(ledger=ledger), "upstream ships it in v0.9.3")

    def test_ledger_entry_outside_every_release_fails(self):
        ledger = make_ledger()
        ledger["commit"].append(
            {"upstream": U_DEV, "release": "v0.9.3", "status": "waived", "reason": "r", "owner": "o"}
        )
        self.assertFailsWith(
            self.run_check(ledger=ledger), f"ledger commit {U_DEV[:10]} is not in any upstream release"
        )

    def test_expired_deferral_fails(self):
        report = self.run_check(today=datetime.date(2026, 11, 6))
        self.assertFailsWith(report, "deferral expired on 2026-11-05")

    def test_live_deferral_passes_through_its_last_day(self):
        self.assertEqual(self.run_check(today=datetime.date(2026, 11, 5)).failures, [])

    def test_deferral_beyond_the_horizon_fails(self):
        ledger = make_ledger()
        entry(ledger, U_MASQUE)["until"] = "2027-06-01"
        self.assertFailsWith(self.run_check(ledger=ledger), "more than 31 days away")

    def test_deferral_with_a_timestamp_instead_of_a_date_fails(self):
        ledger = make_ledger()
        entry(ledger, U_MASQUE)["until"] = datetime.datetime(2026, 11, 5, 12, 0)
        self.assertFailsWith(self.run_check(ledger=ledger), "must be a YYYY-MM-DD date")

    def test_deferral_without_owner_fails(self):
        ledger = make_ledger()
        del entry(ledger, U_MASQUE)["owner"]
        self.assertFailsWith(self.run_check(ledger=ledger), "needs `owner`")

    def test_ported_sha_not_in_history_fails(self):
        ledger = make_ledger()
        entry(ledger, U_TIMEOUT)["fork"] = [F_ELSEWHERE]
        self.assertFailsWith(self.run_check(ledger=ledger), f"fork commit {F_ELSEWHERE} is not in the history")

    def test_ported_sha_missing_from_repository_fails(self):
        ledger = make_ledger()
        entry(ledger, U_TIMEOUT)["fork"] = ["9" * 40]
        self.assertFailsWith(self.run_check(ledger=ledger), "does not exist in this repository")

    def test_ported_patch_id_mismatch_fails(self):
        git = FakeGit()
        git.patch_ids[F_TIMEOUT] = "p-other"
        self.assertFailsWith(self.run_check(git=git), "has the upstream patch-id")

    def test_adapted_port_must_cite_the_upstream_sha(self):
        ledger = make_ledger()
        item = entry(ledger, U_TIMEOUT)
        item["match"] = "adapted"
        item["reason"] = "rebased onto the fork's heartbeat"
        git = FakeGit()
        git.patch_ids[F_TIMEOUT] = "p-other"
        git.added[U_TIMEOUT] = git.added[F_TIMEOUT] = {"bridge.py": ["timeout = 330"]}
        git.files["bridge.py"] = {"timeout = 330"}
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "cites the full upstream SHA")
        git.messages[F_TIMEOUT] = f"fix: timeout\n\nUpstream {U_TIMEOUT}, adapted.\n"
        self.assertEqual(self.run_check(ledger=ledger, git=git).failures, [])
        git.messages[F_TIMEOUT] = f"fix: timeout\n\nUpstream: {U_TIMEOUT}\n"
        self.assertEqual(self.run_check(ledger=ledger, git=git).failures, [])

    def test_adapted_port_citing_only_a_short_prefix_fails(self):
        ledger = make_ledger()
        item = entry(ledger, U_TIMEOUT)
        item["match"] = "adapted"
        item["reason"] = "rebased onto the fork's heartbeat"
        git = FakeGit()
        for message in (
            f"fix: timeout\n\nUpstream {U_TIMEOUT[:7]}, adapted.\n",
            f"fix: timeout\n\nUpstream: {U_TIMEOUT[:12]}\n",
            f"fix: timeout\n\nnot {U_TIMEOUT[:39]}\n",
        ):
            git.messages[F_TIMEOUT] = message
            self.assertFailsWith(self.run_check(ledger=ledger, git=git), "cites the full upstream SHA")

    def test_reverted_port_fails(self):
        git = FakeGit()
        git.reverts = {"7" * 40: (F_TIMEOUT,)}
        self.assertFailsWith(self.run_check(git=git), f"fork commit {F_TIMEOUT[:10]} is reverted")

    def test_reverted_port_cited_by_a_short_sha_fails(self):
        git = FakeGit()
        git.reverts = {"7" * 40: ("0" * 7, F_TIMEOUT[:7])}
        self.assertFailsWith(self.run_check(git=git), f"fork commit {F_TIMEOUT[:10]} is reverted")

    def test_revert_of_a_revert_reapplies_the_port(self):
        git = FakeGit()
        git.reverts = {"7" * 40: (F_TIMEOUT,), "8" * 40: (("7" * 40)[:8],)}
        self.assertEqual(self.run_check(git=git).failures, [])
        git.reverts["9" * 40] = ("8" * 40,)
        self.assertFailsWith(self.run_check(git=git), f"fork commit {F_TIMEOUT[:10]} is reverted")

    def test_reapplied_port_is_judged_by_its_lines_at_head(self):
        git = FakeGit()
        git.reverts = {"7" * 40: (F_TIMEOUT,), "8" * 40: ("7" * 40,)}
        git.added[F_TIMEOUT] = {"bridge.py": ["timeout = 330"]}
        git.files["bridge.py"] = {"timeout = 30"}
        self.assertFailsWith(self.run_check(git=git), "are gone at HEAD")
        git.files["bridge.py"] = {"timeout = 330"}
        self.assertEqual(self.run_check(git=git).failures, [])

    def adapted(self, git):
        ledger = make_ledger()
        entry(ledger, U_TIMEOUT).update(match="adapted", reason="split for review")
        git.patch_ids[F_TIMEOUT] = "p-other"
        git.messages[F_TIMEOUT] = f"fix: timeout\n\nUpstream: {U_TIMEOUT}\n"
        git.added[U_TIMEOUT] = {"bridge.py": ["    timeout = 330", "", "    retry(user)"]}
        present = {"timeout = 330", "timeout  =  330", "retry(user)"}
        git.files.update({"bridge.py": present, "pkg/bridge.py": present})
        return ledger

    def test_adapted_port_of_only_empty_commits_fails(self):
        git = FakeGit()
        ledger = self.adapted(git)
        git.diffs[F_TIMEOUT] = []
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "every fork commit is empty")
        git.added.pop(U_TIMEOUT)
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "every fork commit is empty")

    def test_adapted_port_must_carry_the_upstream_added_lines(self):
        git = FakeGit()
        ledger = self.adapted(git)
        git.added[F_TIMEOUT] = {"pkg/bridge.py": ["timeout  =  330"]}
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "1 line(s) upstream")
        git.added[F_TIMEOUT]["pkg/bridge.py"].append("\tretry(user)")
        self.assertEqual(self.run_check(ledger=ledger, git=git).failures, [])

    def test_adapted_port_lines_of_an_empty_commit_do_not_count(self):
        git = FakeGit()
        ledger = self.adapted(git)
        entry(ledger, U_TIMEOUT)["fork"] = [F_TIMEOUT, F_DIVERGE]
        git.messages[F_DIVERGE] = "x\n"
        git.added[F_TIMEOUT] = {"bridge.py": ["timeout = 330"]}
        git.added[F_DIVERGE] = {"bridge.py": ["retry(user)"]}
        git.diffs[F_DIVERGE] = []
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "1 line(s) upstream")

    def test_adapted_port_line_listed_as_diverged_passes(self):
        git = FakeGit()
        ledger = self.adapted(git)
        item = entry(ledger, U_TIMEOUT)
        git.added[F_TIMEOUT] = {"bridge.py": ["timeout = 330"]}
        git.files["bridge.py"] = {"timeout = 330"}
        item["diverged_lines"] = [{"line": "retry(user)"}]
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "needs a `reason`")
        item["diverged_lines"] = [{"line": "retry( user )", "reason": "held in the store"}]
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "1 line(s) upstream")
        item["diverged_lines"] = [{"line": "  retry(user) ", "reason": "held in the store"}]
        self.assertEqual(self.run_check(ledger=ledger, git=git).failures, [])
        item["diverged_lines"].append({"line": "timeout = 330", "reason": "stale"})
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "the fork commits carry it")
        item["diverged_lines"] = [{"line": "never = upstream", "reason": "stale"}]
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "is not an upstream added line")

    def test_adapted_port_whose_line_one_fork_commit_adds_and_another_deletes_fails(self):
        git = FakeGit()
        ledger = self.adapted(git)
        entry(ledger, U_TIMEOUT)["fork"] = [F_TIMEOUT, F_DIVERGE]
        git.messages[F_DIVERGE] = "fix: drop the retry\n"
        git.added[F_TIMEOUT] = {"bridge.py": ["timeout = 330", "retry(user)"]}
        git.diffs[F_DIVERGE] = ["-retry(user)"]
        git.deleted[F_DIVERGE] = {"bridge.py": ["retry(user)"]}
        git.files["bridge.py"] = {"timeout = 330"}
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "but absent at HEAD")
        entry(ledger, U_TIMEOUT)["diverged_lines"] = [{"line": "retry(user)", "reason": "the store holds retries"}]
        self.assertEqual(self.run_check(ledger=ledger, git=git).failures, [])
        git.files["bridge.py"] = {"timeout = 330", "retry(user)"}
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "the fork commits carry it")

    def test_diverged_lines_that_is_not_a_list_fails_cleanly(self):
        git = FakeGit()
        ledger = self.adapted(git)
        git.added[F_TIMEOUT] = {"bridge.py": ["timeout = 330", "retry(user)"]}
        for value in (5, {"line": "retry(user)", "reason": "r"}, "retry(user)"):
            entry(ledger, U_TIMEOUT)["diverged_lines"] = value
            self.assertFailsWith(self.run_check(ledger=ledger, git=git), "`diverged_lines` must be a list")

    def test_diverged_lines_on_a_row_that_is_not_adapted_fails(self):
        ledger = make_ledger()
        entry(ledger, U_SYNC)["diverged_lines"] = []
        entry(ledger, U_MASQUE)["diverged_lines"] = [{"line": "x", "reason": "r"}]
        entry(ledger, U_BUMP3)["no_added_lines"] = "bump"
        report = self.run_check(ledger=ledger)
        self.assertFailsWith(report, "v0.9.2 1111111111 'fix sync': `diverged_lines` is only allowed")
        self.assertFailsWith(report, "v0.9.3 3333333333 'feat masque': `diverged_lines` is only allowed")
        self.assertFailsWith(report, "v0.9.3 5555555555 'bump to v0.9.3': `no_added_lines` is only allowed")

    def test_adapted_port_of_an_upstream_commit_that_adds_no_lines(self):
        git = FakeGit()
        ledger = self.adapted(git)
        item = entry(ledger, U_TIMEOUT)
        git.added[U_TIMEOUT] = {}
        git.added[F_TIMEOUT] = {"bridge.py": ["timeout = 330"]}
        git.deleted[U_TIMEOUT] = {"bridge.py": ["    legacy(user)", ""]}
        git.files["bridge.py"] = {"timeout = 330", "legacy(user)"}
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "needs a `no_added_lines` text")
        item["no_added_lines"] = "upstream only deletes the legacy call"
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "are still at HEAD")
        git.files["bridge.py"] = {"timeout = 330"}
        self.assertEqual(self.run_check(ledger=ledger, git=git).failures, [])
        git.deleted[U_TIMEOUT] = {}
        git.paths[U_TIMEOUT] = {"logo.png"}
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "changes logo.png, which no fork commit touches")
        git.paths[F_TIMEOUT] = {"bridge.py", "logo.png"}
        self.assertEqual(self.run_check(ledger=ledger, git=git).failures, [])
        git.paths[U_TIMEOUT] = set()
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "changes no file, so there is nothing to port")

    def test_port_whose_lines_are_gone_at_head_fails(self):
        git = FakeGit()
        git.added[F_TIMEOUT] = {"bridge.py": ["    timeout = 330", ""]}
        git.files["bridge.py"] = {"timeout = 30"}
        self.assertFailsWith(self.run_check(git=git), "are gone at HEAD")
        git.files.pop("bridge.py")
        self.assertFailsWith(self.run_check(git=git), "are gone at HEAD")

    def test_port_whose_lines_are_present_at_head_passes(self):
        git = FakeGit()
        git.added[F_TIMEOUT] = {"bridge.py": ["    timeout = 330", ""]}
        git.files["bridge.py"] = {"timeout = 330", "x = 1"}
        self.assertEqual(self.run_check(git=git).failures, [])

    def test_port_lines_a_later_port_removed_pass(self):
        git = FakeGit()
        git.added[F_SYNC] = {"sync.py": ["requeue(user)"]}
        git.files["sync.py"] = {"hold(user)"}
        git.diffs[F_TIMEOUT] = ["-requeue(user)", "+hold(user)"]
        self.assertEqual(self.run_check(git=git).failures, [])

    def test_port_lines_removed_by_an_unnamed_commit_fail(self):
        git = FakeGit()
        git.added[F_SYNC] = {"sync.py": ["requeue(user)"]}
        git.files["sync.py"] = {"hold(user)"}
        git.diffs[F_ELSEWHERE] = ["-requeue(user)", "+hold(user)"]
        self.assertFailsWith(self.run_check(git=git), "are gone at HEAD")

    def test_port_lines_a_diverged_commit_removed_pass(self):
        ledger = make_ledger()
        entry(ledger, U_SYNC).update(diverged=[F_DIVERGE], divergence="retries are held in the store")
        git = FakeGit()
        git.added[F_SYNC] = {"sync.py": ["requeue(user)"]}
        git.files["sync.py"] = {"hold(user)"}
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "are gone at HEAD")
        git.diffs[F_DIVERGE] = ["-requeue(user)", "+hold(user)"]
        self.assertEqual(self.run_check(ledger=ledger, git=git).failures, [])

    def test_divergence_with_fork_commits_in_history_passes(self):
        ledger = make_ledger()
        entry(ledger, U_SYNC).update(diverged=[F_DIVERGE], divergence="retries are held in the store")
        self.assertEqual(self.run_check(ledger=ledger).failures, [])

    def test_diverged_commit_outside_history_fails(self):
        ledger = make_ledger()
        entry(ledger, U_SYNC).update(diverged=[F_DIVERGE, F_ELSEWHERE], divergence="retries are held in the store")
        self.assertFailsWith(self.run_check(ledger=ledger), f"fork commit {F_ELSEWHERE} is not in the history")

    def test_diverged_short_sha_fails(self):
        ledger = make_ledger()
        entry(ledger, U_SYNC).update(diverged=[F_DIVERGE[:12]], divergence="retries are held in the store")
        self.assertFailsWith(self.run_check(ledger=ledger), "is not a full 40-hex SHA")

    def test_diverged_without_a_divergence_text_fails(self):
        ledger = make_ledger()
        entry(ledger, U_SYNC).update(diverged=[F_DIVERGE], divergence="  ")
        self.assertFailsWith(self.run_check(ledger=ledger), "needs a `divergence` text")

    def test_divergence_text_without_diverged_commits_fails(self):
        ledger = make_ledger()
        entry(ledger, U_SYNC)["divergence"] = "retries are held in the store"
        self.assertFailsWith(self.run_check(ledger=ledger), "non-empty `diverged` list")

    def unreleased_ledger(self, **fields):
        ledger = make_ledger()
        row = {
            "upstream": U_DEV,
            "branch": "dev",
            "subject": "wip",
            "status": "waived",
            "checked": datetime.date(2026, 11, 2),
            "owner": "BRIDGE",
            "reason": "on no upstream release",
        }
        row.update(fields)
        ledger["unreleased"] = [row]
        return ledger

    def test_unreleased_waiver_of_a_branch_only_commit_passes(self):
        report = self.run_check(ledger=self.unreleased_ledger())
        self.assertEqual(report.failures, [])
        self.assertTrue(any("waived as non-release" in note and U_DEV[:10] in note for note in report.notes))

    def test_unreleased_waiver_of_a_commit_a_release_now_ships_fails(self):
        snapshot = make_snapshot()
        snapshot.dev_only["dev"] = []
        snapshot.ranges["v0.9.3"].append(parity.Commit(U_DEV, "wip"))
        report = self.run_check(ledger=self.unreleased_ledger(), snapshot=snapshot)
        self.assertFailsWith(report, f"{U_DEV[:10]} now ships in upstream release v0.9.3")

    def test_unreleased_waiver_on_the_wrong_branch_fails(self):
        self.assertFailsWith(
            self.run_check(ledger=self.unreleased_ledger(branch="main")), "is not a commit only on upstream main"
        )

    def test_unreleased_waiver_without_a_date_fails(self):
        ledger = self.unreleased_ledger()
        del ledger["unreleased"][0]["checked"]
        self.assertFailsWith(self.run_check(ledger=ledger), "`checked` must be a YYYY-MM-DD date")

    def test_unreleased_waiver_dated_in_the_future_fails(self):
        ledger = self.unreleased_ledger(checked=datetime.date(2026, 11, 9))
        self.assertFailsWith(self.run_check(ledger=ledger), "is after today")

    def test_unreleased_row_must_be_waived_with_owner_and_reason(self):
        self.assertFailsWith(self.run_check(ledger=self.unreleased_ledger(status="ported")), "must be waived")
        self.assertFailsWith(self.run_check(ledger=self.unreleased_ledger(owner=" ")), "needs `owner`")
        self.assertFailsWith(self.run_check(ledger=self.unreleased_ledger(reason="")), "needs `reason`")

    def test_unreleased_row_that_duplicates_a_release_row_fails(self):
        ledger = self.unreleased_ledger(upstream=U_TIMEOUT)
        self.assertFailsWith(self.run_check(ledger=ledger), "listed twice")

    def test_version_only_with_a_code_change_fails(self):
        git = FakeGit()
        git.diffs[U_BUMP3] = [*V3_LINES, "+timeout = 330"]
        self.assertFailsWith(self.run_check(git=git), "changes more than version labels")

    def test_version_only_that_sets_another_version_fails(self):
        git = FakeGit()
        git.diffs[U_BUMP3] = ['-version = "0.4.9"', '+version = "0.4.10"']
        self.assertFailsWith(self.run_check(git=git), "not the release version 0.9.3")

    def test_version_claim_with_a_deferred_commit_fails(self):
        versions = {"pyproject.toml": "0.9.3+singbox.1", "PasarGuardNodeBridge/__init__.py": "0.9.3+singbox.1"}
        self.assertFailsWith(self.run_check(versions=versions), f"claims upstream 0.9.3 but lacks: {U_MASQUE[:10]}")

    def test_version_claim_passes_once_every_commit_is_accounted_for(self):
        ledger = make_ledger()
        item = entry(ledger, U_MASQUE)
        item.update(status="waived", reason="field clash, decided", owner="BRIDGE")
        del item["until"]
        versions = {"pyproject.toml": "0.9.3+singbox.1", "PasarGuardNodeBridge/__init__.py": "0.9.3+singbox.1"}
        self.assertEqual(self.run_check(ledger=ledger, versions=versions).failures, [])

    def test_version_claim_of_an_unreleased_version_fails(self):
        versions = {"pyproject.toml": "0.9.9", "PasarGuardNodeBridge/__init__.py": "0.9.9"}
        self.assertFailsWith(self.run_check(versions=versions), "which upstream never released")

    def test_version_claim_below_the_floor_fails(self):
        versions = {"pyproject.toml": "0.9.1+x", "PasarGuardNodeBridge/__init__.py": "0.9.1+x"}
        self.assertFailsWith(self.run_check(versions=versions), "below the ledger floor")

    def test_disagreeing_version_labels_fail(self):
        versions = {"pyproject.toml": "0.9.2+singbox.1", "PasarGuardNodeBridge/__init__.py": "0.9.3"}
        self.assertFailsWith(self.run_check(versions=versions), "version labels disagree")

    def test_published_version_without_a_tag_fails(self):
        snapshot = make_snapshot()
        snapshot.published["PyPI"].add("0.9.5")
        self.assertFailsWith(self.run_check(snapshot=snapshot), "PyPI published 0.9.5")

    def test_unknown_status_fails(self):
        ledger = make_ledger()
        entry(ledger, U_BUMP3)["status"] = "ignored"
        self.assertFailsWith(self.run_check(ledger=ledger), "status must be one of")

    def test_duplicate_entry_fails(self):
        ledger = make_ledger()
        ledger["commit"].append(copy.deepcopy(entry(ledger, U_BUMP3)))
        self.assertFailsWith(self.run_check(ledger=ledger), "listed twice")

    def test_parse_ls_remote_peels_annotated_tags_and_splits_non_final(self):
        text = "\n".join(
            [
                f"{'1' * 40}\tHEAD",
                f"{'2' * 40}\trefs/heads/main",
                f"{'3' * 40}\trefs/tags/v0.9.0",
                f"{'4' * 40}\trefs/tags/v0.9.0^{{}}",
                f"{'5' * 40}\trefs/tags/v0.9.4",
                f"{'6' * 40}\trefs/tags/v1.0.0-rc1",
                f"{'7' * 40}\trefs/pull/27/head",
            ]
        )
        releases, non_final, branches = parity.parse_ls_remote(text)
        self.assertEqual(releases, {"v0.9.0": "4" * 40, "v0.9.4": "5" * 40})
        self.assertEqual(non_final, {"v1.0.0-rc1": "6" * 40})
        self.assertEqual(branches, {"main": "2" * 40})

    def test_parse_ls_remote_rejects_malformed_lines(self):
        with self.assertRaises(parity.ParityError):
            parity.parse_ls_remote("not-a-sha\trefs/tags/v1.0.0")

    def test_patch_id_tells_apart_ports_that_differ_only_in_indentation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            git = parity.Git(root)
            git.run("init", "-q", "-b", "main")
            git.run("config", "user.email", "t@example.test")
            git.run("config", "user.name", "t")
            (root / "sync.py").write_text("for u in users:\n    push(u)\n")
            git.run("add", "sync.py")
            git.run("commit", "-q", "-m", "base")
            base = git.resolve("HEAD")
            (root / "sync.py").write_text("for u in users:\n    push(u)\n    mark(u)\n")
            git.run("commit", "-q", "-am", "inside the loop")
            inside = git.resolve("HEAD")
            git.run("reset", "-q", "--hard", base)
            (root / "sync.py").write_text("for u in users:\n    push(u)\nmark(u)\n")
            git.run("commit", "-q", "-am", "after the loop")
            after = git.resolve("HEAD")
            self.assertNotEqual(git.patch_id(inside), git.patch_id(after))

    def test_git_reads_a_revert_of_a_revert_and_an_empty_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            git = parity.Git(root)
            git.run("init", "-q", "-b", "main")
            git.run("config", "user.email", "t@example.test")
            git.run("config", "user.name", "t")
            (root / "sync.py").write_text("push(u)\n")
            git.run("add", "sync.py")
            git.run("commit", "-q", "-m", "base")
            (root / "sync.py").write_text("push(u)\nhold(u)\n")
            git.run("commit", "-q", "-am", "port")
            port = git.resolve("HEAD")
            git.run("revert", "--no-edit", port)
            first = git.resolve("HEAD")
            git.run("revert", "--no-edit", first)
            second = git.resolve("HEAD")
            git.run("revert", "--no-edit", second)
            git.run("commit", "-q", "--amend", "-m", f"Revert\x1e reapply\n\nThis reverts commit {second}.")
            third = git.resolve("HEAD")
            git.run("commit", "-q", "--allow-empty", "-m", f"cite\n\nUpstream: {U_SYNC}")
            empty = git.resolve("HEAD")
            git.run("mv", "sync.py", "store.py")
            git.run("commit", "-q", "-m", "rename")
            moved = git.resolve("HEAD")
            reverts = git.reverted(empty)
            self.assertEqual(reverts, {first: (port,), second: (first,), third: (second,)})
            self.assertEqual(parity.live_revert(port, reverts, set()), first)
            del reverts[third]
            self.assertIsNone(parity.live_revert(port, reverts, set()))
            self.assertEqual(parity.live_revert(first, reverts, set()), second)
            self.assertEqual(git.diff_lines(empty), [])
            self.assertEqual(git.added_lines(empty), {})
            self.assertTrue(git.is_empty(empty))
            self.assertEqual(git.diff_lines(moved), [])
            self.assertFalse(git.is_empty(moved))
            self.assertEqual(git.added_lines(moved), {"store.py": ["push(u)"]})

    def test_parse_diff_tracks_hunks_so_content_lines_that_look_like_headers_count(self):
        diff = "\n".join(
            [
                "diff --git a/x.py b/x.py",
                "--- a/x.py",
                "+++ b/x.py",
                "@@ -1,2 +1,3 @@",
                "--- old",
                "-keep",
                "+++ new",
                "++ plus",
                "+x = 1",
                "\\ No newline at end of file",
                "diff --git a/y.py b/y.py",
                "new file mode 100644",
                "--- /dev/null",
                "+++ b/y.py",
                "@@ -0,0 +1 @@",
                "+y = 2",
                "diff --git a/z.py b/z.py",
                "deleted file mode 100644",
                "--- a/z.py",
                "+++ /dev/null",
                "@@ -1 +0,0 @@",
                "-z = 3",
            ]
        )
        added, deleted = parity.parse_diff(diff)
        self.assertEqual(added, {"x.py": ["++ new", "+ plus", "x = 1"], "y.py": ["y = 2"]})
        self.assertEqual(deleted, {"x.py": ["-- old", "keep"], "z.py": ["z = 3"]})

    def test_git_diff_ignores_prefix_and_colour_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            git = parity.Git(root)
            git.run("init", "-q", "-b", "main")
            git.run("config", "user.email", "t@example.test")
            git.run("config", "user.name", "t")
            (root / "x.py").write_text("-- old\nkeep\n")
            git.run("add", "x.py")
            git.run("commit", "-q", "-m", "base")
            (root / "x.py").write_text("++ new\nkeep\n")
            git.run("commit", "-q", "-am", "change")
            change = git.resolve("HEAD")
            for key, value in (
                ("diff.noprefix", "true"),
                ("diff.srcPrefix", "s/"),
                ("diff.dstPrefix", "d/"),
                ("diff.mnemonicPrefix", "true"),
                ("color.ui", "always"),
                ("color.diff", "always"),
                ("diff.renames", "false"),
            ):
                git.run("config", key, value)
                self.assertEqual(git.added_lines(change), {"x.py": ["++ new"]}, key)
                self.assertEqual(git.deleted_lines(change), {"x.py": ["-- old"]}, key)
                self.assertEqual(git.diff_lines(change), ["--- old", "+++ new"], key)
                self.assertEqual(git.changed_paths(change), {"x.py"}, key)

    def test_repository_ledger_is_well_formed(self):
        with open(parity.LEDGER, "rb") as handle:
            ledger = tomllib.load(handle)
        for item in ledger["commit"]:
            self.assertRegex(item["upstream"], r"^[0-9a-f]{40}$")
            self.assertIn(item["status"], parity.STATUSES)
        for item in ledger.get("unreleased", []):
            self.assertRegex(item["upstream"], r"^[0-9a-f]{40}$")
            self.assertEqual(item["status"], "waived")
        tags = [item["tag"] for item in ledger["release"]]
        self.assertIn(ledger["upstream"]["floor"], tags)
        self.assertEqual(len(tags), len(set(tags)))

    def test_bj_a_a_carried_line_counts_only_where_its_fork_commit_put_it(self):
        git = FakeGit()
        ledger = self.adapted(git)
        entry(ledger, U_TIMEOUT).update(diverged=[F_DIVERGE], divergence="the retry moved out of other.py")
        git.diffs[F_DIVERGE] = ["-retry(user)"]
        git.added[F_TIMEOUT] = {"bridge.py": ["timeout = 330"], "other.py": ["retry(user)"]}
        git.files["other.py"] = {"timeout = 330"}
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "but absent at HEAD")
        git.files["other.py"] = {"retry(user)"}
        self.assertEqual(self.run_check(ledger=ledger, git=git).failures, [])

    def test_bj_e_diverged_lines_must_be_unique_strings_absent_at_head(self):
        git = FakeGit()
        ledger = self.adapted(git)
        item = entry(ledger, U_TIMEOUT)
        git.added[U_TIMEOUT]["bridge.py"].append("count = 5")
        git.added[F_TIMEOUT] = {"bridge.py": ["timeout = 330"]}
        git.files["bridge.py"] = {"timeout = 330"}
        item["diverged_lines"] = [
            {"line": "retry(user)", "reason": "held"},
            {"line": " retry(user)", "reason": "held twice"},
            {"line": "count = 5", "reason": "dropped"},
        ]
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "is listed twice in `diverged_lines`")
        item["diverged_lines"] = [{"line": "retry(user)", "reason": "held"}, {"line": 5, "reason": "number"}]
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "needs a string `line`")
        item["diverged_lines"] = [{"line": "retry(user)", "reason": "held"}, {"line": "count = 5", "reason": "x"}]
        self.assertEqual(self.run_check(ledger=ledger, git=git).failures, [])
        git.files["bridge.py"] = {"timeout = 330", "retry(user)"}
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "is at HEAD but no fork commit carries it")

    def test_bj_f_an_upstream_deletion_needs_a_fork_commit_on_its_path_and_the_file_at_head(self):
        git = FakeGit()
        ledger = self.adapted(git)
        item = entry(ledger, U_TIMEOUT)
        item["no_added_lines"] = "upstream only deletes the legacy call"
        git.added[U_TIMEOUT] = {}
        git.added[F_TIMEOUT] = {"bridge.py": ["timeout = 330"]}
        git.deleted[U_TIMEOUT] = {"legacy.py": ["legacy(user)"]}
        git.files["legacy.py"] = {"other(user)"}
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "changes legacy.py, which no fork commit touches")
        git.paths[F_TIMEOUT] = {"bridge.py", "legacy.py"}
        self.assertEqual(self.run_check(ledger=ledger, git=git).failures, [])
        git.files.pop("legacy.py")
        self.assertFailsWith(self.run_check(ledger=ledger, git=git), "legacy.py is not at HEAD")


class DiffReaderTests(unittest.TestCase):
    def git(self, root, *args):
        env = {
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t",
            "GIT_CONFIG_NOSYSTEM": "1",
            "HOME": str(root),
            "PATH": os.environ.get("PATH", ""),
        }
        subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True, env=env)

    def test_bj_b_c_d_paths_with_spaces_or_non_ascii_and_lines_with_form_feeds_or_cr(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.git(root, "init", "-q")
            self.git(root, "config", "core.quotePath", "true")
            (root / "a b.py").write_bytes(b"x = 1\n")
            (root / "é.py").write_bytes("y = 2\n".encode())
            (root / "f.py").write_bytes(b"a\x0cb\nc\rd\ne\x0bf\n")
            self.git(root, "add", "-A")
            self.git(root, "commit", "-q", "-m", "add")
            git = parity.Git(root)
            added, _ = git.changes("HEAD")
            self.assertEqual(
                added, {"a b.py": ["x = 1"], "é.py": ["y = 2"], "f.py": ["a\x0cb", "c\rd", "e\x0bf"]}
            )
            self.assertEqual(git.file_lines("HEAD", "f.py"), {"a\x0cb", "c\rd", "e\x0bf", ""})
            self.assertEqual(git.changed_paths("HEAD"), {"a b.py", "é.py", "f.py"})

    def test_bj_b_c_parse_diff_reads_git_quoted_and_tab_terminated_names(self):
        diff = (
            "diff --git a/a b b/a b\n--- a/a b\t\n+++ b/a b\t\n@@ -1 +1 @@\n-old\n+new\n"
            'diff --git "a/\\303\\251.py" "b/\\303\\251.py"\n--- "a/\\303\\251.py"\n+++ "b/\\303\\251.py"\n'
            "@@ -1 +1 @@\n-\x0c\n+q\\\"\n"
        )
        added, deleted = parity.parse_diff(diff)
        self.assertEqual(added, {"a b": ["new"], "é.py": ['q\\"']})
        self.assertEqual(deleted, {"a b": ["old"], "é.py": ["\x0c"]})


if __name__ == "__main__":
    unittest.main()
