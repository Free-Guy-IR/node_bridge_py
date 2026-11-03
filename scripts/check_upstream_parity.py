#!/usr/bin/env python3
import argparse
import dataclasses
import datetime
import json
import os
import re
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / "upstream-parity.toml"
VERSION_FILES = ("pyproject.toml", "PasarGuardNodeBridge/__init__.py")
NAMESPACE = "refs/parity/upstream"
FINAL_VERSION = re.compile(r"^v?(\d+(?:\.\d+)*)$")
SHA = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
VERSION_LINE = re.compile(r'^([+-])\s*(?:Version:\s*|__version__\s*=\s*|version\s*=\s*)"?(\d[0-9A-Za-z.+-]*)"?\s*$')
FULL_SHA_TOKEN = re.compile(r"(?<![0-9a-f])[0-9a-f]{40}(?![0-9a-f])")
REVERT = re.compile(r"This reverts commit ([0-9a-f]{7,40})")
STATUSES = ("ported", "version-only", "waived", "deferred")
COMPLETE = ("ported", "version-only", "waived")
MATCHES = ("patch-id", "adapted")
MAX_DEFERRAL_DAYS = 31


@dataclasses.dataclass(frozen=True)
class Commit:
    sha: str
    subject: str


@dataclasses.dataclass
class Snapshot:
    releases: dict[str, str]
    non_final_tags: dict[str, str]
    branches: dict[str, str]
    ranges: dict[str, list[Commit]]
    dev_only: dict[str, list[Commit]]
    published: dict[str, set[str]]


@dataclasses.dataclass
class Report:
    failures: list[str] = dataclasses.field(default_factory=list)
    notes: list[str] = dataclasses.field(default_factory=list)

    def fail(self, message: str) -> None:
        self.failures.append(message)

    def note(self, message: str) -> None:
        self.notes.append(message)


class ParityError(Exception):
    pass


def version_key(name: str) -> tuple[int, ...] | None:
    match = FINAL_VERSION.match(name)
    if not match:
        return None
    return tuple(int(part) for part in match.group(1).split("."))


def parse_ls_remote(text: str) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    direct: dict[str, str] = {}
    peeled: dict[str, str] = {}
    branches: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        sha, _, ref = line.partition("\t")
        sha = sha.strip()
        ref = ref.strip()
        if not SHA.match(sha):
            raise ParityError(f"malformed ls-remote line: {line!r}")
        if ref.startswith("refs/tags/"):
            name = ref[len("refs/tags/") :]
            if name.endswith("^{}"):
                peeled[name[:-3]] = sha
            else:
                direct[name] = sha
        elif ref.startswith("refs/heads/"):
            branches[ref[len("refs/heads/") :]] = sha
    tags = {name: peeled.get(name, sha) for name, sha in direct.items()}
    releases = {name: sha for name, sha in tags.items() if version_key(name) is not None}
    non_final = {name: sha for name, sha in tags.items() if version_key(name) is None}
    return releases, non_final, branches


class Git:
    def __init__(self, root: Path):
        self.root = root

    def run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["git", "-C", str(self.root), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if check and result.returncode != 0:
            raise ParityError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
        return result

    def is_shallow(self) -> bool:
        return self.run("rev-parse", "--is-shallow-repository").stdout.strip() == "true"

    def shallow_grafts(self) -> list[str]:
        path = Path(self.run("rev-parse", "--git-path", "shallow").stdout.strip())
        path = path if path.is_absolute() else self.root / path
        return path.read_text().split() if path.exists() else []

    def resolve(self, ref: str) -> str:
        return self.run("rev-parse", "--verify", f"{ref}^{{commit}}").stdout.strip()

    def has_commit(self, sha: str) -> bool:
        return self.run("cat-file", "-e", f"{sha}^{{commit}}", check=False).returncode == 0

    def is_ancestor(self, sha: str, head: str) -> bool:
        return self.run("merge-base", "--is-ancestor", sha, head, check=False).returncode == 0

    def patch_id(self, sha: str) -> str | None:
        diff = self.run("show", "--format=", "--no-color", "--no-ext-diff", sha).stdout
        if not diff.strip():
            return None
        result = subprocess.run(
            ["git", "-C", str(self.root), "patch-id", "--verbatim"],
            input=diff,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
        return result.stdout.split()[0] if result.stdout.strip() else None

    def message(self, sha: str) -> str:
        return self.run("log", "-1", "--format=%B", sha).stdout

    def diff_lines(self, sha: str) -> list[str]:
        diff = self.run("show", "--format=", "--no-color", "--no-ext-diff", "--unified=0", sha).stdout
        return [line for line in diff.splitlines() if line[:1] in "+-" and not line.startswith(("+++ ", "--- "))]

    def reverted(self, head: str) -> dict[str, tuple[str, ...]]:
        output = self.run("log", "--format=%H%x00%B%x1e", head, "--").stdout
        reverts: dict[str, tuple[str, ...]] = {}
        for record in output.split("\x1e"):
            sha, _, body = record.strip().partition("\x00")
            targets = tuple(REVERT.findall(body))
            if sha and targets:
                reverts[sha] = targets
        return reverts

    def added_lines(self, sha: str) -> dict[str, list[str]]:
        diff = self.run("show", "--format=", "--no-color", "--no-ext-diff", "--no-renames", "--unified=0", sha).stdout
        added: dict[str, list[str]] = {}
        path = None
        for line in diff.splitlines():
            if line.startswith("+++ "):
                target = line[4:]
                path = target[2:] if target.startswith("b/") else None
            elif line.startswith("+") and path is not None:
                added.setdefault(path, []).append(line[1:])
        return added

    def file_lines(self, head: str, path: str) -> set[str] | None:
        result = self.run("show", f"{head}:{path}", check=False)
        if result.returncode != 0:
            return None
        return {line.strip() for line in result.stdout.splitlines()}

    def rev_list(self, include: list[str], exclude: list[str]) -> list[Commit]:
        args = ["log", "--format=%H%x09%s", *include]
        if exclude:
            args += ["--not", *exclude]
        output = self.run(*args, "--").stdout
        commits = []
        for line in output.splitlines():
            sha, _, subject = line.partition("\t")
            commits.append(Commit(sha, subject))
        return commits


def retry(action, attempts: int = 4):
    delay = 2.0
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except (ParityError, urllib.error.URLError, TimeoutError, OSError) as error:
            if attempt == attempts:
                raise ParityError(f"network step failed after {attempts} attempts: {error}") from error
            time.sleep(delay)
            delay *= 2
    raise ParityError("unreachable")


def http_json(url: str, headers: dict[str, str]):
    request = urllib.request.Request(url, headers={"User-Agent": "upstream-parity-check", **headers})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def github_releases(repository: str) -> set[str]:
    headers = {"Accept": "application/vnd.github+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    names: set[str] = set()
    page = 1
    while True:
        url = f"https://api.github.com/repos/{repository}/releases?per_page=100&page={page}"
        batch = retry(lambda url=url: http_json(url, headers))
        if not isinstance(batch, list):
            raise ParityError(f"unexpected GitHub releases payload: {str(batch)[:200]}")
        names.update(item["tag_name"] for item in batch if not item.get("draft"))
        if len(batch) < 100:
            return names
        page += 1


def pypi_versions(package: str) -> set[str]:
    payload = retry(lambda: http_json(f"https://pypi.org/pypi/{package}/json", {"Accept": "application/json"}))
    return set(payload["releases"])


def collect(git: Git, upstream: dict, floor: str) -> Snapshot:
    url = f"https://github.com/{upstream['repository']}.git"
    listing = retry(lambda: git.run("ls-remote", "--tags", "--heads", url).stdout)
    releases, non_final, branches = parse_ls_remote(listing)
    retry(
        lambda: git.run(
            "fetch",
            "--quiet",
            "--no-tags",
            "--prune",
            url,
            f"+refs/tags/*:{NAMESPACE}/tags/*",
            f"+refs/heads/*:{NAMESPACE}/heads/*",
        )
    )
    for name, sha in {**releases, **branches}.items():
        if not git.has_commit(sha):
            raise ParityError(f"upstream {name} moved to {sha} during the check; re-run")
    floor_key = version_key(floor)
    tracked = [sha for name, sha in releases.items() if floor_key is None or (version_key(name) or ()) >= floor_key]
    for graft in git.shallow_grafts():
        if any(git.is_ancestor(graft, sha) for sha in [*tracked, *branches.values()]):
            raise ParityError(f"upstream history is cut at shallow commit {graft}; fetch full history")
    ordered = sorted(releases, key=lambda name: version_key(name) or ())
    ranges: dict[str, list[Commit]] = {}
    for index, name in enumerate(ordered):
        if floor_key is not None and (version_key(name) or ()) < floor_key:
            continue
        earlier = [releases[other] for other in ordered[:index]]
        ranges[name] = git.rev_list([releases[name]], earlier)
    release_shas = list(releases.values())
    dev_only = {name: git.rev_list([sha], release_shas) for name, sha in sorted(branches.items())}
    published = {
        "GitHub releases": github_releases(upstream["repository"]),
        "PyPI": pypi_versions(upstream["pypi"]),
    }
    return Snapshot(releases, non_final, branches, ranges, dev_only, published)


def read_versions(root: Path) -> dict[str, str]:
    versions = {}
    with open(root / VERSION_FILES[0], "rb") as handle:
        versions[VERSION_FILES[0]] = tomllib.load(handle)["project"]["version"]
    init = (root / VERSION_FILES[1]).read_text(encoding="utf-8")
    match = re.search(r'^__version__\s*=\s*"([^"]+)"', init, re.MULTILINE)
    versions[VERSION_FILES[1]] = match.group(1) if match else ""
    return versions


def parse_date(value, label: str, report: Report, field: str = "until") -> datetime.date | None:
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.date.fromisoformat(value)
        except ValueError:
            pass
    report.fail(f"{label}: `{field}` must be a YYYY-MM-DD date, got {value!r}")
    return None


def fork_commits_in_history(shas: list, label: str, git: Git, head: str, report: Report) -> bool:
    present = []
    for sha in shas:
        if not isinstance(sha, str) or not SHA.match(sha):
            report.fail(f"{label}: fork commit {sha!r} is not a full 40-hex SHA")
        elif not git.has_commit(sha):
            report.fail(f"{label}: fork commit {sha} does not exist in this repository")
        elif not git.is_ancestor(sha, head):
            hint = " (the repository is shallow; check out full history)" if git.is_shallow() else ""
            report.fail(f"{label}: fork commit {sha} is not in the history of {head}{hint}")
        else:
            present.append(sha)
    return len(present) == len(shas)


def check_divergence(entry: dict, label: str, git: Git, head: str, report: Report) -> None:
    if "diverged" not in entry and "divergence" not in entry:
        return
    diverged = entry.get("diverged")
    if not isinstance(diverged, list) or not diverged:
        report.fail(f"{label}: a divergence needs a non-empty `diverged` list of the fork commits that diverge")
    else:
        fork_commits_in_history(diverged, label, git, head, report)
    if not str(entry.get("divergence", "")).strip():
        report.fail(f"{label}: a `diverged` list needs a `divergence` text saying how the fork differs")


def named_fork_commits(entries) -> set[str]:
    named: set[str] = set()
    for entry in entries:
        if entry.get("status") != "ported":
            continue
        for field in ("fork", "diverged"):
            shas = entry.get(field)
            if isinstance(shas, list):
                named.update(sha for sha in shas if isinstance(sha, str) and SHA.match(sha))
    return named


def removed_lines(shas: set[str], git: Git, head: str) -> set[str]:
    return {
        line[1:].strip()
        for sha in sorted(shas)
        if git.has_commit(sha) and git.is_ancestor(sha, head)
        for line in git.diff_lines(sha)
        if line.startswith("-")
    }


def live_revert(sha: str, reverts: dict[str, tuple[str, ...]], seen: set[str]) -> str | None:
    if sha in seen:
        return None
    seen = seen | {sha}
    for reverter, targets in sorted(reverts.items()):
        if any(sha.startswith(target) for target in targets) and live_revert(reverter, reverts, seen) is None:
            return reverter
    return None


def normalise(line: str) -> str:
    return " ".join(line.split())


def check_adapted_lines(entry: dict, label: str, fork: list[str], git: Git, report: Report) -> None:
    carriers = [sha for sha in fork if git.diff_lines(sha)]
    if not carriers:
        report.fail(f"{label}: every fork commit is empty, so none carries upstream {entry['upstream'][:10]}'s lines")
        return
    carried = {normalise(line) for sha in carriers for lines in git.added_lines(sha).values() for line in lines}
    upstream = {
        normalise(line) for lines in git.added_lines(entry["upstream"]).values() for line in lines if line.strip()
    }
    excused: set[str] = set()
    for item in entry.get("diverged_lines", []):
        line = normalise(str(item.get("line", ""))) if isinstance(item, dict) else ""
        if not line:
            report.fail(f"{label}: a `diverged_lines` item needs a non-empty `line`")
            continue
        if not str(item.get("reason", "")).strip():
            report.fail(f"{label}: diverged line {line[:80]!r} needs a `reason`")
            continue
        if line not in upstream:
            report.fail(f"{label}: diverged line {line[:80]!r} is not an upstream added line")
        elif line in carried:
            report.fail(f"{label}: diverged line {line[:80]!r} is listed, but the fork commits carry it")
        excused.add(line)
    missing = sorted(upstream - carried - excused)
    if missing:
        report.fail(
            f"{label}: {len(missing)} line(s) upstream {entry['upstream'][:10]} added are in no non-empty fork"
            f" commit and not listed in `diverged_lines`, first {missing[0][:100]!r}"
        )


def check_still_applied(fork: list[str], removed: set[str], label: str, git: Git, head: str, report: Report) -> None:
    reverts = git.reverted(head)
    for sha in fork:
        reverter = live_revert(sha, reverts, set())
        if reverter:
            report.fail(f"{label}: fork commit {sha[:10]} is reverted in the history of {head[:10]} ({reverter[:10]})")
    for sha in fork:
        for path, lines in sorted(git.added_lines(sha).items()):
            present = git.file_lines(head, path) or set()
            gone = [
                line for line in lines if line.strip() and line.strip() not in present and line.strip() not in removed
            ]
            if gone:
                report.fail(
                    f"{label}: {len(gone)} line(s) fork commit {sha[:10]} added to {path} are gone at HEAD and no"
                    f" ledger `fork` or `diverged` commit removed them, first {gone[0].strip()[:100]!r}"
                )


def check_ported(entry: dict, label: str, git: Git, head: str, removed: set[str], report: Report) -> None:
    upstream_sha = entry["upstream"]
    fork = entry.get("fork")
    if not isinstance(fork, list) or not fork:
        report.fail(f"{label}: ported entry needs a non-empty `fork` list of fork commit SHAs")
        return
    if not fork_commits_in_history(fork, label, git, head, report):
        return
    check_still_applied(fork, removed, label, git, head, report)
    match = entry.get("match")
    if match not in MATCHES:
        report.fail(f"{label}: ported entry needs `match` = one of {', '.join(MATCHES)}")
        return
    if match == "patch-id":
        expected = git.patch_id(upstream_sha)
        if expected is None or expected not in {git.patch_id(sha) for sha in fork}:
            report.fail(f"{label}: no fork commit in {', '.join(s[:10] for s in fork)} has the upstream patch-id")
        return
    if not str(entry.get("reason", "")).strip():
        report.fail(f"{label}: an adapted port needs a `reason`")
    check_adapted_lines(entry, label, fork, git, report)
    cited = any(upstream_sha in FULL_SHA_TOKEN.findall(git.message(sha)) for sha in fork)
    if not cited:
        report.fail(
            f"{label}: an adapted port needs a fork commit whose message cites the full upstream SHA"
            f" {upstream_sha} (or an `Upstream: {upstream_sha}` trailer)"
        )


def check_version_only(entry: dict, label: str, release: str, git: Git, report: Report) -> None:
    lines = git.diff_lines(entry["upstream"])
    if not lines:
        report.fail(f"{label}: version-only commit has no diff to verify")
        return
    other = [line for line in lines if not VERSION_LINE.match(line)]
    if other:
        report.fail(f"{label}: marked version-only but changes more than version labels: {other[0][:120]!r}")
        return
    target = release.removeprefix("v")
    added = {match.group(2) for match in map(VERSION_LINE.match, lines) if match and match.group(1) == "+"}
    if added != {target}:
        report.fail(f"{label}: marked version-only but sets {sorted(added)}, not the release version {target}")


def check_entry(
    entry: dict,
    label: str,
    release: str,
    git: Git,
    head: str,
    today: datetime.date,
    removed: set[str],
    report: Report,
) -> None:
    status = entry.get("status")
    if status not in STATUSES:
        report.fail(f"{label}: status must be one of {', '.join(STATUSES)}, got {status!r}")
        return
    check_divergence(entry, label, git, head, report)
    if status == "ported":
        check_ported(entry, label, git, head, removed, report)
    elif status == "version-only":
        check_version_only(entry, label, release, git, report)
    else:
        for field in ("reason", "owner"):
            if not str(entry.get(field, "")).strip():
                report.fail(f"{label}: a {status} entry needs `{field}`")
    if status != "deferred":
        return
    until = parse_date(entry.get("until"), label, report)
    if until is None:
        return
    if today > until:
        report.fail(f"{label}: deferral expired on {until.isoformat()} ({entry.get('owner')}): port it or waive it")
    elif (until - today).days > MAX_DEFERRAL_DAYS:
        report.fail(f"{label}: deferral until {until.isoformat()} is more than {MAX_DEFERRAL_DAYS} days away")
    else:
        report.note(f"deferred until {until.isoformat()} ({(until - today).days} day(s) left), {label}")


def check_unreleased(
    rows: list,
    entries: dict[str, dict],
    owner_release: dict[str, str],
    snapshot: Snapshot,
    today: datetime.date,
    report: Report,
) -> set[str]:
    waived: set[str] = set()
    for row in rows:
        sha = row.get("upstream")
        if not isinstance(sha, str) or not SHA.match(sha):
            report.fail(f"ledger unreleased row has a malformed upstream SHA: {sha!r}")
            continue
        branch = str(row.get("branch", ""))
        label = f"unreleased {branch} {sha[:10]} {row.get('subject', '')!r}"
        if sha in entries or sha in waived:
            report.fail(f"upstream commit {sha[:10]} is listed twice in the ledger")
            continue
        waived.add(sha)
        if sha in owner_release:
            report.fail(
                f"{label}: {sha[:10]} now ships in upstream release {owner_release[sha]}; it is owed, move it to a"
                " [[commit]] row and port it"
            )
            continue
        if sha not in {commit.sha for commit in snapshot.dev_only.get(branch, [])}:
            report.fail(f"{label}: {sha[:10]} is not a commit only on upstream {branch or '(no branch)'}")
            continue
        if row.get("status") != "waived":
            report.fail(f"{label}: a commit on no upstream release must be waived, got {row.get('status')!r}")
        for field in ("reason", "owner"):
            if not str(row.get(field, "")).strip():
                report.fail(f"{label}: an unreleased waiver needs `{field}`")
        checked = parse_date(row.get("checked"), label, report, "checked")
        if checked is not None and checked > today:
            report.fail(f"{label}: checked {checked.isoformat()} is after today {today.isoformat()}")
        elif checked is not None:
            report.note(f"waived as non-release (checked {checked.isoformat()}): {label}")
    return waived


def check_version_claim(
    versions: dict[str, str],
    floor: str,
    snapshot: Snapshot,
    entries: dict[str, dict],
    report: Report,
) -> None:
    distinct = set(versions.values())
    if len(distinct) != 1:
        report.fail(f"version labels disagree: {versions}")
        return
    label = distinct.pop()
    base = label.split("+", 1)[0]
    base_key = version_key(base)
    if base_key is None or base.startswith("v"):
        report.fail(f"version {label!r} has no plain upstream release base")
        return
    claimed = [name for name in snapshot.releases if version_key(name) == base_key]
    if not claimed:
        report.fail(f"version {label!r} claims upstream {base}, which upstream never released")
        return
    if base_key < (version_key(floor) or ()):
        report.fail(f"version {label!r} claims {base}, below the ledger floor {floor}")
        return
    missing = []
    for name, commits in snapshot.ranges.items():
        if (version_key(name) or ()) > base_key:
            continue
        for commit in commits:
            status = entries.get(commit.sha, {}).get("status")
            if status not in COMPLETE:
                missing.append(f"{commit.sha[:10]} ({name}, {status or 'unlisted'})")
    if missing:
        report.fail(f"version {label!r} claims upstream {base} but lacks: {', '.join(missing)}")
    else:
        report.note(f"version {label!r} claims upstream {claimed[0]}; every commit through it is accounted for")
    ahead = sorted(
        (name for name in snapshot.releases if (version_key(name) or ()) > base_key),
        key=lambda name: version_key(name) or (),
    )
    if ahead:
        report.note(f"upstream releases newer than the claimed {base}: {', '.join(ahead)}")


def evaluate(
    ledger: dict,
    snapshot: Snapshot,
    git: Git,
    head: str,
    versions: dict[str, str],
    today: datetime.date,
) -> Report:
    report = Report()
    floor = str(ledger.get("upstream", {}).get("floor", ""))
    floor_key = version_key(floor)
    if floor_key is None:
        report.fail(f"ledger floor {floor!r} is not a release tag")
        return report
    if floor not in snapshot.releases:
        report.fail(f"ledger floor {floor} is not an upstream release tag")

    ledger_releases: dict[str, str] = {}
    for item in ledger.get("release", []):
        tag = item.get("tag")
        if tag in ledger_releases:
            report.fail(f"release {tag} is listed twice in the ledger")
        ledger_releases[tag] = item.get("commit")

    upstream_releases = {name: sha for name, sha in snapshot.releases.items() if (version_key(name) or ()) >= floor_key}
    for name in sorted(upstream_releases, key=lambda n: version_key(n) or ()):
        if name not in ledger_releases:
            report.fail(f"upstream release {name} ({upstream_releases[name][:10]}) is not in the ledger")
        elif ledger_releases[name] != upstream_releases[name]:
            report.fail(
                f"upstream release {name} now points at {upstream_releases[name]}, ledger pins {ledger_releases[name]}"
            )
    for name in ledger_releases:
        if name not in upstream_releases:
            report.fail(f"ledger release {name} is not an upstream release at or above {floor}")
    older = [name for name in snapshot.releases if (version_key(name) or ()) < floor_key]
    report.note(f"{len(older)} upstream release(s) below the floor {floor} are outside this ledger")

    entries: dict[str, dict] = {}
    for item in ledger.get("commit", []):
        sha = item.get("upstream")
        if not isinstance(sha, str) or not SHA.match(sha):
            report.fail(f"ledger commit entry has a malformed upstream SHA: {sha!r}")
            continue
        if sha in entries:
            report.fail(f"upstream commit {sha[:10]} is listed twice in the ledger")
        entries[sha] = item

    owner_release = {commit.sha: name for name, commits in snapshot.ranges.items() for commit in commits}
    removed = removed_lines(named_fork_commits(entries.values()), git, head)
    for name in sorted(snapshot.ranges, key=lambda n: version_key(n) or ()):
        for commit in snapshot.ranges[name]:
            entry = entries.get(commit.sha)
            label = f"{name} {commit.sha[:10]} {commit.subject!r}"
            if entry is None:
                report.fail(f"unknown upstream commit {label}: port it, or record it in the ledger")
                continue
            if entry.get("release") != name:
                report.fail(f"{label}: ledger files it under {entry.get('release')!r}, upstream ships it in {name}")
            check_entry(entry, label, name, git, head, today, removed, report)
    for sha, entry in entries.items():
        if sha not in owner_release:
            report.fail(f"ledger commit {sha[:10]} is not in any upstream release at or above {floor}")

    waived = check_unreleased(ledger.get("unreleased", []), entries, owner_release, snapshot, today, report)

    for source, published in sorted(snapshot.published.items()):
        for version in sorted(published):
            key = version_key(version)
            if key is None:
                report.note(f"{source} lists non-final version {version}")
                continue
            if key < floor_key:
                continue
            if not any(version_key(name) == key for name in snapshot.releases):
                report.fail(f"{source} published {version}, which has no upstream release tag")

    for name, sha in sorted(snapshot.non_final_tags.items()):
        report.note(f"non-final upstream tag {name} ({sha[:10]}) is not tracked; port only from final releases")
    for branch, commits in snapshot.dev_only.items():
        commits = [commit for commit in commits if commit.sha not in waived]
        if commits:
            listed = "; ".join(f"{c.sha[:10]} {c.subject}" for c in commits)
            report.note(f"dev-only on upstream {branch} ({len(commits)} commit(s), in no release): {listed}")
        else:
            report.note(f"upstream {branch} has no unrecorded commit outside a release")

    check_version_claim(versions, floor, snapshot, entries, report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fail when upstream releases are neither ported nor waived.")
    parser.add_argument("--ledger", type=Path, default=LEDGER)
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--today", type=datetime.date.fromisoformat, default=None)
    args = parser.parse_args(argv)
    today = args.today or datetime.datetime.now(datetime.UTC).date()
    git = Git(ROOT)
    try:
        with open(args.ledger, "rb") as handle:
            ledger = tomllib.load(handle)
        upstream = ledger.get("upstream", {})
        if not REPOSITORY.match(str(upstream.get("repository", ""))):
            raise ParityError("ledger [upstream].repository must be owner/name")
        if not str(upstream.get("pypi", "")).strip():
            raise ParityError("ledger [upstream].pypi must name the upstream package")
        head = git.resolve(args.head)
        snapshot = collect(git, upstream, str(upstream.get("floor", "")))
        report = evaluate(ledger, snapshot, git, head, read_versions(ROOT), today)
    except ParityError as error:
        print(f"FAIL: {error}")
        return 1
    shallow = " (shallow repository: fork ancestry checks fail closed)" if git.is_shallow() else ""
    print(f"upstream {upstream['repository']} on {today.isoformat()}, fork HEAD {head}{shallow}")
    for name, sha in sorted(snapshot.branches.items()):
        print(f"  branch {name} = {sha}")
    latest = max(snapshot.releases, key=lambda n: version_key(n) or ())
    print(f"  latest release {latest} = {snapshot.releases[latest]}")
    for note in report.notes:
        print(f"note: {note}")
    for failure in report.failures:
        print(f"FAIL: {failure}")
    if report.failures:
        print(f"upstream parity: {len(report.failures)} failure(s)")
        return 1
    print("upstream parity: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
