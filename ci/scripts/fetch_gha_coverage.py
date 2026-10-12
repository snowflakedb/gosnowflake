#!/usr/bin/env python3
"""Wait for Build and Test GHA coverage artifacts and download them.

Jenkins is triggered by the GitHub webhook at push/PR time, before GHA
finishes. This script polls the Actions API, then downloads every
coverage-* artifact for that commit. Codecov merges the files server-side;
they are not combined here.

Requires GITHUB_TOKEN (GitHub App installation token with actions:read).
Optional CHANGE_ID (PR number) so we query the PR head SHA, which is what
GHA pull_request workflow runs are keyed by.

Written for the snowos agent python3 (3.6).
"""
import argparse
import json
import os
import sys
import time
import zipfile
from io import BytesIO
from pathlib import Path
from http.client import IncompleteRead
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

OWNER = "snowflake-eng"
REPO = "gosnowflake"
WORKFLOW_FILE = "build-test.yml"
ARTIFACT_PREFIX = "coverage-"
API = "https://api.github.com/repos/{0}/{1}".format(OWNER, REPO)
POLL_SECONDS = 60
MAX_WAIT_SECONDS = int(os.environ.get("GHA_COVERAGE_MAX_WAIT_SECONDS", str(3 * 60 * 60)))
OUT_DIR = Path(os.environ.get("GHA_COVERAGE_DIR", "gha-coverage"))
COMMIT_SHA_FILE = OUT_DIR / "COMMIT_SHA"
# Distinct from 1 so Jenkins can retry with a fresh GitHub App token.
EXIT_NOT_READY = 2
ACTIVE_STATUSES = set(
    ["queued", "in_progress", "waiting", "pending", "requested", "waiting_for_change"]
)


class StripAuthRedirect(HTTPRedirectHandler):
    """GitHub artifact zip 302s to S3; S3 rejects the GitHub Authorization header."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return Request(newurl)


def token():
    value = os.environ.get("GITHUB_TOKEN", "").strip()
    if not value:
        sys.exit("GITHUB_TOKEN is required")
    return value


def api_request(url, accept="application/vnd.github+json"):
    req = Request(
        url,
        headers={
            "Authorization": "Bearer {0}".format(token()),
            "Accept": accept,
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "gosnowflake-jenkins-codecov",
        },
    )
    try:
        resp = urlopen(req, timeout=60)
        try:
            return resp.read()
        finally:
            resp.close()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        sys.exit("GitHub API {0} {1}: {2}".format(exc.code, url, body))


def api_json(url):
    return json.loads(api_request(url))


def paginate(url):
    items = []
    while url:
        req = Request(
            url,
            headers={
                "Authorization": "Bearer {0}".format(token()),
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "gosnowflake-jenkins-codecov",
            },
        )
        try:
            resp = urlopen(req, timeout=60)
            try:
                payload = json.loads(resp.read())
                if isinstance(payload, list):
                    items.extend(payload)
                elif isinstance(payload, dict):
                    for key in ("workflow_runs", "artifacts", "items"):
                        if key in payload and isinstance(payload[key], list):
                            items.extend(payload[key])
                            break
                link = resp.headers.get("Link", "")
                url = ""
                for part in link.split(","):
                    if 'rel="next"' in part:
                        url = part.split(";")[0].strip().strip("<>")
            finally:
                resp.close()
        except HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            hint = ""
            if exc.code == 403:
                hint = " (GitHub App likely needs actions:read to list workflow artifacts)"
            sys.exit("GitHub API {0} {1}: {2}{3}".format(exc.code, url, body, hint))
    return items


def resolve_head_sha():
    change_id = os.environ.get("CHANGE_ID", "").strip()
    if change_id:
        pr = api_json("{0}/pulls/{1}".format(API, change_id))
        sha = pr.get("head", {}).get("sha")
        if not sha:
            sys.exit("PR {0} has no head.sha".format(change_id))
        print("PR {0} head SHA {1}".format(change_id, sha), flush=True)
        return sha
    sha = os.environ.get("GIT_COMMIT", "").strip()
    if not sha:
        sys.exit("GIT_COMMIT or CHANGE_ID is required")
    print("branch SHA {0}".format(sha), flush=True)
    return sha


def list_runs(head_sha):
    url = "{0}/actions/workflows/{1}/runs?head_sha={2}&per_page=20".format(
        API, WORKFLOW_FILE, head_sha
    )
    return paginate(url)


def pick_run(runs):
    if not runs:
        return None
    runs = sorted(runs, key=lambda r: r.get("run_attempt", 1), reverse=True)
    runs = sorted(runs, key=lambda r: r.get("created_at") or "", reverse=True)
    active = [r for r in runs if r.get("status") in ACTIVE_STATUSES]
    if active:
        return active[0]
    return runs[0]


def inspect_run(head_sha):
    """Return the newest run, or None if GHA has not started yet."""
    return pick_run(list_runs(head_sha))


def run_is_ready(run):
    """True when artifacts can be downloaded. False when Jenkins should retry."""
    if run is None:
        print("no {0} run yet".format(WORKFLOW_FILE), flush=True)
        return False
    status = run.get("status")
    conclusion = run.get("conclusion")
    print(
        "run {0} attempt={1} status={2} conclusion={3}".format(
            run.get("id"), run.get("run_attempt"), status, conclusion
        ),
        flush=True,
    )
    if status in ACTIVE_STATUSES:
        return False
    if conclusion == "cancelled":
        print("run {0} cancelled; waiting for a replacement".format(run.get("id")), flush=True)
        return False
    print(
        "run {0} finished status={1} conclusion={2}".format(
            run.get("id"), status, conclusion
        ),
        flush=True,
    )
    return True


def wait_for_run(head_sha):
    deadline = time.time() + MAX_WAIT_SECONDS
    last_id = None
    while time.time() < deadline:
        try:
            run = inspect_run(head_sha)
        except SystemExit as exc:
            text = str(exc)
            if "GitHub API 401" in text or "GitHub API 403" in text:
                raise
            print("poll failed: {0}; retrying".format(exc), flush=True)
            time.sleep(POLL_SECONDS)
            continue
        except (HTTPError, URLError, OSError, IncompleteRead) as exc:
            print("poll failed: {0}; retrying".format(exc), flush=True)
            time.sleep(POLL_SECONDS)
            continue
        if run is None:
            print("no {0} run yet for {1}".format(WORKFLOW_FILE, head_sha), flush=True)
            time.sleep(POLL_SECONDS)
            continue
        run_id = run.get("id")
        if run_id != last_id:
            last_id = run_id
        if not run_is_ready(run):
            time.sleep(POLL_SECONDS)
            continue
        return run
    print(
        "timed out after {0}s waiting for {1} artifacts".format(
            MAX_WAIT_SECONDS, WORKFLOW_FILE
        ),
        flush=True,
    )
    return None


def download_zip(url):
    opener = build_opener(StripAuthRedirect())
    req = Request(
        url,
        headers={
            "Authorization": "Bearer {0}".format(token()),
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "gosnowflake-jenkins-codecov",
        },
    )
    resp = opener.open(req, timeout=120)
    try:
        return resp.read()
    finally:
        resp.close()


def download_artifacts(run):
    run_id = run["id"]
    artifacts = paginate("{0}/actions/runs/{1}/artifacts?per_page=100".format(API, run_id))
    wanted = [
        a
        for a in artifacts
        if str(a.get("name", "")).startswith(ARTIFACT_PREFIX) and not a.get("expired")
    ]
    if not wanted:
        names = [a.get("name") for a in artifacts]
        sys.exit(
            "run {0} has no {1}* artifacts (found {2})".format(
                run_id, ARTIFACT_PREFIX, names
            )
        )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for art in wanted:
        name = art["name"]
        dest = OUT_DIR / name
        dest.mkdir(parents=True, exist_ok=True)
        print("downloading {0} ({1} bytes)".format(name, art.get("size_in_bytes")), flush=True)
        blob = download_zip(art["archive_download_url"])
        zf = zipfile.ZipFile(BytesIO(blob))
        try:
            zf.extractall(str(dest))
        finally:
            zf.close()
        count += 1
    files = sorted(
        p
        for p in OUT_DIR.rglob("*")
        if p.is_file() and p.name != COMMIT_SHA_FILE.name
    )
    if not files:
        sys.exit(
            "downloaded {0} artifacts but no files under {1}".format(count, OUT_DIR)
        )
    print("downloaded {0} artifacts, {1} files:".format(count, len(files)), flush=True)
    for path in files:
        print("  {0}".format(path), flush=True)
    return count


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--once",
        action="store_true",
        help="Check once and exit 2 if artifacts are not ready (fresh GitHub App token per Jenkins poll).",
    )
    args = parser.parse_args(argv)
    head_sha = resolve_head_sha()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    COMMIT_SHA_FILE.write_text(head_sha + "\n")
    if args.once:
        try:
            run = inspect_run(head_sha)
        except SystemExit as exc:
            text = str(exc)
            if "GitHub API 401" in text or "GitHub API 403" in text:
                raise
            print("poll failed: {0}".format(exc), flush=True)
            return EXIT_NOT_READY
        except (HTTPError, URLError, OSError, IncompleteRead) as exc:
            print("poll failed: {0}".format(exc), flush=True)
            return EXIT_NOT_READY
        if not run_is_ready(run):
            return EXIT_NOT_READY
    else:
        run = wait_for_run(head_sha)
        if run is None:
            return EXIT_NOT_READY
    html = run.get("html_url", "")
    if html:
        print("workflow run {0}".format(html), flush=True)
    download_artifacts(run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
