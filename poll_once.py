"""One-shot poll of the Algothon leaderboard API, meant to be run on a
schedule (GitHub Actions) rather than as a long-lived process.

Storage is plain JSON files under data/ instead of SQLite so each run's
changes are a readable, diffable git commit:

  data/snapshots.jsonl  - append-only log, one line per changed team snapshot
  data/windows.json     - submission_code -> evaluation window detail
  data/latest.json      - prebuilt payload the dashboard fetches directly
                           (same shape as the local tracker's /api/data)

Run it, then git add/commit/push data/ if anything changed - see
.github/workflows/poll.yml.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

API_BASE = "https://algothon-backend-26.vercel.app"
DATA_DIR = Path(__file__).parent / "data"
SNAPSHOTS_PATH = DATA_DIR / "snapshots.jsonl"
WINDOWS_PATH = DATA_DIR / "windows.json"
LATEST_PATH = DATA_DIR / "latest.json"
USER_AGENT = "algothon-tracker/1.0"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_json(url: str, timeout: float = 15.0) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def load_snapshots() -> list[dict]:
    if not SNAPSHOTS_PATH.exists():
        return []
    with SNAPSHOTS_PATH.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def load_windows() -> dict:
    if not WINDOWS_PATH.exists():
        return {}
    return json.loads(WINDOWS_PATH.read_text(encoding="utf-8"))


def latest_by_team(snapshots: list[dict]) -> dict:
    latest = {}
    for row in snapshots:
        team_id = row["team_id"]
        if team_id not in latest or row["poll_ts"] > latest[team_id]["poll_ts"]:
            latest[team_id] = row
    return latest


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    snapshots = load_snapshots()
    windows = load_windows()
    latest = latest_by_team(snapshots)

    ts = now_iso()
    poll_ok = True
    poll_error = None
    entries: list[dict] = []
    try:
        data = fetch_json(f"{API_BASE}/api/leaderboard")
        entries = data.get("leaderboard") or data.get("entries") or []
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        poll_ok = False
        poll_error = str(exc)

    new_rows = []
    new_codes = []
    for e in entries:
        team_id = e.get("teamId")
        prev = latest.get(team_id)
        is_new = prev is None
        is_changed = prev is not None and (
            prev.get("rank") != e.get("rank")
            or prev.get("score") != e.get("score")
            or prev.get("status") != e.get("status")
            or prev.get("submission_code") != e.get("submissionCode")
        )
        if is_new or is_changed:
            row = {
                "poll_ts": ts,
                "team_id": team_id,
                "team_name": e.get("teamName"),
                "rank": e.get("rank"),
                "score": e.get("score"),
                "mean_pl": e.get("meanPl"),
                "std_pl": e.get("stdPl"),
                "trade_count": e.get("tradeCount"),
                "runtime_ms": e.get("runtimeMs"),
                "status": e.get("status"),
                "submission_status": e.get("submissionStatus"),
                "submission_code": e.get("submissionCode"),
                "submitted_at": e.get("submittedAt"),
                "evaluated_at": e.get("evaluatedAt"),
            }
            new_rows.append(row)
            latest[team_id] = row
        code = e.get("submissionCode")
        if code and code not in windows:
            new_codes.append((code, team_id, e.get("teamName")))

    if new_rows:
        with SNAPSHOTS_PATH.open("a", encoding="utf-8") as f:
            for row in new_rows:
                f.write(json.dumps(row) + "\n")

    def fetch_window(item: tuple[str, str, str]):
        code, team_id, team_name = item
        try:
            sub = fetch_json(f"{API_BASE}/api/submissions/{code}")
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            return None
        w = sub.get("evaluationWindow") or {}
        return code, {
            "submission_code": code,
            "team_id": team_id,
            "team_name": team_name,
            "window_name": w.get("name"),
            "window_start": w.get("startDayIndex"),
            "window_end": w.get("endDayIndex"),
            "fetched_at": now_iso(),
        }

    if new_codes:
        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = [pool.submit(fetch_window, item) for item in new_codes]
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    code, w = result
                    windows[code] = w
        WINDOWS_PATH.write_text(json.dumps(windows, indent=0, sort_keys=True), encoding="utf-8")

    # Rebuild latest.json from the full log so it's always self-consistent,
    # even if a previous run crashed mid-write.
    snapshots = load_snapshots()
    by_team: dict[str, list[dict]] = {}
    for row in snapshots:
        by_team.setdefault(row["team_id"], []).append(row)
    for rows in by_team.values():
        rows.sort(key=lambda r: r["poll_ts"])

    teams = []
    for team_id, rows in by_team.items():
        team = dict(rows[-1])
        team["history"] = rows
        team["evaluationWindow"] = windows.get(team.get("submission_code"))
        teams.append(team)
    teams.sort(key=lambda t: (t.get("rank") is None, t.get("rank")))

    recent_events = sorted(snapshots, key=lambda r: r["poll_ts"], reverse=True)[:100]
    recent_events = [dict(ev, evaluationWindow=windows.get(ev.get("submission_code"))) for ev in recent_events]

    payload = {
        "generatedAt": now_iso(),
        "lastPoll": {"poll_ts": ts, "ok": poll_ok, "error": poll_error, "team_count": len(entries)},
        "teams": teams,
        "recentEvents": recent_events,
    }
    LATEST_PATH.write_text(json.dumps(payload), encoding="utf-8")
    print(f"[{ts}] polled {len(entries)} teams, {len(new_rows)} changed, ok={poll_ok}", flush=True)
    if not poll_ok:
        print(f"[{ts}] poll error: {poll_error}", flush=True)


if __name__ == "__main__":
    main()
