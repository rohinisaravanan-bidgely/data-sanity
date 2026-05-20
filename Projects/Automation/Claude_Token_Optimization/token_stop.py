#!/Users/rohinisaravanan/anaconda3/bin/python3
"""
Stop hook — reads exact usage from session JSONL, tracks input+output+cache per model,
computes cost against monthly budget, writes TOKEN_KPI.txt.
"""
import sys
import json
import calendar
from pathlib import Path
from datetime import date
from glob import glob

STATS_FILE    = Path.home() / '.claude' / 'token_stats.json'
MONTHLY_FILE  = Path.home() / '.claude' / 'token_monthly.json'   # accumulates across days
SESSION_FILE  = Path.home() / '.claude' / 'token_sessions.json'  # persists across days
KPI_FILE      = Path.home() / '.claude' / 'TOKEN_KPI.txt'
CONFIG_FILE   = Path.home() / '.claude' / 'token_config.json'
PROJECTS_DIR  = Path.home() / '.claude' / 'projects'

DEFAULT_PRICES = {"input": 0.003, "output": 0.015, "cache_write": 0.00375, "cache_read": 0.0003}


def load_config() -> dict:
    try:
        return json.loads(CONFIG_FILE.read_text())
    except Exception:
        return {"monthly_budget_usd": 40, "model_prices": {}}


def get_prices(model: str, config: dict) -> dict:
    for key, prices in config.get('model_prices', {}).items():
        if key in (model or ''):
            return prices
    return DEFAULT_PRICES


EMPTY_STATS = lambda: {
    'date': str(date.today()),
    'input_tokens': 0, 'output_tokens': 0,
    'cache_read_tokens': 0, 'cache_write_tokens': 0,
    'total_cost': 0.0, 'messages': 0,
    'sessions': {}
}

def load_stats() -> dict:
    base = EMPTY_STATS()
    if STATS_FILE.exists():
        try:
            s = json.loads(STATS_FILE.read_text())
            if s.get('date') == str(date.today()):
                base.update(s)
                return base
        except Exception:
            pass
    return base


def load_sessions() -> dict:
    """Persistent session line tracking — never resets."""
    if SESSION_FILE.exists():
        try:
            return json.loads(SESSION_FILE.read_text())
        except Exception:
            pass
    return {}


def save_sessions(s: dict):
    SESSION_FILE.write_text(json.dumps(s, indent=2))


def load_monthly() -> dict:
    today = date.today()
    month_key = f"{today.year}-{today.month:02d}"
    if MONTHLY_FILE.exists():
        try:
            m = json.loads(MONTHLY_FILE.read_text())
            if m.get('month') == month_key:
                return m
        except Exception:
            pass
    return {'month': month_key, 'total_cost': 0.0, 'last_scan': 0}


def save_monthly(m: dict):
    MONTHLY_FILE.write_text(json.dumps(m, indent=2))


def scan_all_sessions(config: dict) -> float:
    """Scan ALL project session files for current month — deduped by message ID."""
    month_prefix = f"{date.today().year}-{date.today().month:02d}"
    seen_msg_ids: set = set()
    total_cost = 0.0

    for jsonl_path in PROJECTS_DIR.glob('*/*.jsonl'):
        try:
            for line in jsonl_path.read_text(errors='replace').splitlines():
                try:
                    entry = json.loads(line)
                    ts = entry.get('timestamp', '')
                    if not ts.startswith(month_prefix):
                        continue
                    msg = entry.get('message', {})
                    if msg.get('role') != 'assistant' or 'usage' not in msg:
                        continue
                    msg_id = msg.get('id', '')
                    if msg_id and msg_id in seen_msg_ids:
                        continue
                    if msg_id:
                        seen_msg_ids.add(msg_id)
                    model = msg.get('model', 'unknown')
                    u = msg['usage']
                    p = get_prices(model, config)
                    inp = u.get('input_tokens', 0)
                    out = u.get('output_tokens', 0)
                    cr  = u.get('cache_read_input_tokens', 0)
                    cw  = u.get('cache_creation_input_tokens', 0)
                    raw = (
                        (inp/1000)*p['input'] + (out/1000)*p['output'] +
                        (cr/1000)*p['cache_read'] + (cw/1000)*p['cache_write']
                    )
                    total_cost += raw * config.get('billing_factor', 1.0)
                except Exception:
                    pass
        except Exception:
            pass
    return total_cost


def save_stats(s: dict):
    STATS_FILE.write_text(json.dumps(s, indent=2))


def find_session_file(session_id: str) -> Path | None:
    matches = glob(str(PROJECTS_DIR / '*' / f'{session_id}.jsonl'))
    return Path(matches[0]) if matches else None


def read_session_usage(session_file: Path, config: dict) -> tuple[int, dict]:
    usage_by_model = {}
    line_count = 0
    today = str(date.today())
    try:
        for line in session_file.read_text().splitlines():
            line_count += 1
            try:
                entry = json.loads(line)
                # Only count messages from today
                ts = entry.get('timestamp', '')
                if ts and not ts.startswith(today):
                    continue
                msg = entry.get('message', {})
                if msg.get('role') == 'assistant' and 'usage' in msg:
                    model = msg.get('model', 'unknown')
                    u = msg['usage']
                    p = get_prices(model, config)
                    if model not in usage_by_model:
                        usage_by_model[model] = {
                            'input_tokens': 0, 'output_tokens': 0,
                            'cache_read_tokens': 0, 'cache_write_tokens': 0, 'cost': 0.0
                        }
                    inp = u.get('input_tokens', 0)
                    out = u.get('output_tokens', 0)
                    cr  = u.get('cache_read_input_tokens', 0)
                    cw  = u.get('cache_creation_input_tokens', 0)
                    cost = (inp/1000)*p['input'] + (out/1000)*p['output'] + \
                           (cr/1000)*p['cache_read'] + (cw/1000)*p['cache_write']
                    t = usage_by_model[model]
                    t['input_tokens']       += inp
                    t['output_tokens']      += out
                    t['cache_read_tokens']  += cr
                    t['cache_write_tokens'] += cw
                    t['cost']               += cost
            except Exception:
                pass
    except Exception:
        pass
    return line_count, usage_by_model


def main():
    try:
        data = json.loads(sys.stdin.read())
        session_id = data.get('session_id', '')
    except Exception:
        session_id = ''

    config   = load_config()
    stats    = load_stats()
    sessions = load_sessions()  # persistent, never resets

    if session_id:
        session_file = find_session_file(session_id)
        if session_file:
            line_count, usage_by_model = read_session_usage(session_file, config)
            prev = sessions.get(session_id, {'line_count': 0, 'usage': {}})

            if line_count > prev.get('line_count', 0):
                for model, totals in usage_by_model.items():
                    prev_model = prev.get('usage', {}).get(model, {
                        'input_tokens': 0, 'output_tokens': 0,
                        'cache_read_tokens': 0, 'cache_write_tokens': 0, 'cost': 0.0
                    })
                    stats['input_tokens']      += max(0, totals.get('input_tokens', 0)       - prev_model.get('input_tokens', 0))
                    stats['output_tokens']     += max(0, totals.get('output_tokens', 0)      - prev_model.get('output_tokens', 0))
                    stats['cache_read_tokens'] += max(0, totals.get('cache_read_tokens', 0)  - prev_model.get('cache_read_tokens', 0))
                    stats['cache_write_tokens']+= max(0, totals.get('cache_write_tokens', 0) - prev_model.get('cache_write_tokens', 0))
                    stats['total_cost']        += max(0, totals.get('cost', 0.0)              - prev_model.get('cost', 0.0))

                stats['messages'] += 1
                sessions[session_id] = {'line_count': line_count, 'usage': usage_by_model}

    save_stats(stats)
    save_sessions(sessions)

    # Budget calculations
    monthly = config.get('monthly_budget_usd', 40)
    days_in_month = calendar.monthrange(date.today().year, date.today().month)[1]
    daily_budget  = monthly / days_in_month
    cost_today    = stats['total_cost']
    pct_daily     = (cost_today / daily_budget * 100) if daily_budget else 0

    # Monthly total — full scan of all sessions, cached for 1 hour
    import time
    monthly_data = load_monthly()
    now = time.time()
    if now - monthly_data.get('last_scan', 0) > 3600:
        monthly_data['total_cost'] = scan_all_sessions(config)
        monthly_data['last_scan']  = now
        save_monthly(monthly_data)

    cost_month      = monthly_data['total_cost']
    remaining_month = monthly - cost_month
    pct_month       = (cost_month / monthly * 100) if monthly else 0

    inp  = stats['input_tokens']
    out  = stats['output_tokens']
    cr   = stats['cache_read_tokens']
    cw   = stats['cache_write_tokens']
    msgs = stats['messages']

    p = DEFAULT_PRICES
    kpi_text = (
        f"=== Claude Token KPI — {date.today()} ===\n\n"
        f"  Monthly budget      :  ${monthly:.2f}  →  daily estimate ${daily_budget:.2f}\n\n"
        f"  Input tokens        : {inp:>10,}   ${(inp/1000)*p['input']:.5f}\n"
        f"  Output tokens       : {out:>10,}   ${(out/1000)*p['output']:.5f}\n"
        f"  Cache read tokens   : {cr:>10,}   ${(cr/1000)*p['cache_read']:.5f}\n"
        f"  Cache write tokens  : {cw:>10,}   ${(cw/1000)*p['cache_write']:.5f}\n"
        f"  ─────────────────────────────────────────────\n"
        f"  Cost today          :              ${cost_today:.4f}   ({pct_daily:.1f}% of daily)\n"
        f"  ─────────────────────────────────────────────\n"
        f"  Used this month     :              ${cost_month:.4f}   ({pct_month:.1f}% of ${monthly:.2f})\n"
        f"  Remaining (monthly) :              ${remaining_month:.4f}\n"
        f"  Messages today      :  {msgs}\n"
    )
    KPI_FILE.write_text(kpi_text)

    filled = min(10, int(pct_daily / 10))
    bar = '█' * filled + '░' * (10 - filled)
    over = ' ⚠ OVER BUDGET' if pct_daily > 100 else ''
    summary = f"[{bar}] {pct_daily:.1f}% of ${daily_budget:.2f}/day{over}  |  ↑{inp:,} ↓{out:,} tokens  |  ${cost_today:.4f} spent"
    print(json.dumps({"systemMessage": summary}))


if __name__ == '__main__':
    main()
