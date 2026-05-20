#!/Users/rohinisaravanan/anaconda3/bin/python3
"""
Read by Claude Code's statusLine to display live token KPI in VS Code.
"""
import json
from pathlib import Path
from datetime import date

STATS_FILE = Path.home() / '.claude' / 'token_stats.json'
DAILY_BUDGET = 1_000_000  # adjust to your actual quota


def main():
    if not STATS_FILE.exists():
        print('Tokens: 0 | $0.0000 | 0 msgs')
        return

    try:
        stats = json.loads(STATS_FILE.read_text())
    except Exception:
        print('Tokens: ? | error reading stats')
        return

    if stats.get('date') != str(date.today()):
        print('Tokens: 0 | $0.0000 | 0 msgs today')
        return

    tokens = stats.get('input_tokens', 0)
    cost   = stats.get('cost', 0.0)
    msgs   = stats.get('messages', 0)
    pct    = tokens / DAILY_BUDGET * 100

    print(f'Tokens: {tokens:,} ({pct:.1f}%) | ${cost:.4f} | {msgs} msgs')


if __name__ == '__main__':
    main()
