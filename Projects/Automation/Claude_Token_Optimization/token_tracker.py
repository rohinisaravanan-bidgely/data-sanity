#!/Users/rohinisaravanan/anaconda3/bin/python3
"""
Called by Claude Code's UserPromptSubmit hook on every message.
Reads prompt from stdin JSON, counts tokens, accumulates daily stats.
"""
import sys
import json
from pathlib import Path
from datetime import date

sys.path.insert(0, '/Users/rohinisaravanan/Documents/Claude')
import claude_tokenizer

STATS_FILE = Path.home() / '.claude' / 'token_stats.json'

# Prices per 1K input tokens (update if model changes)
PRICES = {
    'claude-sonnet-4-6':        0.003,
    'claude-haiku-4-5':         0.00025,
    'claude-opus-4-7':          0.015,
}
DEFAULT_PRICE = PRICES['claude-sonnet-4-6']


def load_stats():
    if STATS_FILE.exists():
        try:
            return json.loads(STATS_FILE.read_text())
        except Exception:
            pass
    return {'date': str(date.today()), 'input_tokens': 0, 'cost': 0.0, 'messages': 0}


def save_stats(stats):
    STATS_FILE.write_text(json.dumps(stats, indent=2))


def main():
    try:
        data = json.loads(sys.stdin.read())
        prompt = data.get('prompt', '')
    except Exception:
        return

    if not prompt.strip():
        return

    token_count = claude_tokenizer.count_tokens(prompt)
    cost = (token_count / 1000) * DEFAULT_PRICE

    stats = load_stats()
    if stats.get('date') != str(date.today()):
        stats = {'date': str(date.today()), 'input_tokens': 0, 'cost': 0.0, 'messages': 0}

    stats['input_tokens'] += token_count
    stats['cost'] += cost
    stats['messages'] += 1
    save_stats(stats)


if __name__ == '__main__':
    main()
