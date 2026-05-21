# Claude Token Counter

Track **input + output + cache token costs** for every Claude Code conversation — directly inside VS Code.

Uses Claude Code hooks to read exact token usage from session files and writes a live KPI dashboard.

## What It Shows

```
=== Claude Token KPI — 2026-05-19 ===

  Monthly budget      :  $40.00  →  daily estimate $1.29

  Input tokens        :        131   $0.00039
  Output tokens       :     53,147   $0.79720
  Cache read tokens   :  8,972,976   $2.69189
  Cache write tokens  :    242,256   $0.90846
  ─────────────────────────────────────────────
  Cost today          :              $4.39795
  Daily budget used   :              340.8%
  Remaining today     :              $-3.1076
  Messages            :  16
```

And after every response in chat:
```
[██████████] 340.8% of $1.29/day ⚠ OVER BUDGET  |  ↑131 ↓53,147 tokens  |  $4.40 spent
```

## Requirements

- Python 3.10+ (Anaconda recommended)
- [`tokenizers`](https://pypi.org/project/tokenizers/) library: `pip install tokenizers`
- Claude Code (VS Code extension or CLI)
- [`claude-v3-tokenizer.json`](https://github.com/Jellyfishboy/claude-tokenizer) — place in the same directory

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/rohinisaravanan/token_counter.git ~/.claude/token_counter
pip install tokenizers
```

### 2. Download the Claude tokenizer vocab

```bash
curl -o ~/.claude/token_counter/claude-v3-tokenizer.json \
  https://raw.githubusercontent.com/Jellyfishboy/claude-tokenizer/master/src/claude-v3-tokenizer.json
```

### 3. Set your monthly budget

Edit `token_config.json`:
```json
{
  "monthly_budget_usd": 40
}
```

### 4. Add hooks to `~/.claude/settings.json`

Copy the contents of `settings_snippet.json` into your `~/.claude/settings.json`, replacing `/path/to/` with your actual paths.

Example:
```json
"hooks": {
  "UserPromptSubmit": [{
    "matcher": "",
    "hooks": [{
      "type": "command",
      "command": "/Users/yourname/anaconda3/bin/python3 /Users/yourname/.claude/token_counter/token_stop.py",
      "async": true
    }]
  }],
  "Stop": [{
    "matcher": "",
    "hooks": [{
      "type": "command",
      "command": "/Users/yourname/anaconda3/bin/python3 /Users/yourname/.claude/token_counter/token_stop.py"
    }]
  }]
}
```

### 5. Open the live KPI file in VS Code

Press `Cmd+P`, type `TOKEN_KPI.txt`, open it in a split pane. It updates automatically after every Claude response.

### 6. Restart Claude Code

## Files

| File | Purpose |
|------|---------|
| `token_tracker.py` | `UserPromptSubmit` hook — counts input tokens per message |
| `token_stop.py` | `Stop` hook — reads exact usage from session JSONL, updates KPI |
| `token_status.py` | Status line — shows live token count in Claude Code panel |
| `token_config.json` | Monthly budget + per-model pricing |
| `settings_snippet.json` | Hook config to paste into `~/.claude/settings.json` |

## Supported Models

| Model | Input | Output | Cache Read | Cache Write |
|-------|-------|--------|------------|-------------|
| claude-sonnet-4-6 | $3/1M | $15/1M | $0.30/1M | $3.75/1M |
| claude-haiku-4-5 | $0.25/1M | $1.25/1M | $0.03/1M | $0.30/1M |
| claude-opus-4-7 | $15/1M | $75/1M | $1.50/1M | $18.75/1M |


