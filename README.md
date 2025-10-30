## chan-mcp (FastMCP + czsc + Baostock)

A lightweight MCP server that:
- Fetches K-line data from Baostock
- Computes basic Chan-related signals via `czsc`

References: [czsc repo](https://github.com/waditu/czsc)

### Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

If Baostock fails due to network, retry with a VPN or mirror.

### Run

```bash
python main.py
```

Environment overrides:
- `MCP_HOST` (default `127.0.0.1`)
- `MCP_PORT` (default `8000`)

### Tools

1) get_bars
- Inputs: `symbol` (e.g., `sh.600000`), `start_date`, `end_date` (`YYYY-MM-DD`), `freq` in `{5m,15m,30m,60m,d}`
- Output: JSON containing bars with `open/high/low/close/vol/amount` and ISO `dt`.

2) chan_signals
- Inputs: same as `get_bars`
- Output: small subset of Chan-style signals computed via `czsc.signals`.

Extend `chan_signals` to include more `czsc` signals per your needs.


