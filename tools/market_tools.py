from __future__ import annotations

from typing import Literal, Optional

import pandas as pd

from server import mcp
from datasource.baostock_client import (
    normalize_symbol,
    ensure_dates,
    map_freq_to_baostock,
    baostock_login,
    baostock_logout,
    fetch_bars_df,
    to_rawbars,
)
from analysis.czsc_analysis import chan_basic_signals, analyze_structure


# -------------
# Direct-call API (non-decorated)
# -------------
def get_bars_local(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    freq: Literal["5m", "15m", "30m", "60m", "d", "w", "m"] = "d",
    adjustflag: Literal["1", "2", "3"] = "3",
) -> dict:
    s = normalize_symbol(symbol)
    sd, ed = ensure_dates(start_date, end_date)
    bo_freq = map_freq_to_baostock(freq)

    baostock_login()
    try:
        df = fetch_bars_df(s, sd, ed, bo_freq, adjustflag)
    finally:
        baostock_logout()

    bars = to_rawbars(df, s)
    return {
        "symbol": s,
        "freq": freq,
        "start_date": sd,
        "end_date": ed,
        "count": len(bars),
        "bars": bars,
    }


def chan_signals_local(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    freq: Literal["5m", "15m", "30m", "60m", "d", "w", "m"] = "d",
    adjustflag: Literal["1", "2", "3"] = "3",
) -> dict:
    payload = get_bars_local(symbol, start_date, end_date, freq, adjustflag)
    bars = payload["bars"]
    if not bars:
        return {"symbol": symbol, "freq": freq, "signals": [], "count": 0}
    import pandas as pd  # local to avoid import cycles
    df = pd.DataFrame(bars).sort_values("dt").reset_index(drop=True)
    signals = chan_basic_signals(df[["dt", "open", "high", "low", "close"]], freq)
    return {
        "symbol": payload["symbol"],
        "freq": freq,
        "count": len(df),
        "signals": signals,
    }


def chan_structure_local(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    freq: Literal["5m", "15m", "30m", "60m", "d", "w", "m"] = "d",
    adjustflag: Literal["1", "2", "3"] = "3",
    level: Literal["bi", "zs"] = "bi",
) -> dict:
    payload = get_bars_local(symbol, start_date, end_date, freq, adjustflag)
    bars = payload["bars"]
    if not bars:
        return {"symbol": symbol, "freq": freq, "level": level, "items": [], "beichi": None}
    struct = analyze_structure(bars, level)
    return {
        "symbol": payload["symbol"],
        "freq": freq,
        "level": level,
        "count": len(bars),
        **struct,
    }


@mcp.tool()
def get_bars(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    freq: Literal["5m", "15m", "30m", "60m", "d", "w", "m"] = "d",
    adjustflag: Literal["1", "2", "3"] = "3",
) -> dict:
    return get_bars_local(symbol, start_date, end_date, freq, adjustflag)


@mcp.tool()
def chan_signals(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    freq: Literal["5m", "15m", "30m", "60m", "d", "w", "m"] = "d",
    adjustflag: Literal["1", "2", "3"] = "3",
) -> dict:
    return chan_signals_local(symbol, start_date, end_date, freq, adjustflag)


@mcp.tool()
def chan_structure(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    freq: Literal["5m", "15m", "30m", "60m", "d", "w", "m"] = "d",
    adjustflag: Literal["1", "2", "3"] = "3",
    level: Literal["bi", "zs"] = "bi",
) -> dict:
    return chan_structure_local(symbol, start_date, end_date, freq, adjustflag, level)


