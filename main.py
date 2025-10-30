from __future__ import annotations

import os
from datetime import datetime
from typing import List, Optional, Literal, Dict, Any

import pandas as pd

# Data source
import baostock as bs

# Chan analysis
try:
    from czsc import signals as czsc_signals
    from czsc.objects import RawBar
except Exception:  # pragma: no cover
    czsc_signals = None
    RawBar = None
    
from fastmcp import FastMCP

mcp = FastMCP("chan-mcp")


def _normalize_symbol(symbol: str) -> str:
    # baostock uses sh.600000 / sz.000001 formats
    s = symbol.strip().lower().replace("_", ".").replace("-", ".")
    return s


def _map_freq_to_baostock(freq: str) -> str:
    f = str(freq).lower()
    mapping = {
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "60m": "60",
        "d": "d",
        "w": "w",
        "m": "m",
        "day": "d",
        "daily": "d",
    }
    if f in mapping:
        return mapping[f]
    if f.endswith("m") and f[:-1].isdigit() and f in {"5m", "15m", "30m", "60m"}:
        return f[:-1]
    if f in {"5", "15", "30", "60", "d", "w", "m"}:
        return f
    raise ValueError(f"Unsupported freq: {freq}")


def _ensure_dates(start_date: Optional[str], end_date: Optional[str]) -> tuple[str, str]:
    # Per BaoStock: empty string uses defaults (2015-01-01 to latest trading day)
    sd = start_date or ""
    ed = end_date or ""
    return sd, ed


def _baostock_login() -> None:
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock login failed: {lg.error_msg}")


def _baostock_logout() -> None:
    try:
        bs.logout()
    except Exception:
        pass


def _fetch_bars_df(symbol: str, start_date: str, end_date: str, freq: str, adjustflag: str) -> pd.DataFrame:
    # Select fields per BaoStock docs
    if freq in {"5", "15", "30", "60"}:  # minute
        fields = [
            "date",
            "time",
            "code",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "adjustflag",
        ]
    elif freq in {"w", "m"}:  # week / month
        fields = [
            "date",
            "code",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "adjustflag",
            "turn",
            "pctChg",
        ]
    else:  # daily
        fields = [
            "date",
            "code",
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
            "amount",
            "adjustflag",
            "turn",
            "tradestatus",
            "pctChg",
            "isST",
        ]

    query_fields = ",".join(fields)
    rs = bs.query_history_k_data_plus(
        code=symbol,
        fields=query_fields,
        start_date=start_date,
        end_date=end_date,
        frequency=freq,
        adjustflag=adjustflag,  # 3: 不复权; 1: 后复权; 2: 前复权
    )
    if rs.error_code != "0":
        raise RuntimeError(f"baostock query failed: {rs.error_msg}")

    data_list: List[List[str]] = []
    while rs.error_code == "0" and rs.next():
        data_list.append(rs.get_row_data())

    df = pd.DataFrame(data_list, columns=fields)
    if df.empty:
        return df

    # Normalize types
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    if "preclose" in df.columns:
        df["preclose"] = pd.to_numeric(df.get("preclose"), errors="coerce")
    if "turn" in df.columns:
        # empty string -> 0
        df["turn"] = pd.to_numeric(df["turn"].replace({"": "0"}), errors="coerce")
    if "pctChg" in df.columns:
        df["pctChg"] = pd.to_numeric(df["pctChg"], errors="coerce")
    if "tradestatus" in df.columns:
        df["tradestatus"] = pd.to_numeric(df["tradestatus"], errors="coerce")
    if "isST" in df.columns:
        df["isST"] = pd.to_numeric(df["isST"], errors="coerce")

    # Compose datetime
    if "time" in df.columns and df["time"].notna().any():
        # time format: YYYYMMDDHHMMSSsss (string), combine with date for robustness
        dt = df["date"].astype(str) + " " + df["time"].astype(str)
        # Let pandas infer, it's robust enough for our use
        df["datetime"] = pd.to_datetime(dt, errors="coerce")
    else:
        df["datetime"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    return df


def _to_rawbars(df: pd.DataFrame, symbol: str) -> List[Dict[str, Any]]:
    # Avoid requiring czsc classes at runtime for transport; emit plain dicts
    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        rows.append(
            {
                "symbol": symbol,
                "code": str(r.get("code", symbol)),
                "dt": r["datetime"].to_pydatetime().isoformat(),
                "open": float(r["open"]),
                "close": float(r["close"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "vol": float(r["volume"]),
                "amount": float(r.get("amount", 0.0)),
                "extra": {
                    k: (None if pd.isna(v) else float(v) if k in {"preclose", "turn", "pctChg"} else int(v) if k in {"tradestatus", "isST"} else v)
                    for k, v in {
                        "preclose": r.get("preclose"),
                        "turn": r.get("turn"),
                        "pctChg": r.get("pctChg"),
                        "tradestatus": r.get("tradestatus"),
                        "isST": r.get("isST"),
                    }.items()
                    if k in df.columns
                },
            }
        )
    return rows


@mcp.tool()
def get_bars(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    freq: Literal["5m", "15m", "30m", "60m", "d", "w", "m"] = "d",
    adjustflag: Literal["1", "2", "3"] = "3",
) -> dict:
    """Fetch K-line data from Baostock.

    - symbol: e.g., sh.600000 / sz.000001
    - start_date/end_date: YYYY-MM-DD; empty uses BaoStock defaults (2015-01-01 to latest)
    - freq: one of 5m,15m,30m,60m,d,w,m
    - adjustflag: 3 不复权, 1 后复权, 2 前复权
    """
    s = _normalize_symbol(symbol)
    sd, ed = _ensure_dates(start_date, end_date)
    bo_freq = _map_freq_to_baostock(freq)

    _baostock_login()
    try:
        df = _fetch_bars_df(s, sd, ed, bo_freq, adjustflag)
    finally:
        _baostock_logout()

    bars = _to_rawbars(df, s)
    return {
        "symbol": s,
        "freq": freq,
        "start_date": sd,
        "end_date": ed,
        "count": len(bars),
        "bars": bars,
    }


@mcp.tool()
def chan_signals(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    freq: Literal["5m", "15m", "30m", "60m", "d", "w", "m"] = "d",
    adjustflag: Literal["1", "2", "3"] = "3",
) -> dict:
    """Compute basic Chan-related signals using czsc.

    Note: This returns a small subset of classic signals as an example. Extend as needed.
    """
    if czsc_signals is None:
        raise RuntimeError("czsc is not installed; see requirements.txt")

    bars_payload = get_bars(symbol, start_date, end_date, freq, adjustflag)
    bars = bars_payload["bars"]
    if not bars:
        return {"symbol": symbol, "freq": freq, "signals": [], "count": 0}

    # Convert to a minimal DataFrame expected by many czsc signal helpers
    df = pd.DataFrame(bars)
    df["dt"] = pd.to_datetime(df["dt"])  # ensure dtype
    df = df.sort_values("dt").reset_index(drop=True)

    # Example signals: fractal (分型) and bar power
    signals: List[Dict[str, Any]] = []

    try:
        # cxt_fxs: fractal up/down within a window
        fx_sig = czsc_signals.cxt_fxs_fx_is_inside_b1(
            k1=f"{freq}", k2="fx", k3="inside",
            di=1, m=3,
        )
        signals.append({"name": "cxt_fxs_fx_is_inside_b1", "value": fx_sig})
    except Exception:
        pass

    try:
        power_sig = czsc_signals.bar_zdt_power_V230313(
            k1=f"{freq}", k2="bar", k3="zdt_power",
            di=1, th=0.8,
        )
        signals.append({"name": "bar_zdt_power_V230313", "value": power_sig})
    except Exception:
        pass

    return {
        "symbol": bars_payload["symbol"],
        "freq": freq,
        "count": len(df),
        "signals": signals,
    }


if __name__ == "__main__":
    # Host and port can be customized via env
    mcp.run(transport="http", port=8000)

