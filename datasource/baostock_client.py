from __future__ import annotations

from typing import List, Optional, Dict, Any

import pandas as pd
import baostock as bs


def normalize_symbol(symbol: str) -> str:
    s = symbol.strip().lower().replace("_", ".").replace("-", ".")
    return s


def map_freq_to_baostock(freq: str) -> str:
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


def ensure_dates(start_date: Optional[str], end_date: Optional[str]) -> tuple[str, str]:
    # Empty string -> BaoStock defaults (2015-01-01 to latest trading day)
    return start_date or "", end_date or ""


def baostock_login() -> None:
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock login failed: {lg.error_msg}")


def baostock_logout() -> None:
    try:
        bs.logout()
    except Exception:
        pass


def fetch_bars_df(symbol: str, start_date: str, end_date: str, freq: str, adjustflag: str) -> pd.DataFrame:
    if freq in {"5", "15", "30", "60"}:
        fields = [
            "date", "time", "code", "open", "high", "low", "close", "volume", "amount", "adjustflag",
        ]
    elif freq in {"w", "m"}:
        fields = [
            "date", "code", "open", "high", "low", "close", "volume", "amount", "adjustflag", "turn", "pctChg",
        ]
    else:
        fields = [
            "date", "code", "open", "high", "low", "close", "preclose", "volume", "amount", "adjustflag", "turn", "tradestatus", "pctChg", "isST",
        ]

    rs = bs.query_history_k_data_plus(
        code=symbol,
        fields=",".join(fields),
        start_date=start_date,
        end_date=end_date,
        frequency= freq,
        adjustflag=adjustflag,
    )
    if rs.error_code != "0":
        raise RuntimeError(f"baostock query failed: {rs.error_msg}")

    data_list: List[List[str]] = []
    while rs.error_code == "0" and rs.next():
        data_list.append(rs.get_row_data())

    df = pd.DataFrame(data_list, columns=fields)
    if df.empty:
        return df

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
        df["turn"] = pd.to_numeric(df["turn"].replace({"": "0"}), errors="coerce")
    if "pctChg" in df.columns:
        df["pctChg"] = pd.to_numeric(df["pctChg"], errors="coerce")
    if "tradestatus" in df.columns:
        df["tradestatus"] = pd.to_numeric(df["tradestatus"], errors="coerce")
    if "isST" in df.columns:
        df["isST"] = pd.to_numeric(df["isST"], errors="coerce")

    if "time" in df.columns and df["time"].notna().any():
        dt = df["date"].astype(str) + " " + df["time"].astype(str)
        df["datetime"] = pd.to_datetime(dt, errors="coerce")
    else:
        df["datetime"] = pd.to_datetime(df["date"], errors="coerce")

    return df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)


def to_rawbars(df: pd.DataFrame, symbol: str) -> List[Dict[str, Any]]:
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


