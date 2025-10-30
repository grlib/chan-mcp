from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    from czsc import signals as czsc_signals
    from czsc.objects import RawBar
    from czsc.analyze import CZSC  # type: ignore
except Exception:  # pragma: no cover
    czsc_signals = None
    RawBar = None
    CZSC = None


def bars_to_rawbar_objs(bars: List[Dict[str, Any]]) -> List[RawBar]:
    if RawBar is None:
        raise RuntimeError("czsc is not installed; RawBar unavailable")
    rawbars: List[RawBar] = []
    for i, b in enumerate(bars):
        dt = pd.to_datetime(b["dt"]).to_pydatetime()
        rb = RawBar(
            symbol=b.get("code") or b["symbol"],
            dt=dt,
            id=i,
            open=b["open"],
            close=b["close"],
            high=b["high"],
            low=b["low"],
            vol=b.get("vol", 0.0),
            amount=b.get("amount", 0.0),
        )
        rawbars.append(rb)
    return rawbars


def serialize_dt(obj: Any) -> Any:
    if isinstance(obj, (datetime, pd.Timestamp)):
        return pd.to_datetime(obj).to_pydatetime().isoformat()
    return obj


def chan_basic_signals(df: pd.DataFrame, freq: str) -> List[Dict[str, Any]]:
    if czsc_signals is None:
        raise RuntimeError("czsc is not installed; see requirements.txt")
    df = df.copy()
    df["dt"] = pd.to_datetime(df["dt"])  # ensure dtype
    df = df.sort_values("dt").reset_index(drop=True)

    signals: List[Dict[str, Any]] = []
    try:
        fx_sig = czsc_signals.cxt_fxs_fx_is_inside_b1(
            k1=f"{freq}", k2="fx", k3="inside", di=1, m=3,
        )
        signals.append({"name": "cxt_fxs_fx_is_inside_b1", "value": fx_sig})
    except Exception:
        pass
    try:
        power_sig = czsc_signals.bar_zdt_power_V230313(
            k1=f"{freq}", k2="bar", k3="zdt_power", di=1, th=0.8,
        )
        signals.append({"name": "bar_zdt_power_V230313", "value": power_sig})
    except Exception:
        pass
    return signals


def analyze_structure(bars: List[Dict[str, Any]], level: str) -> Dict[str, Any]:
    if CZSC is None or RawBar is None:
        raise RuntimeError("czsc.analyze 未可用，请确认已安装 czsc >= 0.9.x")
    rawbars = bars_to_rawbar_objs(bars)
    analyzer = CZSC(rawbars)

    result_items: List[Dict[str, Any]] = []
    beichi: Optional[Dict[str, Any]] = None

    if level == "bi":
        bi_list = getattr(analyzer, "bi_list", []) or []
        for bi in bi_list:
            item: Dict[str, Any] = {}
            for attr in [
                "direction", "high", "low", "power", "length", "start_dt", "end_dt", "fx_a", "fx_b",
            ]:
                v = getattr(bi, attr, None)
                if v is None:
                    continue
                if attr in {"start_dt", "end_dt"}:
                    item[attr] = serialize_dt(v)
                elif attr in {"fx_a", "fx_b"}:
                    try:
                        item[attr] = {
                            "dt": serialize_dt(getattr(v, "dt", None)),
                            "price": float(getattr(v, "price", float("nan"))),
                            "fx": getattr(v, "fx", None),
                        }
                    except Exception:
                        item[attr] = str(v)
                else:
                    try:
                        item[attr] = float(v)
                    except Exception:
                        item[attr] = v
            if not item:
                item["text"] = str(bi)
            result_items.append(item)

        same_dir = [b for b in bi_list if getattr(b, "direction", None) in {"up", "down", "向上", "向下"}]
        last = None
        prev = None
        for b in reversed(same_dir):
            if last is None:
                last = b
            elif prev is None and getattr(b, "direction", None) == getattr(last, "direction", None):
                prev = b
                break
        if last is not None and prev is not None:
            try:
                last_len = abs(float(getattr(last, "high", 0)) - float(getattr(last, "low", 0)))
                prev_len = abs(float(getattr(prev, "high", 0)) - float(getattr(prev, "low", 0)))
                direction = getattr(last, "direction", None)
                bearish = direction in {"up", "向上"} and float(getattr(last, "high", 0)) <= float(getattr(prev, "high", 0)) and last_len < prev_len
                bullish = direction in {"down", "向下"} and float(getattr(last, "low", 0)) >= float(getattr(prev, "low", 0)) and last_len < prev_len
                if bearish or bullish:
                    beichi = {
                        "type": "bearish" if bearish else "bullish",
                        "last_start": serialize_dt(getattr(last, "start_dt", None)),
                        "last_end": serialize_dt(getattr(last, "end_dt", None)),
                        "prev_start": serialize_dt(getattr(prev, "start_dt", None)),
                        "prev_end": serialize_dt(getattr(prev, "end_dt", None)),
                    }
            except Exception:
                beichi = None

    else:
        zs_list = getattr(analyzer, "zs_list", []) or []
        for zs in zs_list:
            item = {}
            for attr in ["zd", "zg", "gg", "dd", "start_dt", "end_dt", "level", "direction"]:
                v = getattr(zs, attr, None)
                if v is None:
                    continue
                if attr in {"start_dt", "end_dt"}:
                    item[attr] = serialize_dt(v)
                else:
                    try:
                        item[attr] = float(v)
                    except Exception:
                        item[attr] = v
            if not item:
                item["text"] = str(zs)
            result_items.append(item)

    return {"items": result_items, "beichi": beichi}


