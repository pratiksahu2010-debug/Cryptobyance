"""Auditable self-learning journal for delivered signals and rejected setups.

The journal is observational: it never changes strategy rules or places orders.
"""
from __future__ import annotations

import json
import math
from typing import Any

from strategy import indicators, ADX_MIN, ATR_STOP_MULT, MIN_RR

JOURNAL_DDL = """
CREATE TABLE IF NOT EXISTS journal_feedback (
    id BIGSERIAL PRIMARY KEY,
    signal_id BIGINT NOT NULL REFERENCES signal_history(id) ON DELETE RESTRICT,
    category TEXT NOT NULL CHECK (category IN (
      'GOOD_ENTRY','BAD_SIGNAL','LATE_ENTRY','MISSED_OPPORTUNITY',
      'STOP_LOSS_TOO_TIGHT','TARGET_TOO_CLOSE','WRONG_DIRECTION','NEEDS_REVIEW'
    )),
    comment TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'dashboard' CHECK (source IN ('dashboard','telegram','api')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_journal_feedback_signal_created
 ON journal_feedback(signal_id,created_at DESC);
CREATE TABLE IF NOT EXISTS journal_candidates (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('LONG','SHORT')),
    candle_open_ms BIGINT NOT NULL,
    entry DOUBLE PRECISION NOT NULL,
    stop DOUBLE PRECISION NOT NULL,
    target DOUBLE PRECISION NOT NULL,
    risk_per_unit DOUBLE PRECISION NOT NULL CHECK(risk_per_unit > 0),
    snapshot JSONB NOT NULL,
    rejection_reasons JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN'
      CHECK(status IN ('OPEN','TARGET','STOP_LOSS','AMBIGUOUS')),
    best_favorable_r DOUBLE PRECISION NOT NULL DEFAULT 0,
    worst_adverse_r DOUBLE PRECISION NOT NULL DEFAULT 0,
    bars_observed INTEGER NOT NULL DEFAULT 0,
    last_observed_candle_open_ms BIGINT NOT NULL,
    data_gap BOOLEAN NOT NULL DEFAULT FALSE,
    exit_candle_open_ms BIGINT,
    realized_r DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(symbol,direction,candle_open_ms)
);
CREATE INDEX IF NOT EXISTS ix_journal_candidates_open
 ON journal_candidates(symbol,status,last_observed_candle_open_ms);
CREATE TABLE IF NOT EXISTS journal_candidate_feedback (
    id BIGSERIAL PRIMARY KEY,
    candidate_id BIGINT NOT NULL REFERENCES journal_candidates(id) ON DELETE RESTRICT,
    category TEXT NOT NULL CHECK (category IN ('MISSED_OPPORTUNITY','NEEDS_REVIEW')),
    comment TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'dashboard' CHECK (source IN ('dashboard','telegram','api')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_journal_candidate_feedback_created
 ON journal_candidate_feedback(candidate_id,created_at DESC);
"""

FEEDBACK_CATEGORIES = {
    "GOOD_ENTRY", "BAD_SIGNAL", "LATE_ENTRY", "MISSED_OPPORTUNITY",
    "STOP_LOSS_TOO_TIGHT", "TARGET_TOO_CLOSE", "WRONG_DIRECTION", "NEEDS_REVIEW",
}


def rejected_observation(df5, df15) -> dict[str, Any] | None:
    """Create a conservative, point-in-time rejected-setup snapshot, if meaningful."""
    if df5 is None or len(df5) < 60:
        return None
    frame = indicators(df5.copy())
    if len(frame) < 35:
        return None
    c = frame.iloc[-1]
    fields = ("ema9", "ema21", "rsi", "adx", "plus_di", "minus_di", "atr", "vwap", "close")
    values = {name: float(c[name]) for name in fields}
    if not all(math.isfinite(v) for v in values.values()) or values["atr"] <= 0:
        return None
    long_hint = values["ema9"] > values["ema21"] and values["close"] > values["vwap"] and values["rsi"] > 50
    short_hint = values["ema9"] < values["ema21"] and values["close"] < values["vwap"] and values["rsi"] < 50
    if not (long_hint or short_hint):
        return None
    side = "LONG" if long_hint else "SHORT"
    reasons = []
    if values["adx"] < ADX_MIN:
        reasons.append("adx_below_minimum")
    if (side == "LONG" and values["plus_di"] <= values["minus_di"]) or (side == "SHORT" and values["minus_di"] <= values["plus_di"]):
        reasons.append("directional_index_mismatch")
    alignment_age = 0
    for row in frame.tail(6).iloc[::-1].itertuples(index=False):
        aligned = row.ema9 > row.ema21 if side == "LONG" else row.ema9 < row.ema21
        if not bool(aligned):
            break
        alignment_age += 1
    if alignment_age > 3:
        reasons.append("ema_alignment_too_old")
    prior = frame.iloc[-4]
    fast_slope = (values["ema9"] - float(prior.ema9)) / values["atr"]
    slow_slope = (values["ema21"] - float(prior.ema21)) / values["atr"]
    extension = (values["close"] - values["vwap"]) / values["atr"]
    if side == "LONG" and (fast_slope <= 0 or slow_slope <= 0):
        reasons.append("ema_slope_not_rising")
    if side == "SHORT" and (fast_slope >= 0 or slow_slope >= 0):
        reasons.append("ema_slope_not_falling")
    if abs(extension) > 1.0:
        reasons.append("entry_extended_from_vwap")
    context = None
    if df15 is None or len(df15) < 35:
        reasons.append("15m_context_unavailable")
    else:
        ctx = indicators(df15.copy()).iloc[-1]
        if not math.isfinite(float(ctx.ema9)) or not math.isfinite(float(ctx.ema21)):
            reasons.append("15m_context_invalid")
        else:
            context = "BULLISH" if ctx.ema9 > ctx.ema21 else "BEARISH" if ctx.ema9 < ctx.ema21 else "NEUTRAL"
            if (side == "LONG" and context != "BULLISH") or (side == "SHORT" and context != "BEARISH"):
                reasons.append("15m_trend_veto")
    if not reasons:
        reasons.append("strategy_rejected_unclassified")
    entry = values["close"]
    risk = values["atr"] * ATR_STOP_MULT
    if not math.isfinite(risk) or risk <= 0:
        return None
    stop = entry - risk if side == "LONG" else entry + risk
    target = entry + risk * MIN_RR if side == "LONG" else entry - risk * MIN_RR
    timestamp = int(c.timestamp)
    snapshot = {
        "version": 1, **values, "relative_volume": None,
        "ema_alignment_age_bars": alignment_age,
        "ema9_slope_atr_3bars": fast_slope, "ema21_slope_atr_3bars": slow_slope,
        "price_vwap_distance_atr": extension, "context_15m": context,
    }
    return {
        "symbol": "", "direction": side, "candle_open_ms": timestamp,
        "entry": entry, "stop": stop, "target": target, "risk_per_unit": risk,
        "snapshot": snapshot, "rejection_reasons": sorted(set(reasons)),
    }


class SignalJournal:
    def __init__(self, candidate_store):
        self.pool = candidate_store.pool

    async def initialize(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(JOURNAL_DDL)

    async def record_rejected(self, observation: dict[str, Any]) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO journal_candidates(
                     symbol,direction,candle_open_ms,entry,stop,target,risk_per_unit,
                     snapshot,rejection_reasons,last_observed_candle_open_ms)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$3)
                   ON CONFLICT(symbol,direction,candle_open_ms) DO NOTHING RETURNING id""",
                observation["symbol"], observation["direction"], observation["candle_open_ms"],
                observation["entry"], observation["stop"], observation["target"],
                observation["risk_per_unit"],
                json.dumps(observation["snapshot"], separators=(",", ":"), allow_nan=False),
                json.dumps(observation["rejection_reasons"], separators=(",", ":")),
            )
        return row is not None

    async def update_rejected_outcomes(self, symbol: str, candles: list[dict]) -> int:
        normalized = []
        for candle in candles:
            ts = int(candle.get("timestamp", 0))
            high, low = float(candle.get("high", 0)), float(candle.get("low", 0))
            if ts and math.isfinite(high) and math.isfinite(low) and high >= low > 0:
                normalized.append((ts, high, low))
        if not normalized:
            return 0
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id,direction,candle_open_ms,entry,stop,target,risk_per_unit,
                          best_favorable_r,worst_adverse_r,bars_observed,
                          last_observed_candle_open_ms,data_gap
                   FROM journal_candidates WHERE symbol=$1 AND status='OPEN'
                   ORDER BY candle_open_ms""", symbol)
            updated = 0
            for row in rows:
                side = row["direction"]
                entry, stop, target = float(row["entry"]), float(row["stop"]), float(row["target"])
                risk = float(row["risk_per_unit"])
                best, worst = float(row["best_favorable_r"]), float(row["worst_adverse_r"])
                bars, last_ts, gap = int(row["bars_observed"]), int(row["last_observed_candle_open_ms"]), bool(row["data_gap"])
                status, exit_ts, realized = "OPEN", None, None
                for ts, high, low in normalized:
                    if ts <= last_ts or ts <= int(row["candle_open_ms"]):
                        continue
                    if ts - last_ts != 300_000:
                        status, gap, exit_ts = "AMBIGUOUS", True, ts
                        bars += 1
                        last_ts = ts
                        break
                    favorable = max(0.0, (high-entry)/risk) if side == "LONG" else max(0.0, (entry-low)/risk)
                    adverse = max(0.0, (entry-low)/risk) if side == "LONG" else max(0.0, (high-entry)/risk)
                    best, worst = max(best, favorable), max(worst, adverse)
                    stop_hit = low <= stop if side == "LONG" else high >= stop
                    target_hit = high >= target if side == "LONG" else low <= target
                    bars += 1
                    last_ts = ts
                    if stop_hit and target_hit:
                        status, exit_ts = "AMBIGUOUS", ts
                    elif stop_hit:
                        status, exit_ts, realized = "STOP_LOSS", ts, -1.0
                    elif target_hit:
                        status, exit_ts = "TARGET", ts
                        realized = abs(target-entry)/risk
                    if status != "OPEN":
                        break
                if bars == int(row["bars_observed"]) and gap == bool(row["data_gap"]):
                    continue
                await conn.execute(
                    """UPDATE journal_candidates SET status=$2,best_favorable_r=$3,
                         worst_adverse_r=$4,bars_observed=$5,last_observed_candle_open_ms=$6,
                         data_gap=$7,exit_candle_open_ms=$8,realized_r=$9,updated_at=NOW()
                       WHERE id=$1 AND status='OPEN'""",
                    int(row["id"]), status, best, worst, bars, last_ts, gap, exit_ts, realized)
                updated += 1
        return updated

    async def add_feedback(self, signal_id: int, category: str, comment: str = "",
                           source: str = "dashboard") -> int:
        category = str(category).strip().upper()
        if category not in FEEDBACK_CATEGORIES:
            raise ValueError("unsupported feedback category")
        source = source if source in {"dashboard", "telegram", "api"} else "api"
        comment = str(comment or "").strip()[:1000]
        async with self.pool.acquire() as conn:
            signal = await conn.fetchrow(
                "SELECT id FROM signal_history WHERE id=$1 AND status='SENT'", int(signal_id))
            if signal is None:
                raise LookupError("delivered signal not found")
            row = await conn.fetchrow(
                """INSERT INTO journal_feedback(signal_id,category,comment,source)
                   VALUES($1,$2,$3,$4) RETURNING id""",
                int(signal_id), category, comment, source)
        return int(row["id"])

    async def record_manual_opportunity(self, symbol: str, direction: str,
                                       candle_open_ms: int, entry: float,
                                       stop: float, target: float, comment: str = "") -> int:
        symbol = str(symbol).strip().upper()
        direction = str(direction).strip().upper()
        entry, stop, target = float(entry), float(stop), float(target)
        if not symbol or len(symbol) > 40 or direction not in {"LONG", "SHORT"}:
            raise ValueError("symbol and LONG/SHORT direction are required")
        if not all(math.isfinite(v) and v > 0 for v in (entry, stop, target)):
            raise ValueError("entry, stop and target must be finite positive prices")
        if (direction == "LONG" and not stop < entry < target) or (direction == "SHORT" and not target < entry < stop):
            raise ValueError("stop and target must be on the correct side of entry")
        if int(candle_open_ms) <= 0 or int(candle_open_ms) % 300_000:
            raise ValueError("candle_open_ms must be a UTC-aligned 5-minute candle timestamp")
        risk = abs(entry-stop)
        snapshot = {"version": 1, "manual_comment": str(comment or "").strip()[:1000]}
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO journal_candidates(
                     symbol,direction,candle_open_ms,entry,stop,target,risk_per_unit,
                     snapshot,rejection_reasons,last_observed_candle_open_ms)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$3)
                   ON CONFLICT(symbol,direction,candle_open_ms) DO UPDATE
                     SET snapshot=journal_candidates.snapshot || EXCLUDED.snapshot
                   RETURNING id""",
                symbol, direction, int(candle_open_ms), entry, stop, target, risk,
                json.dumps(snapshot, separators=(",", ":")),
                json.dumps(["manual_missed_opportunity"]),
            )
        return int(row["id"])

    async def add_candidate_feedback(self, candidate_id: int, category: str,
                                     comment: str = "", source: str = "dashboard") -> int:
        category = str(category).strip().upper()
        if category not in {"MISSED_OPPORTUNITY", "NEEDS_REVIEW"}:
            raise ValueError("candidate feedback must be MISSED_OPPORTUNITY or NEEDS_REVIEW")
        source = source if source in {"dashboard", "telegram", "api"} else "api"
        comment = str(comment or "").strip()[:1000]
        async with self.pool.acquire() as conn:
            candidate = await conn.fetchrow("SELECT id FROM journal_candidates WHERE id=$1", int(candidate_id))
            if candidate is None:
                raise LookupError("rejected candidate not found")
            row = await conn.fetchrow(
                """INSERT INTO journal_candidate_feedback(candidate_id,category,comment,source)
                   VALUES($1,$2,$3,$4) RETURNING id""",
                int(candidate_id), category, comment, source)
        return int(row["id"])

    async def add_telegram_feedback(self, signal_id: int, category: str) -> int:
        return await self.add_feedback(signal_id, category, "", "telegram")

    async def entries(self, limit: int = 100) -> list[dict]:
        limit = max(1, min(int(limit), 250))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT s.id,s.symbol,s.direction,s.created_at,s.delivered_at,
                          s.payload::text AS payload,o.status AS outcome_status,
                          o.realized_r,o.best_favorable_r,o.worst_adverse_r,
                          (SELECT json_agg(json_build_object(
                              'id',f.id,'category',f.category,'comment',f.comment,
                              'source',f.source,'created_at',f.created_at)
                              ORDER BY f.created_at DESC)
                           FROM journal_feedback f WHERE f.signal_id=s.id) AS feedback
                   FROM signal_history s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT'
                   ORDER BY s.delivered_at DESC,s.id DESC LIMIT $1""", limit)
        result = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload"))
            item["created_at"] = item["created_at"].isoformat() if item["created_at"] else None
            item["delivered_at"] = item["delivered_at"].isoformat() if item["delivered_at"] else None
            item["feedback"] = json.loads(item["feedback"]) if item["feedback"] else []
            for feedback in item["feedback"]:
                if feedback.get("created_at"):
                    feedback["created_at"] = feedback["created_at"].isoformat()
            result.append(item)
        return result

    async def candidates(self, limit: int = 100) -> list[dict]:
        limit = max(1, min(int(limit), 250))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id,symbol,direction,candle_open_ms,entry,stop,target,
                          snapshot::text AS snapshot,rejection_reasons::text AS rejection_reasons,
                          status,best_favorable_r,worst_adverse_r,bars_observed,
                          data_gap,realized_r,created_at,
                          (SELECT json_agg(json_build_object('id',f.id,'category',f.category,
                              'comment',f.comment,'source',f.source,'created_at',f.created_at)
                              ORDER BY f.created_at DESC)::text
                           FROM journal_candidate_feedback f WHERE f.candidate_id=journal_candidates.id) AS feedback
                   FROM journal_candidates ORDER BY created_at DESC,id DESC LIMIT $1""", limit)
        result = []
        for row in rows:
            item = dict(row)
            item["snapshot"] = json.loads(item["snapshot"])
            item["rejection_reasons"] = json.loads(item["rejection_reasons"])
            item["feedback"] = json.loads(item["feedback"]) if item["feedback"] else []
            item["created_at"] = item["created_at"].isoformat()
            result.append(item)
        return result

    async def learning_summary(self) -> dict:
        async with self.pool.acquire() as conn:
            totals = await conn.fetchrow(
                """SELECT COUNT(*) AS delivered_signals,
                          COUNT(*) FILTER (WHERE o.status='TARGET') AS targets,
                          COUNT(*) FILTER (WHERE o.status='STOP_LOSS') AS stop_losses,
                          COUNT(*) FILTER (WHERE o.status='OPEN') AS open,
                          COUNT(*) FILTER (WHERE o.status='AMBIGUOUS') AS ambiguous,
                          ROUND(AVG(o.realized_r)::numeric,3) AS avg_realized_r
                   FROM signal_history s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT'""")
            feedback = await conn.fetch(
                "SELECT category,COUNT(*) AS count FROM journal_feedback GROUP BY category ORDER BY count DESC,category")
            candidate_feedback = await conn.fetch(
                "SELECT category,COUNT(*) AS count FROM journal_candidate_feedback GROUP BY category ORDER BY count DESC,category")
            rejected = await conn.fetch(
                """SELECT status,COUNT(*) AS count,ROUND(AVG(best_favorable_r)::numeric,3) AS avg_mfe_r,
                          ROUND(AVG(worst_adverse_r)::numeric,3) AS avg_mae_r
                   FROM journal_candidates GROUP BY status ORDER BY status""")
            reasons = await conn.fetch(
                """SELECT reason,COUNT(*) AS count FROM journal_candidates c
                   CROSS JOIN LATERAL jsonb_array_elements_text(c.rejection_reasons) AS r(reason)
                   GROUP BY reason ORDER BY count DESC LIMIT 20""")
        sample = int(totals["targets"] or 0) + int(totals["stop_losses"] or 0)
        recommendations = []
        if sample < 30:
            recommendations.append("Collect at least 30 resolved delivered signals before judging live rule quality.")
        else:
            recommendations.append("Use chronological walk-forward replay before accepting any proposed threshold change.")
        if not feedback:
            recommendations.append("Add manual feedback to delivered signals to distinguish execution quality from candle outcome.")
        if rejected:
            recommendations.append("Compare rejected-candidate outcomes by rejection reason; do not weaken a filter from raw counts alone.")
        return {
            "delivered_signal_totals": dict(totals),
            "manual_feedback": [dict(x) for x in feedback],
            "candidate_feedback": [dict(x) for x in candidate_feedback],
            "rejected_candidate_outcomes": [dict(x) for x in rejected],
            "top_rejection_reasons": [dict(x) for x in reasons],
            "resolved_signal_sample": sample,
            "recommendations": recommendations,
            "automatic_strategy_changes": False,
            "warning": "Research and decision support only. No live strategy thresholds are modified.",
        }


DASHBOARD_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CryptoByance Signal Journal</title>
<style>
body{font:15px system-ui,sans-serif;margin:0;background:#0b1220;color:#e5e7eb}main{max-width:1100px;margin:auto;padding:22px}
h1,h2{margin:8px 0 14px}input,button,textarea{font:inherit;padding:9px;border-radius:8px;border:1px solid #334155;background:#111c30;color:#e5e7eb}
button{cursor:pointer;background:#1d4ed8;border:0;margin:3px}.panel,.entry{background:#111c30;border:1px solid #26364f;border-radius:12px;padding:14px;margin:12px 0}
small,.muted{color:#9ca3af}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px}
.entry{display:grid;grid-template-columns:1fr auto;gap:10px}.entry pre{white-space:pre-wrap;overflow-wrap:anywhere;color:#cbd5e1}
textarea{width:95%;min-height:42px}
</style></head><body><main>
<h1>CryptoByance — Signal Journal</h1>
<p class="muted">Delivered signals, outcomes, feedback and rejected setups. Research only; no orders or automatic strategy changes.</p>
<section class="panel"><label>Journal API token <input id="token" type="password" placeholder="Set JOURNAL_API_TOKEN"></label>
<button onclick="loadAll()">Load journal</button><span id="status" class="muted"></span></section>
<div class="grid"><section class="panel"><h2>Learning summary</h2><pre id="learning">Load journal to view.</pre></section>
<section class="panel"><h2>Rejected candidates</h2><pre id="candidates">Load journal to view.</pre></section></div>
<h2>Delivered signals</h2><div id="entries"></div>
<script>
const headers=()=>({'X-Journal-Token':document.getElementById('token').value,'Content-Type':'application/json'});
async function api(path,opts={}){const r=await fetch(path,{...opts,headers:{...headers(),...(opts.headers||{})}});if(!r.ok)throw new Error((await r.text()).slice(0,250)||r.status);return r.json()}
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
async function loadAll(){const status=document.getElementById('status');status.textContent='Loading…';try{
 const results=await Promise.all([api('/journal/api/entries'),api('/journal/api/candidates'),api('/learning')]);
 const e=results[0],c=results[1],l=results[2];
 document.getElementById('learning').textContent=JSON.stringify(l,null,2);
 document.getElementById('candidates').innerHTML=c.slice(0,30).map(function(x){return '<article class="entry"><div><b>#'+x.id+' '+esc(x.symbol)+' '+esc(x.direction)+'</b><div class="muted">'+esc(x.status)+' · MFE='+esc(x.best_favorable_r)+'R · MAE='+esc(x.worst_adverse_r)+'R · '+esc((x.rejection_reasons||[]).join(', '))+'</div><pre>'+esc(JSON.stringify(x.snapshot,null,2))+'</pre>'+((x.feedback||[]).map(function(f){return '<small>'+esc(f.category)+' — '+esc(f.comment)+'</small><br>'}).join(''))+'</div><div><button onclick="candidateFeedback('+x.id+',\'MISSED_OPPORTUNITY\')">Missed opportunity</button><button onclick="candidateFeedback('+x.id+',\'NEEDS_REVIEW\')">Review</button></div></article>'}).join('');
 document.getElementById('entries').innerHTML=e.map(function(x){const p=x.payload||{};const buttons=[['GOOD_ENTRY','Good entry'],['BAD_SIGNAL','Bad signal'],['LATE_ENTRY','Late'],['WRONG_DIRECTION','Wrong direction'],['STOP_LOSS_TOO_TIGHT','Stop too tight'],['TARGET_TOO_CLOSE','Target too close'],['NEEDS_REVIEW','Review']].map(function(pair){return '<button onclick="feedback('+x.id+',\''+pair[0]+'\')">'+pair[1]+'</button>'}).join('');
 const old=(x.feedback||[]).map(function(f){return '<small>'+esc(f.category)+' — '+esc(f.comment)+'</small><br>'}).join('');
 return '<article class="entry"><div><b>#'+x.id+' '+esc(x.symbol)+' '+esc(x.direction)+'</b><div class="muted">'+esc(x.delivered_at)+' · '+esc(x.outcome_status||'NO OUTCOME')+' · R='+esc(x.realized_r??'—')+' · MFE='+esc(x.best_favorable_r??'—')+' · MAE='+esc(x.worst_adverse_r??'—')+'</div><pre>'+esc(JSON.stringify({entry:p.entry,stop:p.stop,target:p.target,diagnostics:p.diagnostics},null,2))+'</pre><div>'+old+'</div><textarea id="comment-'+x.id+'" placeholder="Optional feedback comment"></textarea></div><div>'+buttons+'</div></article>'}).join('');
 status.textContent='Loaded '+e.length+' signals and '+c.length+' rejected candidates.';
 }catch(err){status.textContent='Error: '+err.message}}
async function feedback(id,category){try{await api('/journal/api/feedback',{method:'POST',body:JSON.stringify({signal_id:id,category:category,comment:document.getElementById('comment-'+id).value})});await loadAll()}catch(err){alert(err.message)}}
async function candidateFeedback(id,category){const comment=prompt('Optional note for this rejected setup:')||'';try{await api('/journal/api/candidate-feedback',{method:'POST',body:JSON.stringify({candidate_id:id,category:category,comment:comment})});await loadAll()}catch(err){alert(err.message)}}
</script></main></body></html>"""
