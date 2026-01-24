from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Dict, List


@dataclass(frozen=True)
class AuditRecord:
    ts: str
    correlation_id: str
    actor: str
    event_type: str
    payload: Dict[str, Any]


def _tail_lines(path: str, *, max_lines: int, max_bytes: int = 2_000_000) -> list[str]:
    """
    Read up to `max_lines` from the end of a text file without loading the entire file into memory.
    `max_bytes` caps worst-case reads for extremely large files.
    """
    if not os.path.exists(path) or max_lines <= 0:
        return []
    # Binary tail read, then decode once.
    data = b""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            # Read backwards in chunks until we have enough newlines or hit max_bytes.
            chunk = 64 * 1024
            pos = end
            while pos > 0 and len(data) < max_bytes and data.count(b"\n") <= (max_lines + 2):
                step = chunk if pos >= chunk else pos
                pos -= step
                f.seek(pos, os.SEEK_SET)
                data = f.read(step) + data
    except Exception:
        return []
    try:
        text = data.decode("utf-8", errors="replace")
    except Exception:
        text = str(data)
    lines = text.splitlines()
    return lines[-max_lines:]


def tail_jsonl(path: str, *, max_lines: int = 200) -> List[AuditRecord]:
    if not os.path.exists(path):
        return []
    lines = _tail_lines(path, max_lines=max_lines)
    out: List[AuditRecord] = []
    for ln in lines:
        try:
            obj = json.loads(ln)
            out.append(
                AuditRecord(
                    ts=obj.get("ts", ""),
                    correlation_id=obj.get("correlation_id", ""),
                    actor=obj.get("actor", ""),
                    event_type=obj.get("event_type", ""),
                    payload=obj.get("payload", {}) or {},
                )
            )
        except json.JSONDecodeError:
            continue
    return out


async def stream_jsonl_sse(path: str, *, poll_interval_s: float = 0.5) -> AsyncGenerator[str, None]:
    """
    Simple file-tail SSE stream. Suitable for local MVP.
    """
    # Emit an initial keepalive so browsers reliably transition from "connecting..." to "open"
    # even when the audit log is empty and no events have been written yet.
    yield ": hello\n\n"
    last_size = 0
    last_ping = time.monotonic()
    while True:
        if os.path.exists(path):
            try:
                size = os.path.getsize(path)
                if size < last_size:
                    last_size = 0  # rotated/truncated
                if last_size == 0 and size > 0:
                    # IMPORTANT: on a fresh SSE connection, do NOT replay the whole file.
                    # That can be tens/hundreds of thousands of events and will freeze browsers.
                    for ln in _tail_lines(path, max_lines=800):
                        yield f"data: {ln}\n\n"
                    last_size = size
                elif size > last_size:
                    with open(path, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(last_size)
                        chunk = f.read()
                    last_size = size
                    for ln in chunk.splitlines():
                        yield f"data: {ln}\n\n"
            except Exception:
                # Don't kill the stream on transient read errors
                pass

        # Keepalive comment every ~2s to avoid "stuck connecting" UX when no new events arrive.
        now = time.monotonic()
        if now - last_ping >= 2.0:
            last_ping = now
            yield ": ping\n\n"

        await asyncio.sleep(poll_interval_s)


def render_ui_html() -> str:
    # No templating deps in Phase-1; just serve a single-page HTML with JS.
    return """<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>PATCHIT Event Console</title>
    <style>
      :root {
        --bg: #0b1020;
        --panel: rgba(255, 255, 255, 0.06);
        --panel2: rgba(255, 255, 255, 0.08);
        --text: #e5e7eb;
        --muted: rgba(229, 231, 235, 0.65);
        --border: rgba(255, 255, 255, 0.10);
        --blue: #60a5fa;
        --green: #34d399;
        --amber: #fbbf24;
        --red: #f87171;
        --purple: #c4b5fd;
      }
      * { box-sizing: border-box; }
      body {
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto;
        margin: 0;
        background: radial-gradient(1200px 600px at 20% 0%, rgba(96,165,250,0.20), transparent 60%),
                    radial-gradient(900px 500px at 80% 10%, rgba(196,181,253,0.16), transparent 60%),
                    radial-gradient(900px 500px at 70% 90%, rgba(52,211,153,0.10), transparent 60%),
                    var(--bg);
        color: var(--text);
      }
      /* Subtle animated aurora */
      body:before {
        content:"";
        position: fixed;
        inset: -20%;
        background:
          radial-gradient(900px 520px at 18% 10%, rgba(96,165,250,0.20), transparent 65%),
          radial-gradient(900px 520px at 82% 15%, rgba(196,181,253,0.18), transparent 65%),
          radial-gradient(900px 520px at 70% 88%, rgba(52,211,153,0.12), transparent 65%);
        filter: blur(22px);
        opacity: 0.9;
        animation: drift 14s ease-in-out infinite alternate;
        pointer-events: none;
        z-index: -2;
      }
      @keyframes drift {
        0% { transform: translate3d(-2%, -1%, 0) scale(1.02); }
        50% { transform: translate3d(1%, 1%, 0) scale(1.04); }
        100% { transform: translate3d(2%, -1%, 0) scale(1.03); }
      }
      #fxCanvas {
        position: fixed;
        inset: 0;
        width: 100vw;
        height: 100vh;
        pointer-events: none;
        z-index: -1;
        opacity: 0.85;
      }
      header, .layout { position: relative; z-index: 1; }
      header {
        position: sticky; top: 0; z-index: 20;
        backdrop-filter: blur(10px);
        background: linear-gradient(to bottom, rgba(11,16,32,0.90), rgba(11,16,32,0.60));
        border-bottom: 1px solid var(--border);
        padding: 14px 16px;
      }
      header:before {
        content: "";
        position: absolute;
        inset: 0;
        pointer-events: none;
        background: radial-gradient(900px 420px at 30% 0%, rgba(96,165,250,0.12), transparent 60%),
                    radial-gradient(900px 420px at 70% 0%, rgba(196,181,253,0.10), transparent 60%);
        opacity: 0.9;
      }
      .brand { display: flex; align-items: center; gap: 10px; }
      .logo {
        width: 28px; height: 28px; border-radius: 10px;
        background: linear-gradient(135deg, rgba(96,165,250,0.95), rgba(196,181,253,0.95));
        box-shadow: 0 8px 30px rgba(96,165,250,0.18);
      }
      .logo { position: relative; overflow: hidden; }
      .logo:after{
        content:"";
        position:absolute; inset:-40%;
        background: linear-gradient(120deg, transparent 30%, rgba(255,255,255,0.22), transparent 60%);
        transform: rotate(20deg);
        animation: shine 3.6s ease-in-out infinite;
      }
      @keyframes shine { 0% { transform: translateX(-35%) rotate(20deg); opacity: 0.0; } 15% { opacity: 1.0; } 45% { opacity: 0.0; } 100% { transform: translateX(35%) rotate(20deg); opacity: 0.0; } }
      /* Cute mascot: makes UI update unmistakable */
      .mascot {
        width: 34px; height: 34px;
        border-radius: 12px;
        border: 1px solid rgba(255,255,255,0.16);
        background: linear-gradient(135deg, rgba(96,165,250,0.25), rgba(196,181,253,0.18));
        display: grid;
        place-items: center;
        position: relative;
        box-shadow: 0 14px 50px rgba(96,165,250,0.12);
        animation: bob 1.8s ease-in-out infinite;
      }
      @keyframes bob { 0%,100% { transform: translateY(0); } 50% { transform: translateY(-2px); } }
      .mascot .face {
        width: 18px; height: 14px;
        border-radius: 8px;
        background: rgba(0,0,0,0.28);
        border: 1px solid rgba(255,255,255,0.10);
        display: flex;
        align-items: center;
        justify-content: space-evenly;
        gap: 6px;
        position: relative;
        overflow: hidden;
      }
      .mascot .eye {
        width: 4px; height: 6px;
        border-radius: 999px;
        background: rgba(52,211,153,0.95);
        box-shadow: 0 0 0 2px rgba(52,211,153,0.10), 0 0 18px rgba(52,211,153,0.18);
        animation: blink 5.5s ease-in-out infinite;
        transform-origin: center;
      }
      @keyframes blink { 0%, 2%, 60%, 100% { transform: scaleY(1); } 1% { transform: scaleY(0.12); } }
      .mascot .antenna {
        position: absolute;
        top: -6px;
        width: 2px; height: 10px;
        background: rgba(255,255,255,0.22);
        border-radius: 999px;
      }
      .mascot .antenna:after{
        content:"";
        position: absolute;
        top: -6px; left: 50%;
        width: 6px; height: 6px;
        transform: translateX(-50%);
        border-radius: 999px;
        background: rgba(96,165,250,0.95);
        box-shadow: 0 0 18px rgba(96,165,250,0.25);
        animation: ping 1.8s ease-out infinite;
      }
      @keyframes ping { 0% { transform: translateX(-50%) scale(1); opacity: 1; } 70% { transform: translateX(-50%) scale(1.8); opacity: 0; } 100% { opacity: 0; } }
      .title { font-weight: 800; letter-spacing: 0.2px; }
      .sub { color: var(--muted); font-size: 12px; }
      .row { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin-top: 10px; }
      .row .spacer { flex: 1; }
      input {
        padding: 10px 10px;
        border-radius: 10px;
        border: 1px solid var(--border);
        background: rgba(255,255,255,0.04);
        color: var(--text);
        min-width: 240px;
        outline: none;
      }
      input:focus { border-color: rgba(96,165,250,0.6); box-shadow: 0 0 0 3px rgba(96,165,250,0.12); }
      button {
        padding: 10px 12px;
        cursor: pointer;
        border-radius: 10px;
        border: 1px solid var(--border);
        color: var(--text);
        background: rgba(255,255,255,0.06);
      }
      button:hover { background: rgba(255,255,255,0.10); }
      .toggle {
        display: inline-flex; align-items: center; gap: 8px;
        padding: 8px 10px;
        border-radius: 10px;
        border: 1px solid var(--border);
        background: rgba(255,255,255,0.04);
        cursor: pointer;
        user-select: none;
        color: var(--muted);
      }
      .toggle input { min-width: auto; }
      .toggle.on { color: var(--text); border-color: rgba(96,165,250,0.35); box-shadow: 0 0 0 3px rgba(96,165,250,0.08); }
      .inhouse-panel{
        position: fixed;
        right: 18px;
        top: 86px;
        background: rgba(8,12,22,0.96);
        border: 1px solid rgba(148,163,184,0.25);
        border-radius: 14px;
        padding: 14px;
        width: 360px;
        box-shadow: 0 20px 60px rgba(0,0,0,0.45);
        z-index: 9999;
      }
      .inhouse-panel.hidden{ display:none; }
      .inhouse-panel h4{ margin:0 0 8px 0; font-size:14px; color: #e2e8f0; }
      .field{ display:flex; flex-direction:column; gap:6px; margin:8px 0; }
      .field label{ font-size:12px; color: rgba(148,163,184,0.9); }
      .field input, .field textarea{
        background: rgba(255,255,255,0.06);
        border: 1px solid rgba(148,163,184,0.2);
        color: #e2e8f0;
        border-radius: 10px;
        padding: 8px 10px;
      }
      .field textarea{ min-height: 88px; }
      .connector-panel{
        position: fixed;
        left: 18px;
        top: 86px;
        background: rgba(8,12,22,0.96);
        border: 1px solid rgba(148,163,184,0.25);
        border-radius: 14px;
        padding: 14px;
        width: 380px;
        box-shadow: 0 20px 60px rgba(0,0,0,0.45);
        z-index: 9999;
      }
      .connector-panel.hidden{ display:none; }
      .pill { font-size: 12px; padding: 4px 10px; border-radius: 999px; border: 1px solid var(--border); color: var(--muted); }
      .pill.live { color: var(--green); border-color: rgba(52,211,153,0.35); }
      .pill.down { color: var(--red); border-color: rgba(248,113,113,0.35); }
      .pill.glow { box-shadow: 0 0 0 3px rgba(96,165,250,0.10), 0 18px 60px rgba(96,165,250,0.12); }
      .layout { display: grid; grid-template-columns: 1.4fr 0.9fr; gap: 14px; padding: 14px 16px; }
      @media (max-width: 1100px) { .layout { grid-template-columns: 1fr; } }
      .panel { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; overflow: hidden; }
      .panel-h { padding: 12px 12px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid var(--border); }
      .panel-h h3 { margin: 0; font-size: 14px; letter-spacing: 0.2px; }
      .panel-h .hint { font-size: 12px; color: var(--muted); }
      .table { width: 100%; }
      .thead { padding: 10px 12px; color: var(--muted); font-size: 12px; border-bottom: 1px solid var(--border); display:flex; justify-content: space-between; gap:10px; }
      .thead .cols { display:flex; gap:10px; align-items:center; }
      .thead .cols span { opacity: 0.9; }
      .feed { padding: 10px 12px; display: grid; gap: 10px; }
      .card {
        position: relative;
        padding: 12px 12px;
        border-radius: 14px;
        border: 1px solid rgba(255,255,255,0.10);
        background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
        cursor: pointer;
        transform: translateY(0);
        transition: transform 160ms ease, border-color 160ms ease, background 160ms ease;
      }
      .card:hover { transform: translateY(-2px); border-color: rgba(96,165,250,0.26); background: linear-gradient(180deg, rgba(255,255,255,0.07), rgba(255,255,255,0.035)); }
      .card.new { animation: slidein 420ms ease-out; }
      @keyframes slidein { from { transform: translateY(6px); opacity: 0.3; } to { transform: translateY(0); opacity: 1; } }
      .card.sel { border-color: rgba(52,211,153,0.32); box-shadow: 0 0 0 3px rgba(52,211,153,0.08); }
      .card:after{
        content:"";
        position:absolute;
        inset: -1px;
        border-radius: 14px;
        background: linear-gradient(135deg, rgba(96,165,250,0.22), rgba(196,181,253,0.18), rgba(52,211,153,0.10));
        opacity: 0;
        transition: opacity 180ms ease;
        z-index: -1;
      }
      .card:hover:after{ opacity: 1; }
      .cardTop { display:flex; align-items:center; justify-content: space-between; gap: 10px; }
      .meta { display:flex; align-items:center; gap: 10px; min-width: 0; }
      .badges { display:flex; gap: 8px; align-items:center; flex-wrap: wrap; justify-content:flex-end; }
      .badge {
        font-size: 11px;
        padding: 3px 8px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.10);
        color: var(--muted);
        background: rgba(0,0,0,0.18);
      }
      .badge.blue { border-color: rgba(96,165,250,0.30); color: rgba(96,165,250,0.95); background: rgba(96,165,250,0.08); }
      .badge.green { border-color: rgba(52,211,153,0.30); color: rgba(52,211,153,0.95); background: rgba(52,211,153,0.08); }
      .badge.red { border-color: rgba(248,113,113,0.30); color: rgba(248,113,113,0.95); background: rgba(248,113,113,0.08); }
      .badge.amber { border-color: rgba(251,191,36,0.30); color: rgba(251,191,36,0.95); background: rgba(251,191,36,0.08); }
      .cardMid { margin-top: 8px; display:flex; gap: 10px; align-items:center; }
      .cardMid .mono { color: rgba(229,231,235,0.95); }
      .small { font-size: 12px; color: var(--muted); }
      .cardMsg { margin-top: 8px; color: var(--muted); font-size: 12px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
      .split { display:flex; gap: 10px; align-items:center; min-width: 0; }
      .split .grow { flex: 1; min-width: 0; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas; }
      .etype { display: inline-flex; align-items: center; gap: 8px; }
      .dot { width: 9px; height: 9px; border-radius: 999px; background: var(--muted); }
      .dot.info { background: var(--blue); }
      .dot.ok { background: var(--green); }
      .dot.warn { background: var(--amber); }
      .dot.err { background: var(--red); }
      .dot.pulse { box-shadow: 0 0 0 0 rgba(96,165,250,0.45); animation: pulse 1.35s ease-out infinite; }
      @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(96,165,250,0.35);} 70% { box-shadow: 0 0 0 8px rgba(96,165,250,0.0);} 100% { box-shadow: 0 0 0 0 rgba(96,165,250,0.0);} }
      .payload { color: var(--muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
      a { color: var(--blue); text-decoration: none; }
      a:hover { text-decoration: underline; }
      pre {
        margin: 0;
        padding: 12px;
        background: rgba(0,0,0,0.35);
        border-top: 1px solid rgba(255,255,255,0.06);
        color: var(--text);
        overflow: auto;
        max-height: 70vh;
      }
      .kv { display: grid; grid-template-columns: 120px 1fr; gap: 8px; padding: 12px; }
      .k { color: var(--muted); font-size: 12px; }
      .v { font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .flowbar {
        height: 10px;
        border-radius: 999px;
        background: rgba(255,255,255,0.06);
        border: 1px solid rgba(255,255,255,0.10);
        overflow: hidden;
      }
      .flowbar > div{
        height: 100%;
        width: 0%;
        background: linear-gradient(90deg, rgba(96,165,250,0.9), rgba(52,211,153,0.85));
        transition: width 220ms ease;
      }
      /* Animated “breathing” effect for live status */
      .pill.live { animation: breathe 1.6s ease-in-out infinite; }
      @keyframes breathe { 0%,100% { box-shadow: 0 0 0 0 rgba(52,211,153,0.0); } 50% { box-shadow: 0 0 0 6px rgba(52,211,153,0.10); } }

      /* ---------- Bottom “Agent Stage” (retro plumber-style) ---------- */
      body { padding-bottom: 120px; } /* reserve space for dock */
      .agentDock {
        position: fixed;
        left: 14px; right: 14px; bottom: 14px;
        z-index: 999;
        border-radius: 18px;
        border: 1px solid rgba(255,255,255,0.14);
        background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
        backdrop-filter: blur(10px);
        box-shadow: 0 30px 90px rgba(0,0,0,0.35);
        overflow: hidden;
      }
      .agentDock:before{
        content:"";
        position:absolute; inset:-2px;
        border-radius: 18px;
        background: linear-gradient(135deg, rgba(96,165,250,0.22), rgba(196,181,253,0.16), rgba(52,211,153,0.10));
        opacity: 0.55;
        pointer-events:none;
      }
      .agentDockInner{
        position: relative;
        display: grid;
        grid-template-columns: 1fr;
        gap: 10px;
        padding: 12px 12px 12px 12px;
      }
      .agentDockTop{
        display:flex;
        align-items:center;
        justify-content: space-between;
        gap: 10px;
        padding: 2px 4px;
      }
      .agentDockTitle{
        display:flex;
        align-items: baseline;
        gap: 10px;
        min-width: 0;
      }
      .agentLabel{
        font-weight: 800;
        letter-spacing: 0.2px;
      }
      .agentSub{
        color: var(--muted);
        font-size: 12px;
        overflow:hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
      .agentStatePill{
        display:inline-flex;
        align-items:center;
        gap: 8px;
        padding: 6px 10px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.12);
        background: rgba(0,0,0,0.18);
        color: var(--text);
        font-size: 14px;
        font-weight: 700;
      }
      .agentStatePill .miniDot{
        width: 8px; height: 8px; border-radius: 999px;
        background: var(--blue);
        box-shadow: 0 0 0 3px rgba(96,165,250,0.10);
      }
      .agentStatePill.ok .miniDot{ background: var(--green); box-shadow: 0 0 0 3px rgba(52,211,153,0.10); }
      .agentStatePill.warn .miniDot{ background: var(--amber); box-shadow: 0 0 0 3px rgba(251,191,36,0.10); }
      .agentStatePill.err .miniDot{ background: var(--red); box-shadow: 0 0 0 3px rgba(248,113,113,0.10); }

      .stage{
        position: relative;
        height: 108px; /* was too small; sprite was getting clipped (looked “invisible”) */
        border-radius: 14px;
        border: 1px solid rgba(255,255,255,0.10);
        background:
          linear-gradient(180deg, rgba(0,0,0,0.22), rgba(0,0,0,0.10)),
          radial-gradient(520px 120px at 20% 20%, rgba(96,165,250,0.10), transparent 60%),
          radial-gradient(520px 120px at 80% 20%, rgba(196,181,253,0.10), transparent 60%);
        overflow: hidden;
      }
      .track{
        position:absolute; left: 10px; right: 10px; bottom: 10px;
        height: 12px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.12);
        background: rgba(255,255,255,0.06);
        overflow: hidden;
      }
      .trackFill{
        height: 100%;
        width: 0%;
        background: linear-gradient(90deg, rgba(96,165,250,0.95), rgba(52,211,153,0.90));
        transition: width 260ms ease;
      }
      .blocks{
        position:absolute;
        left: 10px; right: 10px; top: 10px;
        display:flex;
        gap: 6px;
        opacity: 0.95;
      }
      .block{
        flex: 1;
        height: 10px;
        border-radius: 10px;
        border: 1px solid rgba(255,255,255,0.10);
        background: rgba(255,255,255,0.05);
        transition: background 220ms ease, border-color 220ms ease, transform 220ms ease;
      }
      .block.on{
        background: rgba(52,211,153,0.14);
        border-color: rgba(52,211,153,0.26);
        transform: translateY(-1px);
      }
      .block.err{
        background: rgba(248,113,113,0.14);
        border-color: rgba(248,113,113,0.26);
      }

      /* Retro plumber sprite (original CSS art — “Mario-like”, not Mario assets) */
      .spriteWrap{
        --p: 0; /* 0..1 progress */
        --spr: 54px;
        position:absolute;
        left: 10px; right: 10px; bottom: 20px;
        height: var(--spr);
        pointer-events: none;
      }
      .sprite{
        position:absolute;
        left: calc(var(--p) * (100% - var(--spr)));
        width: var(--spr); height: var(--spr);
        image-rendering: pixelated;
        transform: translateY(0);
        transition: left 260ms ease;
        filter: drop-shadow(0 10px 18px rgba(0,0,0,0.35));
        animation: sprBob 0.9s ease-in-out infinite;
      }
      @keyframes sprBob { 0%,100% { transform: translateY(0); } 50% { transform: translateY(-2px); } }
      .sprite svg { width: var(--spr); height: var(--spr); display:block; }
      .sprite.walking { animation: sprBob 0.55s ease-in-out infinite, sprStep 0.75s linear infinite; }
      @keyframes sprStep { 0% { filter: drop-shadow(0 10px 18px rgba(0,0,0,0.35)); } 50% { filter: drop-shadow(0 12px 20px rgba(0,0,0,0.40)); } 100% { filter: drop-shadow(0 10px 18px rgba(0,0,0,0.35)); } }
      .sprite .tool { opacity: 0; transition: opacity 160ms ease; }
      .sprite.read .tool.book { opacity: 1; }
      .sprite.think .tool.lens { opacity: 1; }
      .sprite.patch .tool.hammer { opacity: 1; }
      .sprite.test .tool.flask { opacity: 1; }
      .sprite.ship .tool.flag { opacity: 1; }
      .sprite.report .tool.scroll { opacity: 1; }
      .speech{
        position:absolute;
        left: calc(var(--p) * (100% - var(--spr)) + 62px);
        bottom: 66px;
        max-width: min(520px, 70vw);
        padding: 8px 10px;
        border-radius: 12px;
        border: 1px solid rgba(255,255,255,0.12);
        background: rgba(0,0,0,0.22);
        color: rgba(229,231,235,0.92);
        font-size: 12px;
        overflow:hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        box-shadow: 0 18px 60px rgba(0,0,0,0.25);
        transform-origin: left bottom;
        animation: pop 220ms ease;
      }
      @keyframes pop { from { transform: scale(0.98); opacity: 0.6; } to { transform: scale(1); opacity: 1; } }
      .speech:before{
        content:"";
        position:absolute;
        left: -6px; bottom: 10px;
        width: 10px; height: 10px;
        background: rgba(0,0,0,0.22);
        border-left: 1px solid rgba(255,255,255,0.12);
        border-bottom: 1px solid rgba(255,255,255,0.12);
        transform: rotate(45deg);
      }
      /* --- Game Mode (3D-ish/isometric) --- */
      .hidden { display:none !important; }
      .gameWrap{
        position: fixed;
        inset: 0;
        z-index: 30;
        background: radial-gradient(1200px 600px at 30% 10%, rgba(96,165,250,0.20), transparent 60%),
                    radial-gradient(900px 500px at 80% 20%, rgba(196,181,253,0.16), transparent 60%),
                    radial-gradient(900px 500px at 60% 90%, rgba(52,211,153,0.10), transparent 60%),
                    rgba(9,12,24,0.98);
        backdrop-filter: blur(6px);
      }
      #gameCanvas{
        position:absolute;
        inset: 0;
        width: 100%;
        height: 100%;
      }
      .gameHud{
        position: absolute;
        left: 14px;
        top: 14px;
        right: 14px;
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 12px;
        pointer-events: none;
      }
      .gameCard{
        pointer-events: auto;
        border: 1px solid rgba(255,255,255,0.12);
        background: rgba(0,0,0,0.28);
        border-radius: 14px;
        padding: 10px 12px;
        box-shadow: 0 20px 70px rgba(0,0,0,0.35);
      }
      .gameTitle{ font-weight: 800; letter-spacing: 0.2px; }
      .gameSub{ color: rgba(229,231,235,0.70); font-size: 12px; }
      .gameLegend{ display:flex; gap:8px; flex-wrap:wrap; margin-top:8px; }
      .gchip{ font-size:12px; padding:4px 8px; border-radius: 999px; border:1px solid rgba(255,255,255,0.12); color: rgba(229,231,235,0.78); background: rgba(255,255,255,0.04); }
      .gbtn{ pointer-events:auto; padding:8px 10px; border-radius: 12px; border:1px solid rgba(255,255,255,0.14); background: rgba(255,255,255,0.06); color: rgba(229,231,235,0.92); cursor:pointer; }
      .gbtn:hover{ background: rgba(255,255,255,0.10); }
      @media (max-width: 900px) {
        body { padding-bottom: 150px; }
        .speech { display:none; }
      }
    </style>
  </head>
  <body>
    <canvas id="fxCanvas"></canvas>
    <div id="gameWrap" class="gameWrap hidden">
      <canvas id="gameCanvas"></canvas>
      <div class="gameHud">
        <div class="gameCard" style="min-width: 320px;">
          <div class="gameTitle">PATCHIT Agent Arcade</div>
          <div class="gameSub mono" id="gameCorr">correlation: —</div>
          <div class="gameSub" id="gameStageTxt">waiting for events…</div>
          <div class="gameSub" style="margin-top:8px; display:flex; align-items:center; gap:10px;">
            <span>Zoom</span>
            <input id="gameZoom" type="range" min="0.8" max="2.0" step="0.05" value="1.25" style="width: 180px;" />
          </div>
          <div class="gameLegend">
            <span class="gchip">Runner</span>
            <span class="gchip">Safety</span>
            <span class="gchip">RCA</span>
            <span class="gchip">Fixer</span>
            <span class="gchip">Verifier</span>
            <span class="gchip">Reporter</span>
            <span class="gchip">PR Bot</span>
          </div>
        </div>
        <div class="gameCard">
          <button class="gbtn" id="exitGameBtn">Exit game</button>
          <div class="gameSub" style="margin-top:8px;">Tip: click any event card to “follow” that correlation.</div>
        </div>
      </div>
    </div>
    <header>
      <div class="brand">
        <div class="logo"></div>
        <div class="mascot" title="PATCHIT is watching">
          <div class="antenna"></div>
          <div class="face"><span class="eye"></span><span class="eye"></span></div>
        </div>
        <div>
          <div class="title">PATCHIT Event Console</div>
          <div class="sub">Live audit stream • verify-before-PR • agent-first</div>
        </div>
        <div style="flex: 1"></div>
        <span class="pill glow" id="status">connecting…</span>
        <span class="pill glow" id="backendStatus">🟡 poller: unknown</span>
        <a href="/docs" target="_blank" class="pill">API docs</a>
      </div>
      <div class="row">
        <button id="refreshBtn">Refresh</button>
        <button id="connectorBtn" title="Configure Airflow URL and repo mapping.">Connect Airflow + Repo</button>
        <button id="resetPollerBtn" title="Clears poller seen-state and restarts poller so failures can be rediscovered.">Reset poller</button>
        <button id="clearViewBtn" title="Clears the UI event buffer in your browser (does not delete audit log).">Clear view</button>
        <label class="toggle" id="gameModeToggle" title="Show the live agent pipeline as an animated game scene.">
          <input id="gameMode" type="checkbox" /> Game mode
        </label>
        <div class="toggle" id="providerGroup" title="Switch the LLM provider for the next remediations (runtime override).">
          <span style="font-size:12px; color: rgba(229,231,235,0.78); margin-right: 8px;">Provider</span>
          <label class="toggle" id="providerOpenrouterToggle" style="padding: 6px 8px;">
            <input name="providerMode" id="providerOpenrouter" type="radio" value="openrouter" />
            OpenRouter
          </label>
          <label class="toggle" id="providerGroqToggle" style="padding: 6px 8px;">
            <input name="providerMode" id="providerGroq" type="radio" value="groq" />
            Groq
          </label>
          <label class="toggle" id="providerInhouseToggle" style="padding: 6px 8px;">
            <input name="providerMode" id="providerInhouse" type="radio" value="inhouse" />
            Inhouse
          </label>
        </div>
        <button id="inhouseConfigBtn" title="Configure Inhouse agent endpoint and headers.">Inhouse settings</button>
        <button id="platformsBtn" title="Connect Databricks/Snowflake/Glue.">Data platforms</button>
        <label class="toggle" id="useCursorToggle" title="Use Cursor Cloud Agent for patch generation; PATCHIT still verifies and creates PR.">
          <input id="useCursor" type="checkbox" /> Use Cursor agent
        </label>
        <label class="toggle" id="cursorCreatesPrToggle" title="If enabled, Cursor creates the PR. PATCHIT will still verify and adopt the PR link (no duplicate PR).">
          <input id="cursorCreatesPr" type="checkbox" /> Cursor PR
        </label>
        <label class="toggle" id="groupToggle"><input id="groupByCorr" type="checkbox" /> Group by correlation</label>
        <input id="corr" placeholder="Filter: correlation_id (contains)"/>
        <input id="etype" placeholder="Filter: event_type (contains)"/>
        <div class="spacer"></div>
        <span class="pill" id="countPill">0 events</span>
      </div>
    </header>

    <div id="connectorPanel" class="connector-panel hidden">
      <h4>Airflow + Repo Connector</h4>
      <div class="field">
        <label>Airflow base URL</label>
        <input id="airflowBaseUrl" placeholder="https://airflow.yourco.internal" />
      </div>
      <div class="field">
        <label>Airflow username</label>
        <input id="airflowUsername" placeholder="airflow" />
      </div>
      <div class="field">
        <label>Airflow password</label>
        <input id="airflowPassword" type="password" placeholder="••••••••" />
      </div>
      <div class="field">
        <label>Repo path (local folder)</label>
        <input id="repoRootOverride" placeholder="/Users/me/projects/my-airflow-repo" />
      </div>
      <div class="field">
        <label>Repo paths (one per line)</label>
        <textarea id="repoPaths" placeholder="/Users/me/projects/repo-a\n/Users/me/projects/repo-b"></textarea>
      </div>
      <div class="field">
        <label>Pipeline ID prefixes (optional, one per line)</label>
        <textarea id="repoPrefixes" placeholder="airflow\nspark\ndbt"></textarea>
      </div>
      <div class="row" style="margin-top: 6px;">
        <label class="toggle" id="pollerToggle">
          <input id="airflowPollerEnabled" type="checkbox" /> Enable Airflow poller
        </label>
      </div>
      <div class="field">
        <label>PR target</label>
        <div class="row">
          <label class="toggle"><input name="prTarget" id="prTargetGithub" type="radio" value="github" /> GitHub PR</label>
          <label class="toggle"><input name="prTarget" id="prTargetLocal" type="radio" value="local" /> Local worktree</label>
        </div>
      </div>
      <div class="field">
        <label>Local worktree root (optional)</label>
        <input id="localWorktreeRoot" placeholder="var/worktrees" />
      </div>
      <div class="field">
        <label>Local validate command (optional)</label>
        <input id="localValidateCmd" placeholder="bash -lc 'cd {repo} && pytest -q'" />
      </div>
      <div class="row" style="margin-top: 6px;">
        <button id="connectorSaveBtn">Save</button>
        <button id="connectorSaveRestartBtn">Save + Restart poller</button>
        <button id="connectorTestBtn">Test connection</button>
        <button id="connectorCloseBtn">Close</button>
      </div>
    </div>

    <div id="inhousePanel" class="inhouse-panel hidden">
      <h4>Inhouse Agent Config</h4>
      <div class="field">
        <label>Agent endpoint URL</label>
        <input id="inhouseUrl" placeholder="https://inhouse-ai.yourco/api/patch" />
      </div>
      <div class="field">
        <label>API key (optional)</label>
        <input id="inhouseApiKey" placeholder="sk-..." />
      </div>
      <div class="field">
        <label>Extra headers JSON (optional)</label>
        <textarea id="inhouseHeaders" placeholder='{"X-Org":"yourco","X-Env":"staging"}'></textarea>
      </div>
      <div class="row" style="margin-top: 6px;">
        <button id="inhouseSaveBtn">Save + Use Inhouse</button>
        <button id="inhouseTestBtn">Test connection</button>
        <button id="inhouseCloseBtn">Close</button>
      </div>
      <div class="sub" style="margin-top: 6px;">Expected response: JSON with a <span class="mono">diff</span> or <span class="mono">patch</span> field.</div>
    </div>

    <div id="platformsPanel" class="connector-panel hidden">
      <h4>Data Platform Connectors</h4>
      <div class="sub" style="margin-bottom: 8px;">Recommended: use platform-native eventing to POST failures to PATCHIT (least credentials, best security). Polling is optional and requires credentials.</div>

      <div class="field">
        <label>PATCHIT ingest URL (use in webhooks)</label>
        <input id="patchitIngestUrl" readonly />
      </div>

      <div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid rgba(255,255,255,0.08);">
        <h4 style="margin:0 0 8px 0;">Databricks (Jobs)</h4>
        <div class="field">
          <label>Workspace URL</label>
          <input id="dbxHost" placeholder="https://adb-xxxxxxxx.18.azuredatabricks.net" />
        </div>
        <div class="field">
          <label>Token</label>
          <input id="dbxToken" type="password" placeholder="dapi..." />
        </div>
        <div class="row">
          <button id="dbxSaveBtn">Save</button>
          <button id="dbxTestBtn">Test connection</button>
        </div>
        <div class="sub" style="margin-top: 6px;">
          Webhook option: configure Job <span class="mono">webhook_notifications.on_failure</span> to call the ingest URL above (via a small relay if required).
        </div>
      </div>

      <div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid rgba(255,255,255,0.08);">
        <h4 style="margin:0 0 8px 0;">Snowflake</h4>
        <div class="sub">
          Best practice: use Snowflake Tasks/Alerts to emit failure notifications to an HTTP endpoint (often via cloud event bus + API destination).
          PATCHIT can consume those events via the ingest URL above.
        </div>
      </div>

      <div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid rgba(255,255,255,0.08);">
        <h4 style="margin:0 0 8px 0;">AWS Glue</h4>
        <div class="sub">
          Best practice: route Glue Job state-change events from EventBridge → API Destination → PATCHIT ingest URL.
        </div>
      </div>

      <div class="row" style="margin-top: 10px;">
        <button id="platformsCloseBtn">Close</button>
      </div>
    </div>

    <div class="layout">
      <div class="panel">
        <div class="panel-h">
          <h3>Events</h3>
          <div class="hint">Newest first • click a card for details • grouped view reduces noise</div>
        </div>
        <div class="table">
          <div class="thead">
            <div class="cols"><span>time</span><span>correlation</span><span>event</span></div>
            <span class="small">tip: select any card → right panel animates progress</span>
          </div>
          <div id="rows" class="feed"></div>
        </div>
      </div>

      <div class="panel">
        <div class="panel-h">
          <h3>Flow</h3>
          <div class="hint" id="selHint">select an event</div>
        </div>
        <div style="padding: 12px; border-bottom: 1px solid rgba(255,255,255,0.06);">
          <div class="sub" style="margin-bottom: 8px;">Progress (per correlation_id)</div>
          <div class="flowbar" style="margin: 0 0 10px 0;"><div id="flowPct"></div></div>
          <div id="flowSteps" style="display:flex; flex-wrap:wrap; gap:8px;"></div>
        </div>
        <div class="kv">
          <div class="k">ts</div><div class="v mono" id="selTs">—</div>
          <div class="k">correlation</div><div class="v mono" id="selCorr">—</div>
          <div class="k">event_type</div><div class="v mono" id="selType">—</div>
          <div class="k">summary</div><div class="v mono" id="selSum">—</div>
          <div class="k">links</div><div class="v" id="selLinks">—</div>
        </div>
        <pre id="detailPre" class="mono">{}</pre>
      </div>
    </div>

    <!-- Bottom “agent stage”: driven by real audit events (pipeline-agnostic). -->
    <div class="agentDock" id="agentDock">
      <div class="agentDockInner">
        <div class="agentDockTop">
          <div class="agentDockTitle">
            <div class="agentLabel">Agent Run</div>
            <div class="agentSub mono" id="agentCorr">correlation: —</div>
          </div>
          <div class="agentStatePill" id="agentState"><span class="miniDot"></span><span id="agentStateTxt">idle</span></div>
        </div>
        <div class="sub" id="agentLast" style="padding: 0 4px 2px 4px; color: rgba(229,231,235,0.78);"></div>
        <div class="stage" id="stage">
          <div class="blocks" id="stageBlocks"></div>
          <div class="spriteWrap" id="spriteWrap" style="--p:0;">
            <div class="sprite think" id="sprite">
              <!-- Original retro plumber-style SVG (no Nintendo assets). -->
              <svg viewBox="0 0 34 34" aria-hidden="true">
                <!-- body -->
                <rect x="0" y="0" width="34" height="34" rx="10" ry="10" fill="rgba(255,255,255,0.00)"/>
                <!-- head -->
                <rect x="12" y="7" width="10" height="8" rx="3" fill="#f4c7a6"/>
                <!-- hat -->
                <rect x="11" y="5" width="12" height="4" rx="2" fill="#ef4444"/>
                <rect x="10" y="8" width="14" height="2" rx="1" fill="#b91c1c"/>
                <!-- face -->
                <rect x="14" y="10" width="2" height="2" rx="1" fill="#111827"/>
                <rect x="18" y="10" width="2" height="2" rx="1" fill="#111827"/>
                <rect x="16" y="12" width="2" height="2" rx="1" fill="#111827" opacity="0.35"/>
                <!-- torso -->
                <rect x="11" y="15" width="12" height="8" rx="3" fill="#2563eb"/>
                <rect x="13" y="15" width="8" height="8" rx="3" fill="#3b82f6" opacity="0.55"/>
                <!-- arms -->
                <rect x="8" y="16" width="4" height="6" rx="2" fill="#f4c7a6"/>
                <rect x="22" y="16" width="4" height="6" rx="2" fill="#f4c7a6"/>
                <!-- legs -->
                <rect x="12" y="23" width="4" height="6" rx="2" fill="#111827"/>
                <rect x="18" y="23" width="4" height="6" rx="2" fill="#111827"/>
                <!-- shoes -->
                <rect x="11" y="28" width="6" height="3" rx="2" fill="#111827"/>
                <rect x="17" y="28" width="6" height="3" rx="2" fill="#111827"/>

                <!-- tools -->
                <g class="tool book">
                  <rect x="1" y="13" width="8" height="8" rx="2" fill="#10b981" opacity="0.95"/>
                  <rect x="2" y="14" width="6" height="6" rx="1" fill="#064e3b" opacity="0.35"/>
                </g>
                <g class="tool lens">
                  <circle cx="5" cy="18" r="4" fill="rgba(96,165,250,0.90)"/>
                  <circle cx="5" cy="18" r="2" fill="rgba(0,0,0,0.25)"/>
                  <rect x="8" y="20" width="6" height="2" rx="1" fill="rgba(96,165,250,0.85)"/>
                </g>
                <g class="tool hammer">
                  <rect x="2" y="12" width="6" height="3" rx="1" fill="#9ca3af"/>
                  <rect x="6" y="10" width="2" height="8" rx="1" fill="#92400e"/>
                </g>
                <g class="tool flask">
                  <path d="M4 12 L6 12 L6 16 Q6 18 7 20 Q8 22 7 24 Q6 26 5 26 Q4 26 3 24 Q2 22 3 20 Q4 18 4 16 Z" fill="rgba(196,181,253,0.85)"/>
                  <path d="M4 20 Q5 22 6 20 Q6 24 5 24 Q4 24 4 20 Z" fill="rgba(52,211,153,0.65)"/>
                </g>
                <g class="tool flag">
                  <rect x="2" y="10" width="2" height="14" rx="1" fill="#e5e7eb" opacity="0.75"/>
                  <path d="M4 11 L12 13 L4 15 Z" fill="#34d399" opacity="0.95"/>
                </g>
                <g class="tool scroll">
                  <rect x="1.5" y="12" width="9" height="10" rx="3" fill="rgba(255,255,255,0.75)"/>
                  <rect x="3" y="14" width="6" height="1.5" rx="1" fill="rgba(17,24,39,0.25)"/>
                  <rect x="3" y="17" width="6" height="1.5" rx="1" fill="rgba(17,24,39,0.25)"/>
                </g>
              </svg>
            </div>
            <div class="speech mono" id="speech">standing by…</div>
          </div>
          <div class="track"><div class="trackFill" id="trackFill"></div></div>
        </div>
      </div>
    </div>

    <script>
      const rowsEl = document.getElementById('rows');
      const preEl = document.getElementById('detailPre');
      const statusEl = document.getElementById('status');
      const backendStatusEl = document.getElementById('backendStatus');
      const corrEl = document.getElementById('corr');
      const etypeEl = document.getElementById('etype');
      const groupByEl = document.getElementById('groupByCorr');
      const groupToggle = document.getElementById('groupToggle');
      const countPill = document.getElementById('countPill');
      const selHint = document.getElementById('selHint');
      const selTs = document.getElementById('selTs');
      const selCorr = document.getElementById('selCorr');
      const selType = document.getElementById('selType');
      const selSum = document.getElementById('selSum');
      const selLinks = document.getElementById('selLinks');
      const flowStepsEl = document.getElementById('flowSteps');
      const flowPctEl = document.getElementById('flowPct');
      const fxCanvas = document.getElementById('fxCanvas');
      const fx = fxCanvas.getContext('2d');
      const gameMode = document.getElementById('gameMode');
      const gameWrap = document.getElementById('gameWrap');
      const gameCanvas = document.getElementById('gameCanvas');
      const gameCtx = gameCanvas.getContext('2d');
      const exitGameBtn = document.getElementById('exitGameBtn');
      const gameCorr = document.getElementById('gameCorr');
      const gameStageTxt = document.getElementById('gameStageTxt');
      const agentCorr = document.getElementById('agentCorr');
      const agentState = document.getElementById('agentState');
      const agentStateTxt = document.getElementById('agentStateTxt');
      const spriteWrap = document.getElementById('spriteWrap');
      const sprite = document.getElementById('sprite');
      const speech = document.getElementById('speech');
      const stageBlocks = document.getElementById('stageBlocks');
      const trackFill = document.getElementById('trackFill');
      const providerGroup = document.getElementById('providerGroup');
      const providerOpenrouter = document.getElementById('providerOpenrouter');
      const providerGroq = document.getElementById('providerGroq');
      const providerInhouse = document.getElementById('providerInhouse');
      const providerOpenrouterToggle = document.getElementById('providerOpenrouterToggle');
      const providerGroqToggle = document.getElementById('providerGroqToggle');
      const providerInhouseToggle = document.getElementById('providerInhouseToggle');
      const useCursor = document.getElementById('useCursor');
      const useCursorToggle = document.getElementById('useCursorToggle');
      const cursorCreatesPr = document.getElementById('cursorCreatesPr');
      const cursorCreatesPrToggle = document.getElementById('cursorCreatesPrToggle');
      const inhouseConfigBtn = document.getElementById('inhouseConfigBtn');
      const inhousePanel = document.getElementById('inhousePanel');
      const inhouseUrl = document.getElementById('inhouseUrl');
      const inhouseApiKey = document.getElementById('inhouseApiKey');
      const inhouseHeaders = document.getElementById('inhouseHeaders');
      const inhouseSaveBtn = document.getElementById('inhouseSaveBtn');
      const inhouseCloseBtn = document.getElementById('inhouseCloseBtn');
      const connectorBtn = document.getElementById('connectorBtn');
      const connectorPanel = document.getElementById('connectorPanel');
      const airflowBaseUrl = document.getElementById('airflowBaseUrl');
      const airflowUsername = document.getElementById('airflowUsername');
      const airflowPassword = document.getElementById('airflowPassword');
      const repoRootOverride = document.getElementById('repoRootOverride');
      const repoPaths = document.getElementById('repoPaths');
      const repoPrefixes = document.getElementById('repoPrefixes');
      const airflowPollerEnabled = document.getElementById('airflowPollerEnabled');
      const prTargetGithub = document.getElementById('prTargetGithub');
      const prTargetLocal = document.getElementById('prTargetLocal');
      const localWorktreeRoot = document.getElementById('localWorktreeRoot');
      const localValidateCmd = document.getElementById('localValidateCmd');
      const connectorSaveBtn = document.getElementById('connectorSaveBtn');
      const connectorSaveRestartBtn = document.getElementById('connectorSaveRestartBtn');
      const connectorTestBtn = document.getElementById('connectorTestBtn');
      const connectorCloseBtn = document.getElementById('connectorCloseBtn');
      const inhouseTestBtn = document.getElementById('inhouseTestBtn');
      const platformsBtn = document.getElementById('platformsBtn');
      const platformsPanel = document.getElementById('platformsPanel');
      const platformsCloseBtn = document.getElementById('platformsCloseBtn');
      const patchitIngestUrl = document.getElementById('patchitIngestUrl');
      const dbxHost = document.getElementById('dbxHost');
      const dbxToken = document.getElementById('dbxToken');
      const dbxSaveBtn = document.getElementById('dbxSaveBtn');
      const dbxTestBtn = document.getElementById('dbxTestBtn');
      const agentLast = document.getElementById('agentLast');
      const resetPollerBtn = document.getElementById('resetPollerBtn');
      const clearViewBtn = document.getElementById('clearViewBtn');
      let showHeartbeats = false;

      let buffer = [];
      let lastSeenKey = null;
      let selectedKey = null;
      let activeCorrId = '';
      let didAutoFixProvider = false;
      const pollerState = {
        lastStartedTs: null,
        baseUrl: null,
        intervalS: null,
        lastErrorTs: null,
        lastError: null,
        lastBackoffTs: null,
        backoffS: null,
      };

      function _tsToMs(ts) {
        const n = Date.parse(ts || '');
        return Number.isFinite(n) ? n : null;
      }

      // ---------------- Game Mode renderer (isometric “3D-ish” scene) ----------------
      const GAME = {
        on: false,
        w: 0, h: 0,
        t: 0,
        followCorr: '',
        zoom: 1.25,
        camY: 140,
        // stations laid out on a simple isometric board
        stations: {
          ingest:   { x: 2, y: 2, label: 'INGEST' },
          safety:   { x: 6, y: 2, label: 'SAFETY' },
          rca:      { x: 10, y: 2, label: 'RCA' },
          fix:      { x: 14, y: 2, label: 'FIX' },
          verify:   { x: 10, y: 6, label: 'VERIFY' },
          report:   { x: 6, y: 6, label: 'REPORT' },
          pr:       { x: 14, y: 6, label: 'PR' },
        },
        agents: {
          runner:   { name: 'Runner',  color: '#60a5fa', pos: {x:2,y:2}, target:{x:2,y:2} },
          safety:   { name: 'Safety',  color: '#fbbf24', pos: {x:6,y:2}, target:{x:6,y:2} },
          rca:      { name: 'RCA',     color: '#c4b5fd', pos: {x:10,y:2}, target:{x:10,y:2} },
          fixer:    { name: 'Fixer',   color: '#34d399', pos: {x:14,y:2}, target:{x:14,y:2} },
          verifier: { name: 'Verifier',color: '#93c5fd', pos: {x:10,y:6}, target:{x:10,y:6} },
          reporter: { name: 'Reporter',color: '#f87171', pos: {x:6,y:6}, target:{x:6,y:6} },
          prbot:    { name: 'PR Bot',  color: '#22c55e', pos: {x:14,y:6}, target:{x:14,y:6} },
        },
        lastStage: 'idle',
      };

      function gameResize(){
        const dpr = Math.max(1, window.devicePixelRatio || 1);
        GAME.w = gameCanvas.clientWidth || window.innerWidth;
        GAME.h = gameCanvas.clientHeight || window.innerHeight;
        gameCanvas.width = Math.floor(GAME.w * dpr);
        gameCanvas.height = Math.floor(GAME.h * dpr);
        gameCtx.setTransform(dpr,0,0,dpr,0,0);
      }

      function iso(x,y,z){
        // Dynamic scaling so the “board” feels big on wide screens (your screenshot showed it too tiny).
        // Users can adjust zoom; defaults tuned for a “presentation” view.
        const zf = (GAME.zoom || 1.25);
        const base = Math.max(52, Math.min(104, Math.min(GAME.w, GAME.h) / 11));
        const tileW = base * zf;
        const tileH = tileW * 0.55;
        const sx = (x - y) * (tileW/2);
        const sy = (x + y) * (tileH/2) - (z||0);
        return {x:sx, y:sy};
      }

      function lerp(a,b,t){ return a + (b-a)*t; }

      // roundRect fallback for Safari/older browsers
      function rr(ctx, x, y, w, h, r){
        const rad = Math.max(0, Math.min(r, Math.min(w,h)/2));
        ctx.beginPath();
        if (ctx.roundRect) {
          ctx.roundRect(x, y, w, h, rad);
          return;
        }
        ctx.moveTo(x + rad, y);
        ctx.arcTo(x + w, y, x + w, y + h, rad);
        ctx.arcTo(x + w, y + h, x, y + h, rad);
        ctx.arcTo(x, y + h, x, y, rad);
        ctx.arcTo(x, y, x + w, y, rad);
        ctx.closePath();
      }

      function drawIsoBox(cx, cy, w, h, dz, colorTop, colorSide){
        const ctx = gameCtx;
        ctx.save();
        ctx.translate(cx, cy);
        // top
        ctx.beginPath();
        ctx.moveTo(0, -dz);
        ctx.lineTo(w/2, h/2 - dz);
        ctx.lineTo(0, h - dz);
        ctx.lineTo(-w/2, h/2 - dz);
        ctx.closePath();
        ctx.fillStyle = colorTop;
        ctx.fill();
        // left side
        ctx.beginPath();
        ctx.moveTo(-w/2, h/2 - dz);
        ctx.lineTo(0, h - dz);
        ctx.lineTo(0, h);
        ctx.lineTo(-w/2, h/2);
        ctx.closePath();
        ctx.fillStyle = colorSide;
        ctx.fill();
        // right side
        ctx.beginPath();
        ctx.moveTo(w/2, h/2 - dz);
        ctx.lineTo(0, h - dz);
        ctx.lineTo(0, h);
        ctx.lineTo(w/2, h/2);
        ctx.closePath();
        ctx.fillStyle = 'rgba(0,0,0,0.18)';
        ctx.fill();
        ctx.restore();
      }

      function drawAgent(ax, ay, color, label){
        const p = iso(ax, ay, 0);
        const baseX = GAME.w/2 + p.x;
        const baseY = GAME.h/2 - (GAME.camY || 140) + p.y;
        // shadow
        gameCtx.fillStyle = 'rgba(0,0,0,0.25)';
        gameCtx.beginPath();
        const sh = 18 * (GAME.zoom || 1.25);
        gameCtx.ellipse(baseX, baseY+26, sh, sh*0.5, 0, 0, Math.PI*2);
        gameCtx.fill();
        // body (bigger, more character)
        const k = (GAME.zoom || 1.25);
        const bodyW = 34 * k;
        const bodyH = 22 * k;
        const bodyZ = 24 * k;
        drawIsoBox(baseX, baseY-12*k, bodyW, bodyH, bodyZ, color, 'rgba(255,255,255,0.06)');

        // “Hero” face plate (plumber-style) + tiny companion (electric mouse-like) for visual fun.
        // (Vector art only — no external/copyrighted assets.)
        gameCtx.save();
        gameCtx.translate(baseX, baseY - 30*k);
        // plumber hat
        gameCtx.fillStyle = 'rgba(239,68,68,0.95)';
        rr(gameCtx, -14*k, -10*k, 28*k, 10*k, 6*k);
        gameCtx.fill();
        gameCtx.fillStyle = 'rgba(185,28,28,0.95)';
        rr(gameCtx, -16*k, -4*k, 32*k, 6*k, 6*k);
        gameCtx.fill();
        // face
        gameCtx.fillStyle = 'rgba(244,199,166,0.96)';
        rr(gameCtx, -10*k, 0, 20*k, 14*k, 7*k);
        gameCtx.fill();
        // eyes
        gameCtx.fillStyle = 'rgba(17,24,39,0.95)';
        gameCtx.beginPath(); gameCtx.arc(-4*k, 6*k, 1.7*k, 0, Math.PI*2); gameCtx.fill();
        gameCtx.beginPath(); gameCtx.arc(4*k, 6*k, 1.7*k, 0, Math.PI*2); gameCtx.fill();
        // moustache hint
        gameCtx.fillStyle = 'rgba(17,24,39,0.25)';
        rr(gameCtx, -7*k, 9*k, 14*k, 3*k, 2*k);
        gameCtx.fill();

        // companion (electric buddy)
        gameCtx.translate(20*k, 2*k);
        gameCtx.fillStyle = 'rgba(251,191,36,0.95)';
        rr(gameCtx, -6*k, -6*k, 12*k, 12*k, 4*k);
        gameCtx.fill();
        gameCtx.fillStyle = 'rgba(17,24,39,0.85)';
        gameCtx.beginPath(); gameCtx.arc(-2*k, -1*k, 1.2*k, 0, Math.PI*2); gameCtx.fill();
        gameCtx.beginPath(); gameCtx.arc(2*k, -1*k, 1.2*k, 0, Math.PI*2); gameCtx.fill();
        gameCtx.strokeStyle = 'rgba(251,191,36,0.90)';
        gameCtx.lineWidth = 2*k;
        gameCtx.beginPath();
        gameCtx.moveTo(8*k, -6*k);
        gameCtx.lineTo(14*k, -10*k);
        gameCtx.lineTo(12*k, -2*k);
        gameCtx.stroke();
        gameCtx.restore();

        // label
        gameCtx.fillStyle = 'rgba(229,231,235,0.92)';
        gameCtx.font = Math.round(13 * k) + 'px ui-sans-serif, system-ui';
        gameCtx.textAlign = 'center';
        gameCtx.fillText(label, baseX, baseY-40*k);
      }

      function drawStation(s){
        const p = iso(s.x, s.y, 0);
        const x = GAME.w/2 + p.x;
        const y = GAME.h/2 - (GAME.camY || 140) + p.y;
        const k = (GAME.zoom || 1.25);
        drawIsoBox(x, y, 70*k, 42*k, 28*k, 'rgba(255,255,255,0.11)', 'rgba(255,255,255,0.06)');
        gameCtx.fillStyle = 'rgba(229,231,235,0.85)';
        gameCtx.font = Math.round(13 * k) + 'px ui-sans-serif, system-ui';
        gameCtx.textAlign = 'center';
        gameCtx.fillText(s.label, x, y-(36*k));
      }

      function gameTick(ts){
        if (!GAME.on) return;
        GAME.t = ts || 0;
        gameCtx.clearRect(0,0,GAME.w,GAME.h);
        // ground grid
        gameCtx.save();
        gameCtx.translate(GAME.w/2, GAME.h/2 - (GAME.camY || 140));
        // Bigger board so it fills screen
        const maxX = 22, maxY = 14;
        for(let gx=0; gx<maxX; gx++){
          for(let gy=0; gy<maxY; gy++){
            const p = iso(gx,gy,0);
            gameCtx.beginPath();
            // derive diamond corners from iso() tile geometry
            const tileW = Math.max(52, Math.min(104, Math.min(GAME.w, GAME.h) / 11)) * (GAME.zoom || 1.25);
            const tileH = tileW * 0.55;
            gameCtx.moveTo(p.x, p.y);
            gameCtx.lineTo(p.x+(tileW/2), p.y+(tileH/2));
            gameCtx.lineTo(p.x, p.y+(tileH));
            gameCtx.lineTo(p.x-(tileW/2), p.y+(tileH/2));
            gameCtx.closePath();
            const c = ((gx+gy)%2===0) ? 'rgba(255,255,255,0.03)' : 'rgba(255,255,255,0.015)';
            gameCtx.fillStyle = c;
            gameCtx.fill();
          }
        }
        gameCtx.restore();

        // stations
        Object.values(GAME.stations).forEach(drawStation);

        // move agents toward target
        Object.values(GAME.agents).forEach(a=>{
          a.pos.x = lerp(a.pos.x, a.target.x, 0.08);
          a.pos.y = lerp(a.pos.y, a.target.y, 0.08);
        });

        // draw agents (order by y for depth)
        const agents = Object.values(GAME.agents).slice().sort((a,b)=> (a.pos.y - b.pos.y));
        agents.forEach(a=> drawAgent(a.pos.x, a.pos.y, a.color, a.name));

        requestAnimationFrame(gameTick);
      }

      function gameSetStage(stage){
        GAME.lastStage = stage || 'idle';
        gameStageTxt.textContent = 'stage: ' + GAME.lastStage;
        // simple mapping: move the “runner” along, and awaken specialist agents
        if(stage === 'event.received'){ GAME.agents.runner.target = {x:2,y:2}; }
        if(stage === 'safety'){ GAME.agents.safety.target = {x:6,y:2}; }
        if(stage === 'rca'){ GAME.agents.rca.target = {x:10,y:2}; GAME.agents.runner.target = {x:10,y:2}; }
        if(stage === 'fix'){ GAME.agents.fixer.target = {x:14,y:2}; GAME.agents.runner.target = {x:14,y:2}; }
        if(stage === 'verify'){ GAME.agents.verifier.target = {x:10,y:6}; GAME.agents.runner.target = {x:10,y:6}; }
        if(stage === 'report'){ GAME.agents.reporter.target = {x:6,y:6}; }
        if(stage === 'pr'){ GAME.agents.prbot.target = {x:14,y:6}; }
      }

      function setGameOn(on){
        GAME.on = !!on;
        if(GAME.on){
          gameWrap.classList.remove('hidden');
          document.querySelector('.layout').classList.add('hidden');
          document.getElementById('agentDock').classList.add('hidden');
          gameResize();
          requestAnimationFrame(gameTick);
        } else {
          gameWrap.classList.add('hidden');
          document.querySelector('.layout').classList.remove('hidden');
          document.getElementById('agentDock').classList.remove('hidden');
        }
      }

      window.addEventListener('resize', ()=>{ if(GAME.on) gameResize(); });
      exitGameBtn.addEventListener('click', ()=>{ gameMode.checked = false; setGameOn(false); });
      gameMode.addEventListener('change', ()=> setGameOn(gameMode.checked));

      // Game zoom controls (presentation-friendly)
      const zoomEl = document.getElementById('gameZoom');
      if (zoomEl) {
        zoomEl.addEventListener('input', () => {
          const v = Number(zoomEl.value || '1.25');
          GAME.zoom = Math.max(0.8, Math.min(2.0, v));
        });
      }

      function updateBackendStatusPill() {
        // “Single status” UX: show one collapsed poller state instead of a noisy event stream.
        // Emoji meanings:
        //   🟢 healthy (recent cycles, no recent errors)
        //   🟠 degraded (backoff)
        //   🔴 down (recent error)
        //   🟡 unknown (no poller events seen)
        const now = Date.now();
        const errAge = pollerState.lastErrorTs ? (now - pollerState.lastErrorTs) : null;
        const backoffAge = pollerState.lastBackoffTs ? (now - pollerState.lastBackoffTs) : null;
        const startedAge = pollerState.lastStartedTs ? (now - pollerState.lastStartedTs) : null;

        if (!pollerState.lastStartedTs) {
          backendStatusEl.textContent = '🟡 poller: unknown';
          return;
        }

        // If an error happened “recently”, prefer red.
        if (errAge !== null && errAge < 60_000) {
          backendStatusEl.textContent = '🔴 poller: airflow unreachable (recent errors)';
          return;
        }

        // If we’re backing off recently, show orange.
        if (backoffAge !== null && backoffAge < 90_000 && pollerState.backoffS) {
          backendStatusEl.textContent = '🟠 poller: backing off (' + pollerState.backoffS + 's)';
          return;
        }

        // Otherwise: green. Include interval so it’s obvious it polls every N seconds.
        const iv = (pollerState.intervalS !== null && pollerState.intervalS !== undefined) ? pollerState.intervalS : '?';
        if (startedAge !== null && startedAge < 6 * 60_000) {
          backendStatusEl.textContent = '🟢 poller: running (every ' + iv + 's)';
          return;
        }
        backendStatusEl.textContent = '🟢 poller: running';
      }

      function maybeConsumePollerEvent(obj) {
        if (!obj) return;
        if ((obj.correlation_id || '') !== 'poller') return;
        const et = (obj.event_type || '');
        if (!et.startsWith('poller.')) return;
        if (et === 'poller.started') {
          pollerState.lastStartedTs = _tsToMs(obj.ts);
          try {
            pollerState.baseUrl = (obj.payload && obj.payload.base_url) ? obj.payload.base_url : pollerState.baseUrl;
            pollerState.intervalS = (obj.payload && obj.payload.interval_s !== undefined) ? obj.payload.interval_s : pollerState.intervalS;
          } catch(e) {}
        } else if (et === 'poller.error') {
          pollerState.lastErrorTs = _tsToMs(obj.ts);
          pollerState.lastError = (obj.payload && obj.payload.error) ? obj.payload.error : null;
        } else if (et === 'poller.backoff') {
          pollerState.lastBackoffTs = _tsToMs(obj.ts);
          pollerState.backoffS = (obj.payload && obj.payload.backoff_s !== undefined) ? obj.payload.backoff_s : pollerState.backoffS;
        }
        updateBackendStatusPill();
      }

      function stageMilestones() {
        // Keep this aligned with flowMilestones() but with friendlier “story” labels
        return [
          { key: 'event.received', label: 'arrived' },
          { key: 'log.fetched', label: 'logs' },
          { key: 'error.parsed', label: 'parse' },
          { key: 'context.collected', label: 'code' },
          { key: 'index.built', label: 'index' },
          { key: 'agent.attempt_started', label: 'think' },
          { key: 'prompt.rca', label: 'rca' },
          { key: 'prompt.fix', label: 'fix' },
          { key: 'prompt.validate', label: 'plan' },
          { key: 'prompt.decide', label: 'decide' },
          { key: 'agent.patch_proposed', label: 'patch' },
          { key: 'verify.finished', label: 'test' },
          { key: 'patch.saved', label: 'pack' },
          { key: 'report.saved', label: 'report' },
          { key: 'pr.created', label: 'ship' },
        ];
      }

      function ensureStageBlocks() {
        if (stageBlocks.childElementCount) return;
        for (let i = 0; i < stageMilestones().length; i++) {
          const b = document.createElement('div');
          b.className = 'block';
          stageBlocks.appendChild(b);
        }
      }

      function isTerminalEventType(etRaw, payload) {
        const et = (etRaw || '').toLowerCase();
        if (!et) return false;
        if (et === 'pr.created') return true;
        if (et === 'report.saved') return true;
        if (et === 'patch.none') return true;
        if (et === 'remediation.escalated') return true;
        if (et === 'policy.decided' && payload && payload.phase === 'final') return true;
        if (et.includes('create_failed')) return true;
        return false;
      }

      function setAgentUI({ corrId, stateTxt, level, p, spriteMode, speechTxt, hasError }) {
        ensureStageBlocks();
        // IMPORTANT: allow explicit clearing of activeCorrId by passing an empty string.
        // (Using `corrId || activeCorrId` makes it impossible to clear the dock.)
        if (corrId !== undefined) activeCorrId = corrId;
        activeCorrId = activeCorrId || '';
        agentCorr.textContent = activeCorrId ? ('correlation: ' + activeCorrId) : 'correlation: —';
        agentStateTxt.textContent = stateTxt || 'idle';
        agentState.className = 'agentStatePill ' + (level || '');

        // progress
        const pp = Math.max(0, Math.min(1, p || 0));
        spriteWrap.style.setProperty('--p', String(pp));
        trackFill.style.width = Math.round(pp * 100) + '%';

        // sprite tool mode
        const base = 'sprite ' + (spriteMode || 'think');
        // If we're mid-flight (not ship/report), keep it “walking/working”
        const walking = (spriteMode === 'read' || spriteMode === 'think' || spriteMode === 'patch' || spriteMode === 'test');
        sprite.className = base + (walking ? ' walking walking' : '');
        speech.textContent = speechTxt || 'standing by…';

        // blocks
        const blocks = stageBlocks.children;
        const seenN = Math.max(0, Math.min(blocks.length, Math.round(pp * (blocks.length))));
        for (let i = 0; i < blocks.length; i++) {
          blocks[i].className = 'block' + (i < seenN ? ' on' : '');
        }
        if (hasError) {
          // make the last block red-ish as a “checkpoint failed”
          if (blocks.length) blocks[blocks.length - 1].className = 'block err';
        }
      }

      function deriveAgentStageForCorr(corrId) {
        if (!corrId) return null;
        const events = buffer.filter(r => (r.correlation_id || '') === corrId);
        if (!events.length) return null;
        const seen = new Set(events.map(e => e.event_type));
        const hasErr = events.some(e => (e.event_type || '').includes('failed') || (e.event_type || '').includes('fetch_failed') || (e.event_type || '').includes('create_failed'));

        const ms = stageMilestones();
        let seenCount = 0;
        for (const m of ms) if (seen.has(m.key)) seenCount += 1;
        const p = seenCount / ms.length;

        // Choose a narrative event:
        // if we've reached an outcome, prefer the latest outcome so the dock doesn't regress back to "thinking..."
        const sig = events.filter(e => (e.event_type || '') !== 'agent.heartbeat');
        const pickLastOf = (t) => { const xs = sig.filter(e => (e.event_type || '') === t); return xs.length ? xs[xs.length - 1] : null; };
        const lastOutcome =
          pickLastOf('pr.created') ||
          pickLastOf('report.saved') ||
          pickLastOf('patch.saved') ||
          pickLastOf('patch.none') ||
          pickLastOf('remediation.escalated') ||
          pickLastOf('pr.create_failed') ||
          pickLastOf('verify.finished') ||
          null;
        const last = lastOutcome || (sig.length ? sig[sig.length - 1] : events[events.length - 1]);
        const et = (last.event_type || '').toLowerCase();
        const isTerminal = isTerminalEventType(et, last.payload || {});
        let mode = 'think';
        let label = 'thinking…';
        let lvl = 'info';
        if (et === 'event.received') { mode = 'think'; label = 'incoming failure'; }
        else if (et.startsWith('log.')) { mode = 'read'; label = 'reading logs'; }
        else if (et.startsWith('error.parsed') || et.startsWith('error.classified')) { mode = 'think'; label = 'analyzing traceback'; }
        else if (et === 'context.collected' || et === 'index.built') { mode = 'read'; label = 'reading codebase'; }
        else if (et === 'agent.attempt_started') { mode = 'think'; label = 'reasoning about fix'; }
        else if (et === 'agent.patch_proposed' || et.startsWith('patch.')) { mode = 'patch'; label = 'patching…'; }
        else if (et.startsWith('verify.')) { mode = 'test'; label = 'testing in sandbox'; }
        else if (et === 'report.saved') { mode = 'report'; label = 'report generated'; lvl = 'ok'; }
        else if (et === 'pr.created') { mode = 'ship'; label = 'PR opened'; lvl = 'ok'; }
        else if (et === 'patch.none') { mode = 'report'; label = 'no patch available'; lvl = 'warn'; }
        else if (et === 'remediation.escalated') { mode = 'report'; label = 'escalated (see report)'; lvl = 'warn'; }
        else if (et === 'policy.decided') { mode = 'report'; label = 'done'; lvl = 'ok'; }
        else if (et.includes('create_failed') || et.includes('patch_apply_failed')) { mode = 'test'; label = 'blocked'; lvl = 'err'; }

        if (hasErr && lvl !== 'ok') lvl = (lvl === 'err') ? 'err' : 'warn';
        if (!corrId) lvl = 'info';
        return { corrId, p, mode, label, lvl, hasErr, lastTs: (last.ts || null), isTerminal };
      }

      function fmtLink(href, label){
        return '<a class="pill" href="' + href + '" target="_blank" rel="noopener">' + label + '</a>';
      }

      function updateLastOutcome() {
        // Show the last completed “outcome” regardless of dock stage (helps on refresh).
        if (!buffer.length) { agentLast.innerHTML = ''; return; }
        const candidates = buffer.filter(r => (r.event_type || '') !== 'agent.heartbeat');
        if (!candidates.length) { agentLast.innerHTML = ''; return; }

        // Find the most recent meaningful outcome, not just the last event (policy.decided often comes last).
        const outcomeTypes = new Set(['pr.created','report.saved','patch.saved','patch.none','pr.create_failed']);
        let last = null;
        for (let i = candidates.length - 1; i >= 0; i--) {
          const et0 = candidates[i].event_type || '';
          if (outcomeTypes.has(et0)) { last = candidates[i]; break; }
        }
        if (!last) { agentLast.innerHTML = ''; return; }
        const et = last.event_type || '';
        const p = last.payload || {};
        const parts = [];
        let text = '';
        if (et === 'pr.created' && p && p.pr && p.pr.pr_url) {
          text = 'Last outcome: PR created';
          parts.push(fmtLink(p.pr.pr_url, 'open PR'));
        } else if (et === 'report.saved' && p.report_id) {
          text = 'Last outcome: report generated';
          parts.push(fmtLink('/api/report/' + p.report_id + '.md', 'download report'));
          parts.push(fmtLink('/api/report/' + p.report_id + '.log', 'validation log'));
        } else if (et === 'patch.saved' && p.patch_id) {
          text = 'Last outcome: patch saved';
          parts.push(fmtLink('/api/patch/' + p.patch_id + '.diff', 'download diff'));
        } else if ((et || '').includes('create_failed')) {
          text = 'Last outcome: blocked (PR creation failed)';
        } else if ((et || '').includes('patch.none')) {
          text = 'Last outcome: no patch available';
        } else {
          // Don't spam; keep it quiet if the latest event is mid-flight.
          agentLast.innerHTML = '';
          return;
        }
        agentLast.innerHTML = '<span class="mono">' + text + '</span> ' + parts.join(' ');
      }

      // Lightweight particles + confetti so UI feels alive (no deps).
      const particles = [];
      function resizeFx() {
        const dpr = window.devicePixelRatio || 1;
        fxCanvas.width = Math.floor(window.innerWidth * dpr);
        fxCanvas.height = Math.floor(window.innerHeight * dpr);
        fx.setTransform(dpr, 0, 0, dpr, 0, 0);
      }
      function rand(a,b){ return a + Math.random()*(b-a); }
      function spawn(n=24){
        for(let i=0;i<n;i++){
          particles.push({
            x: rand(0, window.innerWidth),
            y: rand(0, window.innerHeight),
            vx: rand(-0.12, 0.12),
            vy: rand(-0.10, 0.10),
            r: rand(1.0, 2.4),
            a: rand(0.10, 0.28),
            hue: (Math.random() < 0.5) ? 210 : (Math.random() < 0.5 ? 265 : 150),
            life: rand(140, 360),
            kind: 'float'
          });
        }
      }
      function confettiBurst(){
        const cx = window.innerWidth * 0.78;
        const cy = 120;
        for(let i=0;i<90;i++){
          particles.push({
            x: cx, y: cy,
            vx: rand(-2.2, 2.2),
            vy: rand(-2.5, 1.2),
            g: 0.035,
            r: rand(1.2, 2.6),
            a: rand(0.6, 0.95),
            hue: [210, 265, 150, 35][Math.floor(rand(0,4))],
            life: rand(40, 90),
            kind: 'confetti'
          });
        }
      }
      function tick(){
        fx.clearRect(0,0,window.innerWidth, window.innerHeight);
        // soft glow
        for(const p of particles){
          if(p.kind === 'float'){
            p.x += p.vx; p.y += p.vy;
            if(p.x < -20) p.x = window.innerWidth+20;
            if(p.x > window.innerWidth+20) p.x = -20;
            if(p.y < -20) p.y = window.innerHeight+20;
            if(p.y > window.innerHeight+20) p.y = -20;
          } else {
            p.x += p.vx; p.y += p.vy;
            p.vy += (p.g || 0);
          }
          p.life -= 1;
          const alpha = Math.max(0, Math.min(1, (p.life/120))) * (p.a || 0.2);
          fx.fillStyle = `hsla(${p.hue||210}, 85%, 65%, ${alpha})`;
          fx.beginPath();
          fx.arc(p.x, p.y, p.r, 0, Math.PI*2);
          fx.fill();
        }
        for(let i=particles.length-1;i>=0;i--){
          if(particles[i].life <= 0) particles.splice(i,1);
        }
        if(particles.length < 24) spawn(8);
        requestAnimationFrame(tick);
      }
      window.addEventListener('resize', () => { resizeFx(); });
      resizeFx();
      spawn(30);
      tick();

      function summarize(payload) {
        try {
          if (payload && payload.pr && payload.pr.pr_url) return payload.pr.pr_url;
          if (payload && payload.patch_id) return "saved diff: " + payload.patch_id;
          if (payload && payload.report && payload.report.failure_summary) return "escalated: " + payload.report.failure_summary;
          if (payload && payload.decision) return JSON.stringify(payload.decision);
          if (payload && payload.classification) return JSON.stringify(payload);
          if (payload && payload.log_uri) return JSON.stringify({log_uri: payload.log_uri});
          return JSON.stringify(payload).slice(0, 200);
        } catch(e) { return ''; }
      }

      function levelFor(eventType, payload) {
        const t = (eventType || '').toLowerCase();
        // Special: verifier per-command events should reflect command success/failure.
        if ((eventType || '') === 'verify.cmd_finished') {
          const rc = (payload && (payload.returncode !== undefined)) ? payload.returncode : null;
          if (rc === 0) return 'ok';
          if (rc === null) return 'warn';
          return 'err';
        }
        if (t.includes('remediation.escalated') || t.includes('patch.rejected')) return 'warn';
        if (t.includes('failed') || t.includes('error') || t.includes('exception') || t.includes('denied') || t.includes('patch_apply_failed')) return 'err';
        if (t.includes('verify.finished')) return 'warn';
        if (t.includes('pr.created') || t.includes('verify.ok') || t.includes('log.fetched')) return 'ok';
        return 'info';
      }

      function applyFilters(recs) {
        const c = (corrEl.value || '').trim();
        const e = (etypeEl.value || '').trim();
        const eLower = (e || '').toLowerCase();
        return recs.filter(r => {
          if (!showHeartbeats && (r.event_type || '') === 'agent.heartbeat') {
            // If user explicitly filters for heartbeats, show them.
            if (!e || !e.toLowerCase().includes('heartbeat')) return false;
          }
          // Hide poller noise by default; keep a single “backend status” pill instead.
          // If the user types "poller" into event_type filter, show them.
          if (((r.event_type || '').startsWith('poller.')) && !eLower.includes('poller')) {
            return false;
          }
          return (!c || (r.correlation_id||'').includes(c)) && (!e || (r.event_type||'').includes(e));
        });
      }

      function selectRecord(r, opts) {
        const driveStage = !(opts && opts.driveStage === false);
        preEl.textContent = JSON.stringify(r, null, 2);
        selHint.textContent = (r && r.correlation_id) ? ('correlation: ' + r.correlation_id.slice(0, 12) + '…') : 'select an event';
        selTs.textContent = r.ts || '—';
        selCorr.textContent = r.correlation_id || '—';
        selType.textContent = r.event_type || '—';
        selSum.textContent = summarize(r.payload || {}) || '—';
        // Action links (diff/report/pr)
        selLinks.innerHTML = '—';
        const links = [];
        try {
          const p = (r && r.payload) ? r.payload : {};
          if (r.event_type === 'patch.saved' && p.patch_id) {
            links.push({ href: '/api/patch/' + p.patch_id + '.diff', label: 'download diff' });
          }
          if (r.event_type === 'report.saved' && p.report_id) {
            links.push({ href: '/api/report/' + p.report_id + '.md', label: 'download report' });
          }
          if (p && p.pr && p.pr.pr_url) {
            links.push({ href: p.pr.pr_url, label: 'open PR' });
          }
        } catch(e) {}
        if (links.length) {
          selLinks.innerHTML = '';
          for (const l of links) {
            const a = document.createElement('a');
            a.className = 'pill';
            a.href = l.href;
            a.target = '_blank';
            a.textContent = l.label;
            selLinks.appendChild(a);
          }
        }
        selectedKey = (r.correlation_id || '') + '::' + (r.event_type || '') + '::' + (r.ts || '');
        renderFlow(r.correlation_id || '');
        // Drive bottom stage only on explicit user selection; auto-select keeps dock idle.
        if (driveStage) {
          activeCorrId = (r.correlation_id || '');
          const st = deriveAgentStageForCorr(activeCorrId);
          if (st) setAgentUI({ corrId: st.corrId, stateTxt: st.label, level: st.lvl, p: st.p, spriteMode: st.mode, speechTxt: st.label, hasError: st.hasErr });
          // Game mode: follow the selected correlation and animate agents toward the inferred stage.
          try {
            GAME.followCorr = activeCorrId;
            gameCorr.textContent = activeCorrId ? ('correlation: ' + activeCorrId) : 'correlation: —';
            if (st && GAME.on) {
              const lbl = (st.label || '').toLowerCase();
              if (lbl.includes('injection') || lbl.includes('refuse') || lbl.includes('policy')) gameSetStage('safety');
              else if (lbl.includes('rca')) gameSetStage('rca');
              else if (lbl.includes('patch')) gameSetStage('fix');
              else if (lbl.includes('verify') || lbl.includes('test')) gameSetStage('verify');
              else if (lbl.includes('report')) gameSetStage('report');
            }
          } catch(e) {}
        }
      }

      function flowMilestones() {
        return [
          { key: 'event.received', label: 'received' },
          { key: 'log.fetched', label: 'log' },
          { key: 'error.parsed', label: 'parse' },
          { key: 'context.collected', label: 'context' },
          { key: 'prompt.rca', label: 'rca' },
          { key: 'prompt.fix', label: 'fix' },
          { key: 'prompt.validate', label: 'validate' },
          { key: 'prompt.decide', label: 'decide' },
          { key: 'agent.patch_proposed', label: 'agent' },
          { key: 'verify.finished', label: 'verify' },
          { key: 'patch.saved', label: 'saved' },
          { key: 'report.saved', label: 'report' },
          { key: 'pr.created', label: 'PR' },
        ];
      }

      function renderFlow(corrId) {
        flowStepsEl.innerHTML = '';
        flowPctEl.style.width = '0%';
        if (!corrId) return;
        const events = buffer.filter(r => (r.correlation_id || '') === corrId);
        const seen = new Set(events.map(e => e.event_type));
        const hasFail = events.some(e => (e.event_type || '').includes('failed') || (e.event_type || '').includes('fetch_failed'));

        let seenCount = 0;
        for (const m of flowMilestones()) {
          const chip = document.createElement('span');
          chip.className = 'pill';
          chip.style.transition = 'transform 200ms ease, background 200ms ease, border-color 200ms ease';
          chip.textContent = m.label;

          if (seen.has(m.key)) {
            seenCount += 1;
            chip.style.color = 'var(--text)';
            chip.style.borderColor = 'rgba(52,211,153,0.35)';
            chip.style.background = 'rgba(52,211,153,0.10)';
            chip.style.transform = 'translateY(-1px)';
          }
          if (m.key === 'log.fetched' && events.some(e => e.event_type === 'log.fetch_failed')) {
            chip.style.borderColor = 'rgba(248,113,113,0.35)';
            chip.style.background = 'rgba(248,113,113,0.10)';
            chip.style.color = 'var(--text)';
          }
          flowStepsEl.appendChild(chip);
        }
        const pct = Math.round((seenCount / flowMilestones().length) * 100);
        flowPctEl.style.width = Math.max(6, pct) + '%';
        if (hasFail) {
          const warn = document.createElement('span');
          warn.className = 'pill';
          warn.style.borderColor = 'rgba(248,113,113,0.35)';
          warn.style.background = 'rgba(248,113,113,0.10)';
          warn.style.color = 'var(--text)';
          warn.textContent = 'blocked';
          flowStepsEl.appendChild(warn);
        }
        // Keep bottom stage synced with the selected correlation
        if (corrId && corrId === activeCorrId) {
          const st = deriveAgentStageForCorr(corrId);
          if (st) setAgentUI({ corrId: st.corrId, stateTxt: st.label, level: st.lvl, p: st.p, spriteMode: st.mode, speechTxt: st.label, hasError: st.hasErr });
        }
      }

      function shortCorr(id) {
        if (!id) return '—';
        return id.length > 12 ? (id.slice(0, 12) + '…') : id;
      }

      function tsShort(ts) {
        // keep it compact; we already have full ts in details
        return (ts || '').replace('T', ' ').replace('Z','').slice(0, 19);
      }

      function renderCard(r, isGroupedPrimary) {
        const card = document.createElement('div');
        card.className = 'card';
        const key = (r.correlation_id || '') + '::' + (r.event_type || '') + '::' + (r.ts || '');
        if (key === lastSeenKey) card.classList.add('new');
        if (key === selectedKey) card.classList.add('sel');

        const lvl = levelFor(r.event_type, r.payload || {});
        const dotCls = 'dot ' + lvl + (key === lastSeenKey ? ' pulse' : '');

        const top = document.createElement('div');
        top.className = 'cardTop';
        const meta = document.createElement('div');
        meta.className = 'meta mono';
        meta.innerHTML = '<span class=\"' + dotCls + '\"></span>' +
                         '<span class=\"small\">' + tsShort(r.ts || '') + '</span>' +
                         '<span class=\"small\">•</span>' +
                         '<span class=\"mono\">' + shortCorr(r.correlation_id || '') + '</span>';

        const badges = document.createElement('div');
        badges.className = 'badges';
        const et = document.createElement('span');
        et.className = 'badge ' + (lvl === 'err' ? 'red' : (lvl === 'warn' ? 'amber' : (lvl === 'ok' ? 'green' : 'blue')));
        et.textContent = (r.event_type || 'event');
        badges.appendChild(et);
        if (isGroupedPrimary) {
          const b = document.createElement('span');
          b.className = 'badge';
          b.textContent = 'trace';
          badges.appendChild(b);
        }

        top.appendChild(meta);
        top.appendChild(badges);

        const msg = document.createElement('div');
        msg.className = 'cardMsg mono';
        msg.textContent = summarize(r.payload || {});

        card.appendChild(top);
        card.appendChild(msg);

        card.onclick = () => selectRecord(r);
        return card;
      }

      function render() {
        const filtered = applyFilters(buffer);
        countPill.textContent = filtered.length + ' events';
        rowsEl.innerHTML = '';

        if (groupByEl.checked) {
          // Grouped view: show the newest event per correlation_id
          const latestByCorr = new Map();
          for (const r of filtered) {
            const cid = r.correlation_id || '';
            if (!cid) continue;
            const prev = latestByCorr.get(cid);
            if (!prev || (r.ts || '') > (prev.ts || '')) latestByCorr.set(cid, r);
          }
          const recs = Array.from(latestByCorr.values()).sort((a,b) => (b.ts||'').localeCompare(a.ts||'')).slice(0, 250);
          for (const r of recs) rowsEl.appendChild(renderCard(r, true));
        } else {
          // Feed view: newest first
          const recs = filtered.slice(-300).reverse();
          for (const r of recs) rowsEl.appendChild(renderCard(r, false));
        }
      }

      async function refresh() {
        try {
          const r = await fetch('/api/audit/recent?n=200', { cache: 'no-store' });
          if (!r.ok) throw new Error('audit_recent_http_' + r.status);
          const data = await r.json();
          buffer = data.records || [];
          // Update backend status based on the most recent poller events.
          for (const rec of buffer) maybeConsumePollerEvent(rec);
          // pick a stable “last seen” marker (most recent record)
          const last = buffer[buffer.length - 1];
          lastSeenKey = last ? ((last.correlation_id || '') + '::' + (last.event_type || '') + '::' + (last.ts || '')) : null;
          render();
          // Auto-select most recent event for details, but keep the bottom dock idle until the user selects a correlation.
          if (!selectedKey && last) selectRecord(last, { driveStage: false });
          updateLastOutcome();
          // Even if SSE is blocked, a successful refresh means the UI can talk to backend.
          if (statusEl.textContent === 'connecting…') {
            statusEl.textContent = 'api ok';
            statusEl.className = 'pill live';
          }
        } catch (e) {
          try {
            statusEl.textContent = 'api error';
            statusEl.className = 'pill down';
            agentLast.innerHTML = '<span class="mono">UI cannot fetch /api/audit/recent: ' + (e && e.message ? e.message : String(e)) + '</span>';
          } catch (_) {}
        }
      }

      document.getElementById('refreshBtn').onclick = refresh;
      clearViewBtn.onclick = () => {
        buffer = [];
        selectedKey = null;
        activeCorrId = '';
        preEl.textContent = '{}';
        selHint.textContent = 'select an event';
        selTs.textContent = '—';
        selCorr.textContent = '—';
        selType.textContent = '—';
        selSum.textContent = '—';
        selLinks.innerHTML = '—';
        render();
        setAgentUI({ corrId: '', stateTxt: 'idle', level: 'info', p: 0, spriteMode: 'think', speechTxt: 'standing by…', hasError: false });
        agentLast.innerHTML = '<span class="mono">Cleared UI view (audit log unchanged).</span>';
      };
      resetPollerBtn.onclick = async () => {
        try {
          resetPollerBtn.disabled = true;
          resetPollerBtn.textContent = 'Resetting…';
          const resp = await fetch('/api/poller/reset', { method: 'POST' });
          const j = await resp.json().catch(() => ({}));
          if (!resp.ok) {
            const err = (j && j.error) ? j.error : ('http_' + resp.status);
            agentLast.innerHTML = '<span class="mono">Poller reset failed: ' + err + '</span>';
          } else {
            agentLast.innerHTML = '<span class="mono">Poller reset. You can re-run your Airflow DAG now.</span>';
            // Reload audit buffer so the user sees poller.reset breadcrumb
            refresh().catch(() => {});
          }
        } catch(e) {
          agentLast.innerHTML = '<span class="mono">Poller reset failed (network error).</span>';
        } finally {
          resetPollerBtn.disabled = false;
          resetPollerBtn.textContent = 'Reset poller';
        }
      };
      corrEl.oninput = render;
      etypeEl.oninput = render;
      groupByEl.onchange = () => {
        groupToggle.className = 'toggle' + (groupByEl.checked ? ' on' : '');
        render();
      };
      groupToggle.className = 'toggle';

      // Hide heartbeat spam by default; still used for bottom stage animation.
      // (User can type "heartbeat" into event_type filter to inspect them.)
      showHeartbeats = false;

      // Initial load
      refresh().catch(() => {});

      // Live stream (SSE) with auto-reconnect + polling fallback.
      let es = null;
      let esBackoffMs = 800;
      let lastSseOpenMs = 0;
      let fallbackTimer = null;

      function startFallbackPolling() {
        if (fallbackTimer) return;
        fallbackTimer = setInterval(() => {
          // When SSE is unhealthy, poll for recent audit lines so the UI still moves.
          if (!lastSseOpenMs || (Date.now() - lastSseOpenMs) > 10_000) {
            refresh().catch(() => {});
          }
        }, 1500);
      }

      function connectSSE() {
        try {
          if (es) { try { es.close(); } catch (_) {} }
          es = new EventSource('/api/audit/stream');
          es.onopen = () => {
            lastSseOpenMs = Date.now();
            esBackoffMs = 800;
            statusEl.textContent = 'live';
            statusEl.className = 'pill live';
            // SSE is healthy; stop fallback polling to reduce noise.
            if (fallbackTimer) { try { clearInterval(fallbackTimer); } catch (_) {} fallbackTimer = null; }
          };
          es.onerror = () => {
            statusEl.textContent = 'reconnecting…';
            statusEl.className = 'pill down';
            startFallbackPolling();
            try { es.close(); } catch (_) {}
            es = null;
            const wait = Math.min(10_000, esBackoffMs);
            esBackoffMs = Math.min(10_000, Math.round(esBackoffMs * 1.6));
            setTimeout(connectSSE, wait);
          };
          es.onmessage = (ev) => {
            try {
              lastSseOpenMs = Date.now();
              const obj = JSON.parse(ev.data);
              maybeConsumePollerEvent(obj);
              // Game mode: animate agent characters as the pipeline progresses.
              try {
                if (GAME.on) {
                  if (!GAME.followCorr) GAME.followCorr = activeCorrId || (obj.correlation_id || '');
                  if ((obj.correlation_id || '') && (obj.correlation_id || '') === GAME.followCorr) {
                    gameCorr.textContent = 'correlation: ' + GAME.followCorr;
                    const et = (obj.event_type || '');
                    if (et === 'event.received') gameSetStage('event.received');
                    if (et === 'safety.prompt_injection_detected') gameSetStage('safety');
                    if (et === 'prompt.rca') gameSetStage('rca');
                    if (et === 'prompt.fix' || et === 'agent.patch_proposed') gameSetStage('fix');
                    if (et.startsWith('verify.') || et === 'verify.finished') gameSetStage('verify');
                    if (et === 'report.saved' || et === 'remediation.escalated') gameSetStage('report');
                    if (et === 'pr.created') gameSetStage('pr');
                  }
                }
              } catch(e) {}
              // Don’t spam the main feed with heartbeats; keep them for live stage updates.
              if ((obj.event_type || '') !== 'agent.heartbeat') {
                buffer.push(obj);
              }
              // Hard cap the in-memory buffer so long-running demos don't freeze the browser.
              // Render already uses slices, but deriveAgentStageForCorr/filtering scans the buffer.
              if (buffer.length > 2500) buffer = buffer.slice(-2500);
              lastSeenKey = (obj.correlation_id || '') + '::' + (obj.event_type || '') + '::' + (obj.ts || '');
              if ((obj.event_type || '') === 'pr.created') {
                confettiBurst();
              }
              render();
              // If the user hasn't selected anything yet, keep auto-selecting the newest event for details.
              if (!selectedKey && (obj.event_type || '') !== 'agent.heartbeat') selectRecord(obj, { driveStage: false });
              // If this event is for the currently active correlation, animate the stage live.
              if (activeCorrId && (obj.correlation_id || '') === activeCorrId) {
                const st = deriveAgentStageForCorr(activeCorrId);
                if (st) setAgentUI({ corrId: st.corrId, stateTxt: st.label, level: st.lvl, p: st.p, spriteMode: st.mode, speechTxt: st.label, hasError: st.hasErr });
              }
              updateLastOutcome();
            } catch(e) {}
          };
        } catch (e) {
          statusEl.textContent = 'reconnecting…';
          statusEl.className = 'pill down';
          startFallbackPolling();
          setTimeout(connectSSE, 1200);
        }
      }

      // Surface JS errors in-UI (helps when the page appears "stuck").
      try {
        window.addEventListener('error', (e) => {
          try { agentLast.innerHTML = '<span class="mono">UI error: ' + (e && e.message ? e.message : 'unknown') + '</span>'; } catch (_) {}
        });
        window.addEventListener('unhandledrejection', (e) => {
          try { agentLast.innerHTML = '<span class="mono">UI rejection: ' + (e && e.reason ? String(e.reason) : 'unknown') + '</span>'; } catch (_) {}
        });
      } catch (_) {}

      // IMPORTANT: Always start fallback polling immediately.
      // Some browsers/extensions can leave EventSource in a perpetual "connecting" state without firing onerror.
      // Polling guarantees the UI shows events and that runtime_config syncs even when SSE is blocked.
      startFallbackPolling();
      connectSSE();
      // Also do an immediate refresh so the page never boots "empty" even before the first poll tick.
      refresh().catch(() => {});
      // Poller status should not depend on poller.* events (which are hidden by default).
      setInterval(() => { refreshPollerStatus().catch(() => {}); }, 3000);
      refreshPollerStatus().catch(() => {});

      // Initialize dock in an “idle” state
      ensureStageBlocks();
      setAgentUI({ corrId: '', stateTxt: 'idle', level: 'info', p: 0, spriteMode: 'think', speechTxt: 'standing by…', hasError: false });
      updateLastOutcome();

      async function loadRuntimeConfig() {
        try {
          const r = await fetch('/api/runtime_config', { cache: 'no-store' });
          const j = await r.json();
          const warnings = (j && j.warnings && Array.isArray(j.warnings)) ? j.warnings : [];
          const avail = (j && j.available) ? j.available : {};
          const hasOpenrouter = !!avail.openrouter;
          const hasGroq = !!avail.groq;
          const hasCursor = !!avail.cursor;
          const hasInhouse = !!avail.inhouse;

          // If the persisted runtime config is "groq" but Groq is unavailable (missing key),
          // auto-switch back to OpenRouter once (prevents the "stuck on Groq" feeling).
          const cfgMode = (j && j.config && j.config.agent_mode) ? j.config.agent_mode : null;
          if (!didAutoFixProvider && cfgMode === 'groq' && !hasGroq && hasOpenrouter) {
            didAutoFixProvider = true;
            await setProvider('openrouter');
            return;
          }

          // Disable impossible selections (missing API keys).
          providerOpenrouter.disabled = !hasOpenrouter;
          providerGroq.disabled = !hasGroq;
          providerInhouse.disabled = false;
          providerOpenrouterToggle.style.opacity = hasOpenrouter ? '1.0' : '0.55';
          providerGroqToggle.style.opacity = hasGroq ? '1.0' : '0.55';
          providerInhouseToggle.style.opacity = hasInhouse ? '1.0' : '0.85';
          providerOpenrouterToggle.title = hasOpenrouter ? 'Use OpenRouter' : 'OpenRouter disabled: OPENROUTER_API_KEY missing';
          providerGroqToggle.title = hasGroq ? 'Use Groq' : 'Groq disabled: GROQ_API_KEY missing';
          providerInhouseToggle.title = hasInhouse ? 'Use Inhouse agent' : 'Inhouse: set endpoint in settings';
          useCursor.disabled = !hasCursor;
          useCursorToggle.style.opacity = hasCursor ? '1.0' : '0.55';
          useCursorToggle.title = hasCursor ? 'Use Cursor Cloud Agent for patch generation' : 'Cursor disabled: CURSOR_API_KEY + GitHub real mode required';

          // Prefer OpenRouter by default (if available) unless effective agent_mode says otherwise.
          const mode = (j && j.effective && j.effective.agent_mode) ? j.effective.agent_mode : (hasOpenrouter ? 'openrouter' : 'groq');
          useCursor.checked = (cfgMode === 'cursor') && hasCursor;

          // Radio provider reflects the non-cursor provider choice; when cursor is active, keep radios usable for when you uncheck it.
          const radioMode = (mode === 'openrouter' || mode === 'groq' || mode === 'inhouse') ? mode : (hasOpenrouter ? 'openrouter' : (hasGroq ? 'groq' : 'inhouse'));
          providerOpenrouter.checked = (radioMode === 'openrouter');
          providerGroq.checked = (radioMode === 'groq');
          providerInhouse.checked = (radioMode === 'inhouse');
          providerOpenrouterToggle.className = 'toggle' + (providerOpenrouter.checked ? ' on' : '');
          providerGroqToggle.className = 'toggle' + (providerGroq.checked ? ' on' : '');
          providerInhouseToggle.className = 'toggle' + (providerInhouse.checked ? ' on' : '');

          if (j && j.config) {
            if (typeof j.config.inhouse_agent_url === 'string') inhouseUrl.value = j.config.inhouse_agent_url;
            if (typeof j.config.airflow_base_url === 'string') airflowBaseUrl.value = j.config.airflow_base_url;
            if (typeof j.config.airflow_username === 'string') airflowUsername.value = j.config.airflow_username;
            if (typeof j.config.repo_root_override === 'string') repoRootOverride.value = j.config.repo_root_override;
            if (typeof j.config.local_sync_worktree_root === 'string') localWorktreeRoot.value = j.config.local_sync_worktree_root;
            if (typeof j.config.local_validate_cmd === 'string') localValidateCmd.value = j.config.local_validate_cmd;
            if (typeof j.config.airflow_poller_enabled === 'boolean') airflowPollerEnabled.checked = j.config.airflow_poller_enabled;
            const prTarget = (typeof j.config.pr_target === 'string') ? j.config.pr_target : 'github';
            prTargetGithub.checked = (prTarget === 'github');
            prTargetLocal.checked = (prTarget === 'local');
            if (typeof j.config.databricks_host === 'string') dbxHost.value = j.config.databricks_host;
            try {
              if (typeof j.config.repo_registry_json === 'string' && j.config.repo_registry_json.trim()) {
                const reg = JSON.parse(j.config.repo_registry_json);
                if (Array.isArray(reg)) {
                  const paths = [];
                  const prefixes = [];
                  reg.forEach((r) => {
                    if (r && typeof r.repo_root === 'string') paths.push(r.repo_root);
                    if (r && typeof r.pipeline_id_regex === 'string') {
                      const m = r.pipeline_id_regex.match(/^\^([A-Za-z0-9_.-]+)/);
                      prefixes.push(m ? m[1] : '');
                    } else {
                      prefixes.push('');
                    }
                  });
                  repoPaths.value = paths.join('\\n');
                  repoPrefixes.value = prefixes.join('\\n');
                }
              }
            } catch (_) {}
          }

          // Cursor PR toggle
          const cap = !!(j && j.config && j.config.cursor_auto_create_pr);
          cursorCreatesPr.checked = useCursor.checked ? cap : false;
          cursorCreatesPr.disabled = !useCursor.checked;
          cursorCreatesPrToggle.style.opacity = useCursor.checked ? '1.0' : '0.55';

          // Make provider fallbacks explicit in the UI (no silent confusion).
          // If runtime requested cursor/groq/openrouter but server is forced to use a different effective mode,
          // surface a one-line banner.
          try {
            if (warnings.length) {
              agentLast.innerHTML = '<span class="mono">⚠️ ' + warnings[0] + '</span>';
            }
          } catch (_) {}
        } catch(e) {}
      }

      async function refreshPollerStatus() {
        try {
          const r = await fetch('/api/poller/status', { cache: 'no-store' });
          if (!r.ok) return;
          const j = await r.json().catch(() => ({}));
          if (!j || !j.ok) return;
          const alive = !!j.alive;
          const enabled = !!j.enabled;
          const iv = (j.interval_s !== undefined && j.interval_s !== null) ? j.interval_s : '?';
          if (!enabled) {
            backendStatusEl.textContent = '⚪ poller: disabled';
          } else if (!alive) {
            backendStatusEl.textContent = '🔴 poller: down';
          } else {
            backendStatusEl.textContent = '🟢 poller: running (every ' + iv + 's)';
          }
        } catch (e) {
          // leave as-is
        }
      }

      async function setProvider(target) {
        try {
          const payload = { agent_mode: target };
          if (target === 'inhouse') {
            payload.inhouse_agent_url = (inhouseUrl.value || '').trim();
            payload.inhouse_api_key = (inhouseApiKey.value || '').trim();
            payload.inhouse_headers_json = (inhouseHeaders.value || '').trim();
          }
          const resp = await fetch('/api/runtime_config', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
          if (!resp.ok) {
            const j = await resp.json().catch(() => ({}));
            const err = (j && j.error) ? j.error : ('http_' + resp.status);
            agentLast.innerHTML = '<span class="mono">Provider switch failed: ' + err + '</span>';
            // Reload to revert UI to server truth
            loadRuntimeConfig();
          } else {
            loadRuntimeConfig();
          }
        } catch(e) {
          try {
            agentLast.innerHTML = '<span class="mono">Provider switch failed: ' + (e && e.message ? e.message : String(e)) + '</span>';
          } catch (_) {}
          loadRuntimeConfig();
        }
      }

      // If Cursor is enabled, provider radios are “armed” for when Cursor is disabled again.
      providerOpenrouter.onchange = async () => { if (!useCursor.checked && providerOpenrouter.checked) await setProvider('openrouter'); };
      providerGroq.onchange = async () => { if (!useCursor.checked && providerGroq.checked) await setProvider('groq'); };
      providerInhouse.onchange = async () => {
        if (!useCursor.checked && providerInhouse.checked) {
          inhousePanel.classList.remove('hidden');
          await setProvider('inhouse');
        }
      };

      useCursor.onchange = async () => {
        if (useCursor.checked) {
          await setProvider('cursor');
        } else {
          if (providerInhouse.checked) {
            await setProvider('inhouse');
          } else {
            await setProvider(providerOpenrouter.checked ? 'openrouter' : 'groq');
          }
        }
      };

      inhouseConfigBtn.onclick = () => {
        if (inhousePanel.classList.contains('hidden')) inhousePanel.classList.remove('hidden');
        else inhousePanel.classList.add('hidden');
      };
      inhouseCloseBtn.onclick = () => { inhousePanel.classList.add('hidden'); };
      inhouseSaveBtn.onclick = async () => {
        await setProvider('inhouse');
        inhousePanel.classList.add('hidden');
      };

      platformsBtn.onclick = () => {
        // Show a convenient ingest URL for external systems to post failures to PATCHIT.
        try {
          const base = window.location.origin.replace(/\\/ui\\s*$/, '');
          patchitIngestUrl.value = base.replace(/\\/$/, '') + '/events/ingest';
        } catch (_) {
          patchitIngestUrl.value = '/events/ingest';
        }
        if (platformsPanel.classList.contains('hidden')) platformsPanel.classList.remove('hidden');
        else platformsPanel.classList.add('hidden');
      };
      platformsCloseBtn.onclick = () => { platformsPanel.classList.add('hidden'); };
      dbxSaveBtn.onclick = async () => {
        try {
          const resp = await fetch('/api/runtime_config', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ databricks_host: (dbxHost.value || '').trim(), databricks_token: (dbxToken.value || '').trim() }),
          });
          if (!resp.ok) {
            const j = await resp.json().catch(() => ({}));
            agentLast.innerHTML = '<span class="mono">Databricks save failed: ' + (j.error || ('http_' + resp.status)) + '</span>';
          } else {
            agentLast.innerHTML = '<span class="mono">Databricks config saved.</span>';
          }
        } catch (_) {
          agentLast.innerHTML = '<span class="mono">Databricks save failed.</span>';
        }
      };
      dbxTestBtn.onclick = async () => {
        try {
          const resp = await fetch('/api/databricks/test', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ databricks_host: (dbxHost.value || '').trim(), databricks_token: (dbxToken.value || '').trim() }),
          });
          const j = await resp.json().catch(() => ({}));
          agentLast.innerHTML = '<span class="mono">Databricks test: ' + (j.ok ? 'ok' : ('failed: ' + (j.error || 'unknown'))) + '</span>';
        } catch (_) {
          agentLast.innerHTML = '<span class="mono">Databricks test failed.</span>';
        }
      };

      async function saveConnectorConfig({ restartPoller }) {
        const prTarget = prTargetLocal.checked ? 'local' : 'github';
        const paths = (repoPaths.value || '').split('\\n').map(s => s.trim()).filter(Boolean);
        const prefixes = (repoPrefixes.value || '').split('\\n').map(s => s.trim());
        let repoRegistryJson = '';
        if (paths.length) {
          const regs = paths.map((p, idx) => {
            const key = p.split('/').filter(Boolean).slice(-1)[0] || ('repo' + idx);
            const pref = prefixes[idx] || '';
            return {
              key,
              pipeline_id_regex: pref ? ('^' + pref) : null,
              repo_root: p,
              local_sync_repo_path: p,
            };
          });
          repoRegistryJson = JSON.stringify(regs);
        }
        const payload = {
          airflow_base_url: (airflowBaseUrl.value || '').trim(),
          airflow_username: (airflowUsername.value || '').trim(),
          airflow_password: (airflowPassword.value || '').trim(),
          airflow_poller_enabled: !!airflowPollerEnabled.checked,
          repo_root_override: (repoRootOverride.value || '').trim(),
          repo_registry_json: repoRegistryJson,
          pr_target: prTarget,
          local_sync_enabled: (prTarget === 'local'),
          local_sync_repo_path: (repoRootOverride.value || '').trim(),
          local_sync_worktree_root: (localWorktreeRoot.value || '').trim(),
          local_validate_cmd: (localValidateCmd.value || '').trim(),
        };
        try {
          const resp = await fetch('/api/runtime_config', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
          if (!resp.ok) {
            const j = await resp.json().catch(() => ({}));
            const err = (j && j.error) ? j.error : ('http_' + resp.status);
            agentLast.innerHTML = '<span class="mono">Connector save failed: ' + err + '</span>';
          }
        } catch (e) {
          agentLast.innerHTML = '<span class="mono">Connector save failed</span>';
        }
        if (restartPoller) {
          try {
            await fetch('/api/poller/reset', { method: 'POST' });
          } catch (_) {}
        }
        loadRuntimeConfig();
      }

      connectorBtn.onclick = () => {
        if (connectorPanel.classList.contains('hidden')) connectorPanel.classList.remove('hidden');
        else connectorPanel.classList.add('hidden');
      };
      connectorCloseBtn.onclick = () => { connectorPanel.classList.add('hidden'); };
      connectorSaveBtn.onclick = async () => { await saveConnectorConfig({ restartPoller: false }); };
      connectorSaveRestartBtn.onclick = async () => { await saveConnectorConfig({ restartPoller: true }); };
      connectorTestBtn.onclick = async () => {
        const paths = (repoPaths.value || '').split('\\n').map(s => s.trim()).filter(Boolean);
        const repoPath = (repoRootOverride.value || '').trim() || (paths[0] || '');
        try {
          const resp = await fetch('/api/airflow/test', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({
              airflow_base_url: (airflowBaseUrl.value || '').trim(),
              airflow_username: (airflowUsername.value || '').trim(),
              airflow_password: (airflowPassword.value || '').trim(),
            }),
          });
          const j = await resp.json().catch(() => ({}));
          agentLast.innerHTML = '<span class="mono">Airflow test: ' + (j.ok ? 'ok' : ('failed: ' + (j.error || 'unknown'))) + '</span>';
        } catch (e) {
          agentLast.innerHTML = '<span class="mono">Airflow test failed</span>';
        }
        if (repoPath) {
          try {
            const resp = await fetch('/api/repo/test', {
              method: 'POST',
              headers: {'Content-Type':'application/json'},
              body: JSON.stringify({ repo_path: repoPath }),
            });
            const j = await resp.json().catch(() => ({}));
            if (j && j.ok) {
              agentLast.innerHTML = '<span class="mono">Repo test: ok (' + (j.file_count || 0) + ' files)</span>';
            } else {
              agentLast.innerHTML = '<span class="mono">Repo test failed: ' + (j.error || 'unknown') + '</span>';
            }
          } catch (e) {
            agentLast.innerHTML = '<span class="mono">Repo test failed</span>';
          }
        }
      };
      inhouseTestBtn.onclick = async () => {
        try {
          const resp = await fetch('/api/inhouse/test', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({
              inhouse_agent_url: (inhouseUrl.value || '').trim(),
              inhouse_api_key: (inhouseApiKey.value || '').trim(),
              inhouse_headers_json: (inhouseHeaders.value || '').trim(),
            }),
          });
          const j = await resp.json().catch(() => ({}));
          if (j && j.ok) {
            agentLast.innerHTML = '<span class="mono">Inhouse test: ok</span>';
          } else {
            agentLast.innerHTML = '<span class="mono">Inhouse test failed: ' + (j.error || 'unknown') + '</span>';
          }
        } catch (e) {
          agentLast.innerHTML = '<span class="mono">Inhouse test failed</span>';
        }
      };

      cursorCreatesPr.onchange = async () => {
        try {
          const resp = await fetch('/api/runtime_config', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({cursor_auto_create_pr: !!cursorCreatesPr.checked}) });
          if (!resp.ok) {
            const j = await resp.json().catch(() => ({}));
            const err = (j && j.error) ? j.error : ('http_' + resp.status);
            agentLast.innerHTML = '<span class="mono">Cursor PR toggle failed: ' + err + '</span>';
          }
        } catch (e) {
          agentLast.innerHTML = '<span class="mono">Cursor PR toggle failed</span>';
        }
        loadRuntimeConfig();
      };
      loadRuntimeConfig();

      // Keep the bottom stage feeling alive even when no new events arrive:
      // show elapsed time since last event for the active correlation.
      function fmtElapsed(ms){
        const s = Math.max(0, Math.floor(ms/1000));
        const m = Math.floor(s/60);
        const r = s%60;
        return (m>0 ? (m+'m ') : '') + r + 's';
      }
      // Throttle: avoid “updating every second” noise.
      setInterval(() => {
        if (!activeCorrId) return;
        const st = deriveAgentStageForCorr(activeCorrId);
        if (!st) return;
        if (st.isTerminal) return; // Stop the “elapsed” timer once we reach a terminal outcome.
        const ts = st.lastTs ? Date.parse(st.lastTs) : null;
        if (!ts || Number.isNaN(ts)) return;
        const age = Date.now() - ts;
        const extra = (age > 4000) ? (' • elapsed ' + fmtElapsed(age)) : '';
        setAgentUI({
          corrId: st.corrId,
          stateTxt: st.label,
          level: st.lvl,
          p: st.p,
          spriteMode: st.mode,
          speechTxt: st.label + extra,
          hasError: st.hasErr,
        });
      }, 2500);
    </script>
  </body>
</html>"""


