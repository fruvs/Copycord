# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations
from collections import deque
import contextlib
import json
import os
import uuid
import asyncio
import websockets
from pathlib import Path
import unicodedata
import tarfile, tempfile, shutil
import re
import time
import logging
from typing import Dict, List, Set, Literal, Optional
from admin.logging_setup import (
    LOGGER,
    get_logger,
    configure_app_logging,
    req_id_var,
    route_var,
    client_var,
    REDACT_KEYS,
)
from fastapi import (
    FastAPI,
    Request,
    WebSocket,
    WebSocketDisconnect,
    Body,
    status,
    HTTPException,
    File,
    UploadFile,
    Form,
)
from anyio import EndOfStream
from fastapi.responses import (
    RedirectResponse,
    PlainTextResponse,
    StreamingResponse,
    JSONResponse,
    FileResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.types import Scope, Receive, Send
from starlette.datastructures import MutableHeaders
from starlette.middleware.base import BaseHTTPMiddleware
from common.config import CURRENT_VERSION
from common.constants import PROFILE_BOOL_KEYS, PROFILE_TEXT_KEYS
from common.db import DBManager
from common.backup_scheduler import BackupConfig, DailySQLiteBackupScheduler
from admin.web_config import router as links_router
from admin.web_config import startup_links, shutdown_links
from contextlib import suppress
from time import perf_counter
import sys as _sys, json as _json, contextvars
from datetime import datetime
import aiohttp


GITHUB_REPO = os.getenv("GITHUB_REPO", "Copycord/Copycord")
RELEASE_POLL_SECONDS = int(os.getenv("RELEASE_POLL_SECONDS", "1800"))


def _set_ws_context(route: str, ws: WebSocket):
    route_var.set(route)

    c = getattr(ws, "client", None)
    if c:
        client_var.set(f"{getattr(c, 'host', '?')}:{getattr(c, 'port', '?')}")
    else:
        client_var.set("-")

    req_id_var.set(uuid.uuid4().hex[:8])


def _redact_dict(d: dict) -> dict:
    try:
        rd = dict(d or {})
        for k in REDACT_KEYS:
            if k in rd and rd[k]:
                rd[k] = "***REDACTED***"
        return rd
    except Exception:
        return {"_redact_error": True}


class _Timer:
    def __init__(self, label: str):
        self.label = label
        self._t0 = None
        self.ms = 0.0

    def __enter__(self):
        self._t0 = perf_counter()
        return self

    def __exit__(self, *exc):
        self.ms = (perf_counter() - self._t0) * 1000.0


def _safe(x):
    try:
        s = str(x)
        return (s[:500] + "…") if len(s) > 500 else s
    except Exception:
        return "<unprintable>"


APP_TITLE = "Copycord"


DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = os.getenv("DB_PATH", "/data/data.db")
db = DBManager(DB_PATH)

BACKUP_DIR = Path(os.getenv("BACKUP_DIR", str(DATA_DIR / "backups")))
BACKUP_RETAIN = int(os.getenv("BACKUP_RETAIN", "14"))
BACKUP_AT = os.getenv("BACKUP_AT", "03:17")
BACKUP_TZ = os.getenv("TZ", "UTC")

_backup_cfg = BackupConfig(
    db_path=DB_PATH,
    backup_dir=BACKUP_DIR,
    retain=BACKUP_RETAIN,
    run_at=BACKUP_AT,
    timezone=BACKUP_TZ,
)


async def _record_backup_stats(archive_path: Path):
    try:
        size = archive_path.stat().st_size if archive_path.exists() else 0
        now_iso = datetime.utcnow().isoformat() + "Z"
        db.set_config("DB_LAST_BACKUP_AT", now_iso)
        db.set_config("DB_LAST_BACKUP_FILE", archive_path.name)
        db.set_config("DB_LAST_BACKUP_SIZE", str(size))
    except Exception as e:
        LOGGER.exception("Failed writing backup stats: %s", e)


backup_scheduler = DailySQLiteBackupScheduler(
    cfg=_backup_cfg,
    logger=LOGGER,
    on_complete=_record_backup_stats,
)


SERVER_CTRL_URL = os.getenv("WS_SERVER_CTRL_URL", "ws://server:9101")
CLIENT_CTRL_URL = os.getenv("WS_CLIENT_CTRL_URL", "ws://client:9102")

CLIENT_AGENT_URL = os.getenv("WS_CLIENT_URL", "ws://client:8766")
SERVER_AGENT_URL = os.getenv("WS_SERVER_URL", "ws://server:8765")


ALLOWED_ENV = [
    "SERVER_TOKEN",
    "CLONE_GUILD_ID",
    "COMMAND_USERS",
    "DELETE_CHANNELS",
    "DELETE_MESSAGES",
    "EDIT_MESSAGES",
    "DELETE_THREADS",
    "DELETE_ROLES",
    "CLONE_EMOJI",
    "CLONE_STICKER",
    "CLONE_ROLES",
    "MIRROR_ROLE_PERMISSIONS",
    "CLIENT_TOKEN",
    "HOST_GUILD_ID",
    "ENABLE_CLONING",
    "LOG_LEVEL",
    "LOG_FORMAT",
    "MIRROR_CHANNEL_PERMISSIONS",
]
REQUIRED = ["SERVER_TOKEN", "CLIENT_TOKEN", "CLONE_GUILD_ID"]
BOOL_KEYS = list(PROFILE_BOOL_KEYS)
DEFAULTS: Dict[str, str] = {
    "DELETE_CHANNELS": "True",
    "DELETE_THREADS": "True",
    "DELETE_ROLES": "True",
    "EDIT_MESSAGES": "True",
    "DELETE_MESSAGES": "True",
    "CLONE_EMOJI": "True",
    "CLONE_STICKER": "True",
    "CLONE_ROLES": "True",
    "MIRROR_ROLE_PERMISSIONS": "False",
    "ENABLE_CLONING": "True",
    "LOG_LEVEL": "INFO",
    "LOG_FORMAT": "HUMAN",
    "COMMAND_USERS": "",
    "MIRROR_CHANNEL_PERMISSIONS": "False",
}


app = FastAPI(title=APP_TITLE)
BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.globals.setdefault("links", {})
app.include_router(links_router)
shutdown_event = asyncio.Event()


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:8]
        token_r = req_id_var.set(rid)
        token_s = route_var.set(request.url.path or "-")
        token_c = client_var.set(
            f"{getattr(request.client, 'host', '?')}:{getattr(request.client, 'port', '?')}"
        )

        response = None
        try:
            try:
                response = await call_next(request)
            except EndOfStream:

                LOGGER.info(
                    "Client disconnected during request body read",
                )

                return PlainTextResponse("client disconnected", status_code=499)
            except asyncio.CancelledError:

                LOGGER.debug("Request task cancelled (client gone)")
                return PlainTextResponse("client disconnected", status_code=499)
        finally:

            if response is not None:
                try:
                    response.headers["X-Request-ID"] = rid
                except Exception:
                    pass

            req_id_var.reset(token_r)
            route_var.reset(token_s)
            client_var.reset(token_c)

        return response


app.add_middleware(RequestContextMiddleware)


class ConnCloseOnShutdownASGI:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        async def send_wrapper(message):
            if message["type"] == "http.response.start" and shutdown_event.is_set():
                headers = MutableHeaders(raw=message.setdefault("headers", []))
                headers["Connection"] = "close"
                LOGGER.debug(
                    "ConnCloseOnShutdownASGI | injected Connection: close for path=%s",
                    scope.get("path"),
                )
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except asyncio.CancelledError:
            LOGGER.debug(
                "ConnCloseOnShutdownASGI | request cancelled path=%s", scope.get("path")
            )
            return
        except Exception:
            LOGGER.exception(
                "ASGI pipeline error at path=%s method=%s",
                scope.get("path"),
                scope.get("method"),
            )
            raise


class BusHub:
    def __init__(self):
        self.status = {"server": {}, "client": {}}
        self.subscribers: Set[asyncio.Queue[str]] = set()
        self.ui_sockets: Set[WebSocket] = set()
        self.lock = asyncio.Lock()
        self.recent = deque(maxlen=200)

    def subscribe(self) -> asyncio.Queue[str]:
        q = asyncio.Queue(maxsize=200)
        self.subscribers.add(q)
        LOGGER.debug("BusHub.subscribe | subscribers=%d", len(self.subscribers))
        return q

    async def remove_ui(self, ws: WebSocket):
        async with self.lock:
            self.ui_sockets.discard(ws)

    def unsubscribe(self, q: asyncio.Queue[str]):
        self.subscribers.discard(q)
        LOGGER.debug("BusHub.unsubscribe | subscribers=%d", len(self.subscribers))

    def _mkmsg(self, kind, role, payload=None):
        return json.dumps({"kind": kind, "role": role, "payload": payload or {}})

    def _normalize(self, obj: dict) -> dict:
        if not isinstance(obj, dict):
            return {"kind": "log", "role": "unknown", "payload": {"raw": _safe(obj)}}
        kind = obj.get("kind") or obj.get("type") or "event"
        role = obj.get("role") or "unknown"
        payload = obj.get("payload")
        if payload is None:
            payload = {k: v for k, v in obj.items() if k not in ("kind", "role")}
        return {"kind": kind, "role": role, "payload": payload or {}}

    async def publish(self, kind: str, role: str, payload: dict):
        if kind == "status" and role in ("server", "client"):
            self.status[role] = payload or {}

        rec = {"kind": kind, "role": role, "payload": payload or {}}
        self.recent.append(rec)

        text = json.dumps(rec, separators=(",", ":"))

        dead_q = []
        for q in list(self.subscribers):
            try:
                q.put_nowait(text)
            except asyncio.QueueFull:
                dead_q.append(q)
        for q in dead_q:
            self.subscribers.discard(q)

        await self._broadcast_text(text)

        LOGGER.debug(
            "BusHub.publish | kind=%s role=%s sse=%d ui=%d recent=%d",
            kind,
            role,
            len(self.subscribers),
            len(self.ui_sockets),
            len(self.recent),
        )

    async def add_ui(self, ws: WebSocket):
        async with self.lock:
            self.ui_sockets.add(ws)
        LOGGER.debug("BusHub.add_ui | ui_sockets=%d", len(self.ui_sockets))
        for role, payload in self.status.items():
            if payload:
                await ws.send_text(self._mkmsg("status", role, payload))
        for m in list(self.recent)[-20:]:
            try:
                rec = self._normalize(m)
                await ws.send_text(json.dumps(rec, separators=(",", ":")))
            except Exception as e:
                LOGGER.debug("BusHub.add_ui replay failed: %s", repr(e))

    async def broadcast(self, obj: dict):
        rec = self._normalize(obj)
        self.recent.append(rec)
        text = json.dumps(rec, separators=(",", ":"))

        dead_q = []
        for q in list(self.subscribers):
            try:
                q.put_nowait(text)
            except asyncio.QueueFull:
                dead_q.append(q)
        for q in dead_q:
            self.subscribers.discard(q)

        await self._broadcast_text(text)
        LOGGER.debug(
            "BusHub.broadcast | kind=%s role=%s ui_sockets=%d",
            rec.get("kind"),
            rec.get("role"),
            len(self.ui_sockets),
        )

    async def _broadcast_text(self, text: str):
        dead = []
        async with self.lock:
            for ws in list(self.ui_sockets):
                try:
                    await ws.send_text(text)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                with suppress(Exception):
                    await ws.close()
                self.ui_sockets.discard(ws)
        if dead:
            LOGGER.debug(
                "BusHub._broadcast_text | cleaned_dead=%d remaining=%d",
                len(dead),
                len(self.ui_sockets),
            )


hub = BusHub()
agent_sockets: Set[WebSocket] = set()
bus_sockets: Set[WebSocket] = set()


class BackfillLocks:
    def __init__(self, ttl_launching_sec: float = 20.0):
        self._launching_ttl = ttl_launching_sec
        self._launching: Dict[int, float] = {}
        self._running: set[int] = set()
        self._lock = asyncio.Lock()

    async def clear_all(self):
        async with self._lock:
            self._launching.clear()
            self._running.clear()

    async def try_acquire_launching(self, channel_id: int) -> bool:
        now = time.time()
        async with self._lock:
            self._launching = {
                cid: exp for cid, exp in self._launching.items() if exp > now
            }
            if channel_id in self._running or channel_id in self._launching:
                return False
            self._launching[channel_id] = now + self._launching_ttl
            return True

    async def promote_to_running(self, channel_id: int):
        async with self._lock:
            self._launching.pop(channel_id, None)
            self._running.add(channel_id)

    async def release(self, channel_id: int):
        async with self._lock:
            self._launching.pop(channel_id, None)
            self._running.discard(channel_id)

    async def status(self, channel_id: int) -> Literal["idle", "launching", "running"]:
        now = time.time()
        async with self._lock:
            if channel_id in self._running:
                return "running"
            if self._launching.get(channel_id, 0) > now:
                return "launching"
            return "idle"


locks = BackfillLocks()


async def _lock_listener():
    q = hub.subscribe()
    while True:
        raw = await q.get()
        try:
            ev = json.loads(raw)
        except Exception:
            continue
        if ev.get("kind") != "client":
            continue
        p = ev.get("payload") or {}
        t = p.get("type")
        d = p.get("data") or {}
        cid = d.get("channel_id") or p.get("channel_id")
        try:
            cid = int(cid)
        except Exception:
            continue
        if t in ("backfill_ack",):
            await locks.promote_to_running(cid)
        elif t in ("backfill_done",):
            await locks.release(cid)
        elif t in ("backfill_busy",):
            await locks.promote_to_running(cid)
        elif t in ("backfill_stream_end",):

            pass


async def _close_ws_quietly(
    ws: WebSocket, code: int = 1001, reason: str = "server shutdown"
):
    with contextlib.suppress(RuntimeError, WebSocketDisconnect, Exception):
        await ws.close(code=code, reason=reason)


@app.websocket("/bus")
async def admin_bus(ws: WebSocket):
    await ws.accept()
    _set_ws_context("/bus", ws)
    bus_sockets.add(ws)
    socket_id = id(ws)
    local_log = get_logger("copycord.ws.bus", socket_id=socket_id)
    route_var.set("/bus")
    client_var.set("-")

    local_log.info("WS connected | peers=%d", len(bus_sockets))
    count = 0
    try:
        while True:
            raw = await ws.receive_text()
            local_log.debug("Recv | raw=%s", raw[:300])
            count += 1
            try:
                ev = json.loads(raw)
                local_log.debug(
                    "Parsed | kind=%s role=%s keys=%s",
                    ev.get("kind"),
                    ev.get("role"),
                    list(ev.keys()),
                )
            except Exception:
                ev = {"kind": "log", "role": "unknown", "payload": {"raw": raw}}
                local_log.warning("JSON parse failed | raw=%s", raw[:200])
            if not isinstance(ev, dict):
                ev = {"kind": "log", "role": "unknown", "payload": {"raw": _safe(ev)}}
            kind = ev.get("kind") or "log"
            role = ev.get("role") or "unknown"
            payload = ev.get("payload") or {}
            await hub.publish(kind, role, payload)
            if count % 50 == 0:
                local_log.debug("Forwarded=%d", count)
    except WebSocketDisconnect:
        local_log.info("WS disconnected | forwarded=%d", count)
    finally:
        bus_sockets.discard(ws)


@app.get("/bus/stream")
async def bus_stream(request: Request):

    client = request.client
    client_addr = f"{getattr(client, 'host', '?')}:{getattr(client, 'port', '?')}"
    conn_id = uuid.uuid4().hex[:8]

    local_log = get_logger("copycord.sse", conn_id=conn_id)
    route_var.set("/bus/stream")
    client_var.set(client_addr)

    local_log.info("Client connected")

    async def gen():
        q = hub.subscribe()
        events_sent = 0
        heartbeats_sent = 0

        def _summarize(msg: str) -> str:
            try:
                obj = json.loads(msg)
                kind = obj.get("kind") or obj.get("type") or "?"
                role = obj.get("role") or obj.get("source") or "-"
                return f"kind={kind} role={role} len={len(msg)}"
            except Exception:
                return f"kind=? (non-json) len={len(msg)}"

        try:

            initial = 0
            for role, payload in hub.status.items():
                if payload:
                    data = json.dumps(
                        {"kind": "status", "role": role, "payload": payload}
                    )
                    yield f"data: {data}\n\n"
                    initial += 1
                    events_sent += 1
            local_log.debug("Initial status flush | entries=%d", initial)

            while not shutdown_event.is_set():
                if await request.is_disconnected():
                    local_log.info(
                        "Client disconnected",
                        extra={
                            "events_sent": events_sent,
                            "heartbeats": heartbeats_sent,
                        },
                    )
                    return
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=1.0)
                    local_log.debug(
                        "Yield event | %s | qsize=%d", _summarize(msg), q.qsize()
                    )
                    yield f"data: {msg}\n\n"
                    events_sent += 1
                except asyncio.TimeoutError:
                    yield ":ka\n\n"
                    heartbeats_sent += 1
                    if heartbeats_sent % 60 == 0:
                        local_log.debug(
                            "Heartbeat checkpoint",
                            extra={"heartbeats": heartbeats_sent},
                        )
        except asyncio.CancelledError:
            local_log.debug(
                "Closed by client",
                extra={"events_sent": events_sent, "heartbeats": heartbeats_sent},
            )
            return
        finally:
            hub.unsubscribe(q)
            local_log.info(
                "Closed",
                extra={"events_sent": events_sent, "heartbeats": heartbeats_sent},
            )

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.websocket("/ws/ui")
async def ws_ui(ws: WebSocket):
    await ws.accept()
    _set_ws_context("/ws/ui", ws)
    hub.ui_sockets.add(ws)
    socket_id = id(ws)
    local_log = get_logger("copycord.ws.ui", socket_id=socket_id)
    route_var.set("/ws/ui")
    client_var.set("-")

    local_log.info("Connected | ui_sockets=%d", len(hub.ui_sockets))

    backlog = list(hub.recent)[-20:]
    for m in backlog:
        await ws.send_text(json.dumps(m))
    local_log.debug("Sent backlog | count=%d", len(backlog))

    try:
        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(ws.receive_text(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
    except WebSocketDisconnect:
        local_log.info("Disconnected")
    except asyncio.CancelledError:
        local_log.debug("Cancelled")
        return
    finally:
        hub.ui_sockets.discard(ws)
        local_log.debug("Removed | ui_sockets=%d", len(hub.ui_sockets))


@app.websocket("/ws/out")
async def ws_out(websocket: WebSocket):
    await websocket.accept()
    _set_ws_context("/ws/out", websocket)
    await hub.add_ui(websocket)

    socket_id = id(websocket)
    local_log = get_logger("copycord.ws.out", socket_id=socket_id)
    route_var.set("/ws/out")
    client_var.set("-")

    local_log.info("Client connected", extra={"events_sent": 0})
    local_log.debug("WebSocket attached to hub")

    try:
        while not shutdown_event.is_set():
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        local_log.info("Client disconnected", extra={"events_sent": 0})
    except asyncio.CancelledError:
        local_log.debug("Connection cancelled (client closed)")
        return
    finally:
        await hub.remove_ui(websocket)
        local_log.debug("Cleanup complete | active_ui_sockets=%d", len(hub.ui_sockets))


@app.websocket("/ws/in")
async def ws_in(websocket: WebSocket):
    await websocket.accept()
    _set_ws_context("/ws/in", websocket)
    agent_sockets.add(websocket)

    socket_id = id(websocket)
    local_log = get_logger("copycord.ws.in", socket_id=socket_id)
    route_var.set("/ws/in")
    client_var.set("-")

    local_log.info("Agent connected", extra={"forwarded": 0})

    forwarded = 0
    try:
        while not shutdown_event.is_set():
            try:
                ev = await asyncio.wait_for(websocket.receive(), timeout=1.0)
                local_log.debug(
                    "Event received | type=%s keys=%s", ev.get("type"), list(ev.keys())
                )
            except asyncio.TimeoutError:
                continue

            typ = ev.get("type")
            if typ == "websocket.disconnect":
                local_log.debug("Disconnect signal received")
                break
            if typ != "websocket.receive":
                local_log.debug("Ignored non-receive event | type=%s", typ)
                continue

            raw = ev.get("text")
            if raw:
                local_log.debug(
                    "Raw text received | length=%d | preview=%s", len(raw), raw[:200]
                )
            else:
                raw_bytes = ev.get("bytes") or []
                local_log.debug("Raw bytes received | length=%d", len(raw_bytes))
                if raw_bytes:
                    try:
                        raw = raw_bytes.decode("utf-8", "ignore")
                    except Exception as e:
                        local_log.warning("Failed to decode raw bytes | error=%s", e)
                        continue

            try:
                msg = json.loads(raw)
                local_log.debug("JSON parsed successfully | keys=%s", list(msg.keys()))
            except Exception:
                msg = {"type": "raw", "data": raw}
                local_log.warning("JSON parse failed | raw_preview=%s", raw[:200])

            if isinstance(msg, dict) and ("kind" in msg or "payload" in msg):
                await hub.publish(
                    kind=msg.get("kind") or msg.get("type") or "event",
                    role=msg.get("role") or "ui",
                    payload=msg.get("payload") or {},
                )
                forwarded += 1
                local_log.debug(
                    "Published message to hub", extra={"forwarded": forwarded}
                )
                with contextlib.suppress(Exception):
                    await websocket.send_text('{"ok":true}')
                continue

            out = {
                "kind": "agent",
                "role": msg.get("role") or "unknown",
                "type": msg.get("type") or "event",
                "ts": msg.get("ts"),
                "data": msg.get("data", {}),
            }

            await hub.broadcast(out)
            forwarded += 1
            local_log.debug("Broadcast message", extra={"forwarded": forwarded})
            if forwarded % 100 == 0:
                local_log.info(
                    "Forwarding checkpoint reached", extra={"forwarded": forwarded}
                )

            with contextlib.suppress(Exception):
                await websocket.send_text('{"ok":true}')

    except WebSocketDisconnect:
        local_log.info("Agent disconnected", extra={"forwarded": forwarded})
    except asyncio.CancelledError:
        local_log.debug(
            "Connection cancelled (client closed)", extra={"forwarded": forwarded}
        )
        return
    finally:
        agent_sockets.discard(websocket)
        with contextlib.suppress(Exception):
            await websocket.close()
        local_log.debug("Cleanup complete | active_agents=%d", len(agent_sockets))


async def _ws_cmd(url: str, payload: dict, timeout: float = 0.7) -> dict:
    with _Timer(f"_ws_cmd {url}") as t:
        try:
            async with asyncio.timeout(timeout):
                async with websockets.connect(
                    url,
                    open_timeout=timeout,
                    close_timeout=0.1,
                    ping_interval=None,
                ) as ws:
                    await ws.send(json.dumps(payload))
                    msg = await ws.recv()
                    if isinstance(msg, (bytes, str)):
                        res = json.loads(msg)
                        LOGGER.debug(
                            "_ws_cmd ok | url=%s took_ms=%.1f payload=%s -> %s",
                            url,
                            t.ms,
                            _safe(payload),
                            _safe(res),
                            extra={"took_ms": round(t.ms, 1)},
                        )
                        return res
                    LOGGER.debug(
                        "_ws_cmd bad-response | url=%s took_ms=%.1f",
                        url,
                        t.ms,
                        extra={"took_ms": round(t.ms, 1)},
                    )
                    return {"ok": False, "running": False, "error": "bad-response"}
        except Exception as e:
            LOGGER.debug(
                "_ws_cmd error | url=%s took_ms=%.1f err=%s",
                url,
                t.ms,
                repr(e),
                extra={"took_ms": round(t.ms, 1)},
            )
            return {"ok": False, "running": False, "error": str(e)}


@app.get("/", response_class=None)
async def index(request: Request):
    env = _read_env()
    s_server = await _ws_cmd(SERVER_CTRL_URL, {"cmd": "status"})
    s_client = await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "status"})

    both_running = bool(s_server.get("running")) and bool(s_client.get("running"))

    text_keys = [
        "SERVER_TOKEN",
        "CLIENT_TOKEN",
        "HOST_GUILD_ID",
        "CLONE_GUILD_ID",
        "COMMAND_USERS",
    ]
    bool_keys = BOOL_KEYS
    log_level = env.get("LOG_LEVEL", "INFO")
    LOGGER.debug(
        "GET / | both_running=%s server=%s client=%s",
        both_running,
        s_server.get("status"),
        s_client.get("status"),
    )

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": APP_TITLE,
            "env": env,
            "text_keys": text_keys,
            "bool_keys": bool_keys,
            "log_level": log_level,
            "server_status": s_server,
            "client_status": s_client,
            "both_running": both_running,
            "version": CURRENT_VERSION,
        },
    )


@app.get("/health", response_class=PlainTextResponse)
async def health():
    s1 = await _ws_cmd(SERVER_CTRL_URL, {"cmd": "status"})
    s2 = await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "status"})
    ok = s1.get("ok", True) and s2.get("ok", True)
    LOGGER.info(
        "Health check | server=%s client=%s ok=%s",
        s1.get("status"),
        s2.get("status"),
        ok,
    )
    return "ok" if ok else PlainTextResponse("control not reachable", status_code=500)


@app.post("/save")
async def save(request: Request):
    form = await request.form()
    values = {k: str(form.get(k, "")).strip() for k in ALLOWED_ENV}
    LOGGER.debug("POST /save | form=%s", _redact_dict(values))
    errs = _validate(values)
    if errs:
        LOGGER.warning("POST /save invalid | errs=%s", errs)
        return PlainTextResponse("Invalid config: " + "; ".join(errs), status_code=400)
    _write_env(values)
    return RedirectResponse("/", status_code=303)


@app.post("/start")
async def start_all():
    errs = _validate(_read_env())
    if errs:
        LOGGER.warning("POST /start blocked | errs=%s", errs)
        return PlainTextResponse("Cannot start: " + "; ".join(errs), status_code=400)
    srv = await _ws_cmd(SERVER_CTRL_URL, {"cmd": "start"})
    cli = await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "start"})
    if not srv.get("ok") or srv.get("error") or not cli.get("ok") or cli.get("error"):
        detail = f"server={srv.get('error') or srv.get('status')}, client={cli.get('error') or cli.get('status')}"
        LOGGER.error("POST /start failed | %s", detail)
        return PlainTextResponse(f"Start failed: {detail}", status_code=502)
    LOGGER.info("POST /start ok")
    return RedirectResponse("/", status_code=303)


@app.post("/stop")
async def stop_all():
    LOGGER.info("POST /stop requested")
    await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "stop"})
    await _ws_cmd(SERVER_CTRL_URL, {"cmd": "stop"})

    await locks.clear_all()

    return RedirectResponse("/", status_code=303)


@app.on_event("shutdown")
async def on_shutdown():
    LOGGER.info("Shutdown initiated")
    shutdown_event.set()

    async def _close_group(peers: Set[WebSocket], timeout: float = 0.2):
        sockets = list(peers)
        peers.clear()
        if not sockets:
            return
        tasks = [asyncio.create_task(_close_ws_quietly(ws)) for ws in sockets]
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        for t in pending:
            t.cancel()
        LOGGER.debug(
            "Closed WS group | closed=%d cancelled=%d", len(done), len(pending)
        )

    await asyncio.gather(
        _close_group(hub.ui_sockets),
        _close_group(bus_sockets),
        _close_group(agent_sockets),
    )
    LOGGER.info("Shutdown complete")


@app.post("/logs/clear")
async def clear_logs():
    cleared = []
    for name in ("server.log", "client.log", "server.out", "client.out"):
        p = DATA_DIR / name
        try:
            if p.exists():
                with open(p, "w", encoding="utf-8"):
                    pass
                cleared.append(name)
        except Exception:
            pass
    LOGGER.info("POST /logs/clear done | cleared=%s", cleared)
    return RedirectResponse("/", status_code=303)


@app.get("/logs/{which}", response_class=PlainTextResponse)
async def logs(which: str, tail: int = 20000):
    if which == "server":
        candidates = ["server.out", "server.log"]
    elif which == "client":
        candidates = ["client.out", "client.log"]
    else:
        return PlainTextResponse("invalid", status_code=400)

    for name in candidates:
        p = DATA_DIR / name
        try:
            if p.exists() and p.stat().st_size > 0:
                text = p.read_text(encoding="utf-8", errors="ignore")
                if tail and tail > 0 and len(text) > tail:
                    text = text[-tail:]
                return PlainTextResponse(text)
        except Exception:
            continue

    return PlainTextResponse("No logs yet.", status_code=404)


@app.on_event("startup")
async def _startup_links():
    await startup_links(app, templates_env=templates.env, set_jinja_global=True)


@app.on_event("shutdown")
async def _shutdown():
    await shutdown_links(app)


@app.on_event("startup")
async def _apply_db_log_level_and_banner():
    try:
        env = _read_env()
        lvl_name = (env.get("LOG_LEVEL") or "INFO").upper()
        LOGGER.logger.setLevel(getattr(logging, lvl_name, logging.INFO))
        if env.get("LOG_FORMAT"):
            os.environ["LOG_FORMAT"] = env["LOG_FORMAT"]
            configure_app_logging()
    except Exception:
        pass
    LOGGER.debug(
        "Starting %s | LOG_LEVEL=%s | LOG_FORMAT=%s | WS_SERVER_CTRL=%s | WS_CLIENT_CTRL=%s",
        APP_TITLE,
        logging.getLevelName(LOGGER.logger.level),
        os.getenv("LOG_FORMAT", "HUMAN"),
        SERVER_CTRL_URL,
        CLIENT_CTRL_URL,
    )


@app.on_event("startup")
async def _start_bg_tasks():
    asyncio.create_task(_lock_listener())


@app.on_event("startup")
async def _start_release_watcher():
    asyncio.create_task(_release_watch_loop())


@app.on_event("startup")
async def _start_backup_scheduler():
    backup_scheduler.start()


@app.on_event("shutdown")
async def _stop_backup_scheduler():
    await backup_scheduler.stop()


@app.api_route("/admin/backup-now", methods=["GET", "POST"])
async def backup_now():
    out_path = await backup_scheduler.run_now()

    return {"ok": True, "file": out_path.name}


@app.get("/api/backup/info")
async def backup_info():
    def _cfg(k, d=""):
        return db.get_config(k, d)

    last_at = _cfg("DB_LAST_BACKUP_AT", "")
    last_file = _cfg("DB_LAST_BACKUP_FILE", "")
    last_size = int(_cfg("DB_LAST_BACKUP_SIZE", "0") or 0)
    archives = []
    if BACKUP_DIR.exists():
        for p in sorted(
            BACKUP_DIR.glob("*.tar.gz"), key=lambda x: x.stat().st_mtime, reverse=True
        ):
            try:
                st = p.stat()
                archives.append(
                    {"name": p.name, "size": st.st_size, "mtime": int(st.st_mtime)}
                )
            except Exception:
                pass
    return {
        "ok": True,
        "last_backup_at": last_at,
        "last_backup_file": last_file,
        "last_backup_size": last_size,
        "dir": str(BACKUP_DIR),
        "archives": archives,
    }


@app.get("/api/backup/download/{name}")
async def backup_download(name: str):
    p = BACKUP_DIR / name
    if not p.exists() or not p.is_file():
        return PlainTextResponse("not found", status_code=404)
    return FileResponse(str(p), filename=name, media_type="application/gzip")


@app.post("/api/backup/delete")
async def backup_delete(name: str = Form(...)):
    """
    Permanently delete a backup archive from BACKUP_DIR.
    """
    p = BACKUP_DIR / name
    if not p.exists() or not p.is_file():
        return PlainTextResponse("not found", status_code=404)
    try:
        p.unlink()
    except Exception as e:
        return PlainTextResponse(f"delete failed: {e}", status_code=500)
    return {"ok": True, "deleted": name}


@app.post("/api/backup/restore")
async def backup_restore(
    source: str = Form("upload"),
    file: UploadFile | None = File(None),
    name: str | None = Form(None),
):
    """
    Restore from an uploaded .tar.gz or from an existing archive in BACKUP_DIR.
    Safeguards:
      - Stops agents
      - Atomic replace of live DB
    """
    if source not in ("upload", "existing"):
        return PlainTextResponse("bad source", status_code=400)

    if source == "existing":
        if not name:
            return PlainTextResponse("name required", status_code=400)
        arc = BACKUP_DIR / name
        if not arc.exists():
            return PlainTextResponse("archive not found", status_code=404)
    else:
        if not file:
            return PlainTextResponse("file required", status_code=400)
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        arc = BACKUP_DIR / f"restore-{int(time.time())}.tar.gz"
        with open(arc, "wb") as f:
            f.write(await file.read())

    try:
        await _ws_cmd(SERVER_CTRL_URL, {"cmd": "stop"})
        await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "stop"})
    except Exception as e:
        LOGGER.warning("restore: stop agents failed (continuing): %s", e)

    with tempfile.TemporaryDirectory() as td:
        tmp_dir = Path(td)
        with tarfile.open(arc, "r:gz") as tar:
            members = tar.getmembers()
            names = [m.name for m in members]
            if "data.db" not in names:
                return PlainTextResponse("archive missing data.db", status_code=400)
            tar.extract("data.db", path=tmp_dir)
        extracted = tmp_dir / "data.db"
        if not extracted.exists():
            return PlainTextResponse("extraction failed", status_code=500)

        live = Path(DB_PATH)
        bak = live.with_suffix(".bak")
        try:
            if live.exists():
                shutil.copy2(live, bak)

            shutil.copy2(extracted, live)
        except Exception as e:
            return PlainTextResponse(f"restore failed: {e}", status_code=500)

    db.set_config("DB_LAST_RESTORE_AT", datetime.utcnow().isoformat() + "Z")
    return {"ok": True, "restored_from": arc.name}


@app.get("/system")
async def system_page(request: Request):
    return templates.TemplateResponse(
        "system.html",
        {
            "request": request,
            "title": f"System · {APP_TITLE}",
            "version": CURRENT_VERSION,
        },
    )


@app.get("/logs/stream/{which}")
async def logs_stream(which: str, request: Request, tail_bytes: int = 50000):
    if which == "server":
        candidates = ["server.out", "server.log"]
    elif which == "client":
        candidates = ["client.out", "client.log"]
    else:
        return PlainTextResponse("invalid", status_code=400)

    async def gen():
        def pick_path():
            for n in candidates:
                p = DATA_DIR / n
                if p.exists():
                    return p
            return None

        HEARTBEAT_EVERY = 15.0

        while not shutdown_event.is_set():
            if await request.is_disconnected():
                break

            path = pick_path()
            if not path:
                yield ": keepalive\n\n"
                await asyncio.sleep(0.2)
                continue

            try:
                last_stat = path.stat()
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    f.seek(0, os.SEEK_END)
                    size = f.tell()
                    start = max(0, size - int(tail_bytes))
                    f.seek(start)
                    if start > 0:
                        f.readline()

                    last_hb = time.monotonic()

                    batch = []
                    for line in f:
                        if shutdown_event.is_set() or await request.is_disconnected():
                            break
                        batch.append(line.rstrip())
                        if len(batch) >= 50:
                            yield f"data: {json.dumps({'lines': batch})}\n\n"
                            batch.clear()
                    if batch:
                        yield f"data: {json.dumps({'lines': batch})}\n\n"

                    while not shutdown_event.is_set():
                        if await request.is_disconnected():
                            break

                        pos = f.tell()
                        line = f.readline()
                        if line:
                            yield f"data: {json.dumps({'line': line.rstrip()})}\n\n"
                            last_hb = time.monotonic()
                        else:
                            await asyncio.sleep(0.2)
                            now = time.monotonic()
                            if now - last_hb >= HEARTBEAT_EVERY:
                                yield ":ka\n\n"
                                last_hb = now
                            try:
                                st = os.stat(path)
                            except FileNotFoundError:
                                break
                            if (st.st_ino != last_stat.st_ino) or (
                                st.st_dev != last_stat.st_dev
                            ):
                                break
                            if pos > st.st_size:
                                f.seek(st.st_size)
                            else:
                                f.seek(pos)
            except Exception:
                if shutdown_event.is_set() or await request.is_disconnected():
                    break
                yield ": keepalive\n\n"
                await asyncio.sleep(0.2)

        LOGGER.info("SSE /logs/stream/%s closed", which)
        yield "event: close\ndata: bye\n\n"

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


def _derive_state(obj: dict) -> str:
    """
    Normalize a status payload into 'running' or 'stopped' (or passthrough).
    Accepts either:
      - running=True
      - status string in a set of "good" states
      - presence of a pid as a strong hint of running
    """
    s = str(obj.get("status", "")).strip().lower()
    if obj.get("running") is True:
        return "running"

    good = {
        "running",
        "started",
        "active",
        "online",
        "ok",
        "ready",
        "up",
        "connected",
        "logged_in",
        "logged-in",
        "authenticated",
        "awake",
    }
    bad = {"stopped", "offline", "down", "error", "dead", "failed"}

    if s in good:
        return "running"
    if s in bad:
        return "stopped"
    if obj.get("pid"):
        return "running"
    return "stopped" if s == "" else s


def _enrich_from_bus(ctrl: dict, bus: dict) -> dict:
    out = dict(ctrl or {})
    if not out.get("status") and bus.get("status"):
        out["status"] = bus["status"]
    if not out.get("pid") and bus.get("pid"):
        out["pid"] = bus["pid"]
    if "running" not in out and "running" in bus:
        out["running"] = bus["running"]
    if "discord" not in out and isinstance(bus.get("discord"), dict):
        out["discord"] = bus["discord"]
    out.setdefault("status", "")
    return out


def _is_discord_ready(obj: dict) -> bool:
    """
    Accepts multiple shapes so agents can send whatever is convenient.
    We consider the bot 'ready' if any of these are truthy/readyish:
      - obj['discord']['ready' | 'connected' | 'online'] is True
      - obj['discord']['state'] in {'ready','connected','online'}
      - obj['gateway'] in {'ready','connected','online'}
      - obj['discord_status'] in {'ready','connected','online'}
    """
    if not isinstance(obj, dict):
        return False

    d = obj.get("discord")
    if isinstance(d, dict):
        if d.get("ready") or d.get("connected") or d.get("online"):
            return True
        st = str(d.get("state", "")).lower()
        if st in {"ready", "connected", "online"}:
            return True

    st2 = str(obj.get("gateway") or obj.get("discord_status") or "").lower()
    return st2 in {"ready", "connected", "online"}


async def _collect_status() -> dict:
    with _Timer("/status") as t:
        s_server = await _ws_cmd(SERVER_CTRL_URL, {"cmd": "status"}, timeout=0.7)
        s_client = await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "status"}, timeout=0.7)

    bus_srv = hub.status.get("server") or {}
    bus_cli = hub.status.get("client") or {}

    s_server = _enrich_from_bus(s_server, bus_srv)
    s_client = _enrich_from_bus(s_client, bus_cli)

    server_state = _derive_state(s_server)
    client_state = _derive_state(s_client)
    both_running = (server_state == "running") and (client_state == "running")

    server_ready = _is_discord_ready(s_server) or _is_discord_ready(bus_srv)
    client_ready = _is_discord_ready(s_client) or _is_discord_ready(bus_cli)
    both_ready = server_ready and client_ready

    res = {
        "server": {**s_server, "state": server_state, "ready": server_ready},
        "client": {**s_client, "state": client_state, "ready": client_ready},
        "both_running": both_running,
        "both_ready": both_ready,
        "running_and_ready": both_running and both_ready,
        "running": both_running,
        "status": "running" if both_running else "stopped",
    }

    LOGGER.debug(
        "GET /status | took_ms=%.1f running=%s ready=%s",
        t.ms,
        both_running,
        both_ready,
        extra={"took_ms": round(t.ms, 1)},
    )
    return res


@app.get("/api/status", response_class=JSONResponse)
async def api_status_alias():
    return await _collect_status()


@app.get("/filters")
def get_filters():
    f = db.get_filters()
    out = {
        "whitelist": {"category": [], "channel": []},
        "exclude": {"category": [], "channel": []},
    }
    for scope in ("category", "channel"):
        out["whitelist"][scope] = [str(i) for i in sorted(f["whitelist"][scope])]
        out["exclude"][scope] = [str(i) for i in sorted(f["exclude"][scope])]
    LOGGER.debug(
        "GET /filters | wl_cat=%d wl_ch=%d ex_cat=%d ex_ch=%d",
        len(out["whitelist"]["category"]),
        len(out["whitelist"]["channel"]),
        len(out["exclude"]["category"]),
        len(out["exclude"]["channel"]),
    )
    return out


@app.post("/filters/save")
async def save_filters(request: Request):
    form = await request.form()

    def parse_ids(key: str) -> list[int]:
        raw = str(form.get(key, "") or "").replace("\n", ",").replace(" ", ",")
        items = [s for s in (x.strip() for x in raw.split(",")) if s]
        out = []
        for s in items:
            try:
                out.append(int(s))
            except Exception:
                pass
        return list(dict.fromkeys(out))

    wl_cats = parse_ids("wl_categories")
    wl_chs = parse_ids("wl_channels")
    ex_cats = parse_ids("ex_categories")
    ex_chs = parse_ids("ex_channels")

    db.replace_filters(wl_cats, wl_chs, ex_cats, ex_chs)

    payload = {
        "whitelist": {"category": wl_cats, "channel": wl_chs},
        "exclude": {"category": ex_cats, "channel": ex_chs},
    }
    LOGGER.info(
        "POST /filters/save | wl_cat=%d wl_ch=%d ex_cat=%d ex_ch=%d",
        len(wl_cats),
        len(wl_chs),
        len(ex_cats),
        len(ex_chs),
    )
    await hub.publish("filters", "both", payload)
    asyncio.create_task(
        _ws_cmd(CLIENT_AGENT_URL, {"type": "filters_reload"}, timeout=1.0)
    )
    return RedirectResponse("/", status_code=303)


@app.post("/api/filters/blacklist", response_class=JSONResponse)
async def api_blacklist_add(payload: dict = Body(...)):
    try:
        scope = str(payload.get("scope", "")).strip().lower()
        if scope not in ("category", "channel"):
            raise ValueError("invalid-scope")

        raw_id = str(payload.get("obj_id", "")).strip()
        if not raw_id.isdigit():
            raise ValueError("invalid-obj_id")
        obj_id = int(raw_id)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid-input")

    try:
        db.add_filter("exclude", scope, obj_id)
        asyncio.create_task(
            _ws_cmd(CLIENT_AGENT_URL, {"type": "filters_reload"}, timeout=1.0)
        )
        return {"ok": True, "scope": scope, "obj_id": str(obj_id)}

    except Exception:
        raise HTTPException(status_code=500, detail="db-failure")


def _read_env() -> Dict[str, str]:
    vals = DEFAULTS.copy()
    try:
        stored = db.get_all_config()
        for k, v in stored.items():
            if k in ALLOWED_ENV and v is not None:
                vals[k] = str(v)
    except Exception:
        pass
    for k in ALLOWED_ENV:
        vals.setdefault(k, "")
    LOGGER.debug("Config read | %s", _redact_dict(vals))
    return vals


def _write_env(values: Dict[str, str]) -> None:
    for k in ALLOWED_ENV:
        v = values.get(k, "") or ""
        if k in BOOL_KEYS:
            v = _norm_bool_str(v)
        if k == "LOG_LEVEL":
            v = "DEBUG" if str(v).upper() == "DEBUG" else "INFO"
        if k == "LOG_FORMAT":
            v = "JSON" if str(v).upper() == "JSON" else "HUMAN"
        db.set_config(k, v)
    LOGGER.info("Config saved | %s", _redact_dict(values))

    try:
        active = db.get_active_profile()
    except Exception:
        active = None
    if active:
        settings_payload = {
            key: _as_bool(values.get(key, active.get("settings", {}).get(key, False)))
            for key in PROFILE_BOOL_KEYS
        }
        profile_payload = {
            "name": active.get("name"),
            "server_token": values.get("SERVER_TOKEN", active.get("server_token", "")),
            "client_token": values.get("CLIENT_TOKEN", active.get("client_token", "")),
            "host_guild_id": values.get("HOST_GUILD_ID", active.get("host_guild_id", "")),
            "clone_guild_id": values.get("CLONE_GUILD_ID", active.get("clone_guild_id", "")),
            "command_users": values.get("COMMAND_USERS", active.get("command_users", "")),
            "settings": settings_payload,
        }
        db.update_profile(active.get("id"), profile_payload)
        db.set_active_profile(active.get("id"))


def _validate(values: Dict[str, str]) -> List[str]:
    errs: List[str] = []

    for k in REQUIRED:
        if not (values.get(k) or "").strip():
            errs.append(f"Missing {k}")

    raw_clone = (values.get("CLONE_GUILD_ID", "") or "").strip()
    try:
        if int(raw_clone) <= 0:
            errs.append("CLONE_GUILD_ID must be a positive integer")
    except Exception:
        errs.append("CLONE_GUILD_ID must be an integer")

    raw_host = (values.get("HOST_GUILD_ID", "") or "").strip()
    if raw_host != "":
        try:
            if int(raw_host) <= 0:
                errs.append("HOST_GUILD_ID must be a positive integer")
        except Exception:
            errs.append("HOST_GUILD_ID must be an integer")

    if errs:
        LOGGER.warning("Config validation failed | errs=%s", errs)
    else:
        LOGGER.debug("Config validation ok")

    return errs


def _norm_bool_str(v: str) -> str:
    return "True" if str(v).strip().lower() in ("true", "1", "yes", "on") else "False"


def _as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in ("true", "1", "yes", "on")


def _serialize_profile(profile: dict, active_id: str) -> dict:
    settings = {k: bool(profile.get("settings", {}).get(k, False)) for k in PROFILE_BOOL_KEYS}
    return {
        "id": profile.get("id"),
        "name": profile.get("name") or "",
        "server_token": profile.get("server_token") or "",
        "client_token": profile.get("client_token") or "",
        "host_guild_id": profile.get("host_guild_id") or "",
        "clone_guild_id": profile.get("clone_guild_id") or "",
        "command_users": profile.get("command_users") or "",
        "settings": settings,
        "created_at": profile.get("created_at"),
        "updated_at": profile.get("updated_at"),
        "is_active": profile.get("id") == active_id,
    }


def _profile_defaults() -> dict:
    env = _read_env()
    settings = {k: _as_bool(env.get(k, False)) for k in PROFILE_BOOL_KEYS}
    return {
        "name": "New profile",
        "server_token": env.get("SERVER_TOKEN", ""),
        "client_token": env.get("CLIENT_TOKEN", ""),
        "host_guild_id": env.get("HOST_GUILD_ID", ""),
        "clone_guild_id": env.get("CLONE_GUILD_ID", ""),
        "command_users": env.get("COMMAND_USERS", ""),
        "settings": settings,
    }


def _prepare_profile_payload(data: dict, existing: dict | None = None) -> dict:
    existing = existing or {}
    settings_existing = existing.get("settings", {})
    settings_input = data.get("settings") or {}
    if not isinstance(settings_input, dict):
        settings_input = {}

    settings = {}
    for key in PROFILE_BOOL_KEYS:
        if key in settings_input:
            settings[key] = _as_bool(settings_input.get(key))
        else:
            settings[key] = bool(settings_existing.get(key, False))

    return {
        "name": (data.get("name") or existing.get("name") or "Untitled profile").strip(),
        "server_token": (data.get("server_token") or existing.get("server_token") or "").strip(),
        "client_token": (data.get("client_token") or existing.get("client_token") or "").strip(),
        "host_guild_id": (data.get("host_guild_id") or existing.get("host_guild_id") or "").strip(),
        "clone_guild_id": (data.get("clone_guild_id") or existing.get("clone_guild_id") or "").strip(),
        "command_users": (data.get("command_users") or existing.get("command_users") or "").strip(),
        "settings": settings,
    }


def _validate_profile_payload(data: dict, existing: dict | None = None) -> list[str]:
    errs: list[str] = []
    existing = existing or {}

    name = (data.get("name") or existing.get("name") or "").strip()
    if not name:
        errs.append("Profile name is required")

    server_token = (data.get("server_token") or existing.get("server_token") or "").strip()
    client_token = (data.get("client_token") or existing.get("client_token") or "").strip()
    clone_id = (data.get("clone_guild_id") or existing.get("clone_guild_id") or "").strip()
    host_id = (data.get("host_guild_id") or existing.get("host_guild_id") or "").strip()

    if not server_token:
        errs.append("SERVER_TOKEN is required")
    if not client_token:
        errs.append("CLIENT_TOKEN is required")
    if not clone_id:
        errs.append("CLONE_GUILD_ID is required")
    elif not clone_id.isdigit():
        errs.append("CLONE_GUILD_ID must be a positive integer")

    if host_id and not host_id.isdigit():
        errs.append("HOST_GUILD_ID must be a positive integer")

    return errs


@app.get("/api/profiles", response_class=JSONResponse)
async def api_profiles_list():
    profiles = db.list_profiles()
    active_id = db.get_active_profile_id()
    serialized = [_serialize_profile(p, active_id) for p in profiles]
    return {
        "ok": True,
        "profiles": serialized,
        "active_profile_id": active_id,
        "bool_keys": PROFILE_BOOL_KEYS,
        "text_keys": PROFILE_TEXT_KEYS,
        "defaults": _profile_defaults(),
    }


@app.post("/api/profiles", response_class=JSONResponse)
async def api_profiles_create(payload: dict = Body(...)):
    errs = _validate_profile_payload(payload)
    if errs:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="; ".join(errs))

    profile_data = _prepare_profile_payload(payload)
    profile = db.create_profile(profile_data)

    if _as_bool(payload.get("activate")):
        profile = db.set_active_profile(profile["id"])

    active_id = db.get_active_profile_id()
    return {
        "ok": True,
        "profile": _serialize_profile(profile, active_id),
        "active_profile_id": active_id,
    }


@app.put("/api/profiles/{profile_id}", response_class=JSONResponse)
async def api_profiles_update(profile_id: str, payload: dict = Body(...)):
    existing = db.get_profile(profile_id)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profile not found")

    errs = _validate_profile_payload(payload, existing)
    if errs:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="; ".join(errs))

    profile_data = _prepare_profile_payload(payload, existing)
    updated = db.update_profile(profile_id, profile_data)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profile not found")

    if _as_bool(payload.get("activate")):
        updated = db.set_active_profile(profile_id)

    active_id = db.get_active_profile_id()
    return {
        "ok": True,
        "profile": _serialize_profile(updated, active_id),
        "active_profile_id": active_id,
    }


@app.delete("/api/profiles/{profile_id}", response_class=JSONResponse)
async def api_profiles_delete(profile_id: str):
    active_id = db.get_active_profile_id()
    if active_id == profile_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete the active profile",
        )
    deleted = db.delete_profile(profile_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profile not found")
    return {"ok": True, "deleted": profile_id}


@app.post("/api/profiles/{profile_id}/activate", response_class=JSONResponse)
async def api_profiles_activate(profile_id: str):
    try:
        profile = db.set_active_profile(profile_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profile not found")

    active_id = db.get_active_profile_id()
    return {
        "ok": True,
        "profile": _serialize_profile(profile, active_id),
        "active_profile_id": active_id,
    }


@app.get("/channels")
async def channels_page(request: Request):
    env = _read_env()
    return templates.TemplateResponse(
        "channels.html",
        {
            "request": request,
            "title": APP_TITLE,
            "version": CURRENT_VERSION,
            "log_level": env.get("LOG_LEVEL", "INFO"),
        },
    )


@app.get("/api/channels", response_class=JSONResponse)
async def channels_api():
    chans = [dict(r) for r in db.get_all_channel_mappings()]
    cat_rows = [dict(r) for r in db.get_all_category_mappings()]

    cats_by_id = {int(r["original_category_id"]): r for r in cat_rows}

    out = []
    for ch in chans:
        pid = ch.get("original_parent_category_id")
        pid_int = int(pid) if pid not in (None, "", 0) else None

        cat_info = cats_by_id.get(pid_int, {})
        original_cat_name = cat_info.get("original_category_name")
        cloned_cat_name = cat_info.get("cloned_category_name") or None
        cloned_cat_id = (
            str(cat_info.get("cloned_category_id"))
            if cat_info.get("cloned_category_id") not in (None, "", 0)
            else None
        )

        out.append(
            {
                "original_channel_id": (
                    str(ch["original_channel_id"])
                    if ch.get("original_channel_id")
                    else ""
                ),
                "original_channel_name": ch.get("original_channel_name") or "",
                "cloned_channel_id": (
                    str(ch["cloned_channel_id"])
                    if ch.get("cloned_channel_id")
                    else None
                ),
                "channel_type": int(ch.get("channel_type", 0)),
                "category_name": original_cat_name,
                "original_category_name": original_cat_name,
                "cloned_category_name": cloned_cat_name,
                "original_parent_category_id": str(pid_int) if pid_int else None,
                "cloned_category_id": cloned_cat_id,
                "channel_webhook_url": ch.get("channel_webhook_url"),
                "clone_channel_name": ch.get("clone_channel_name") or None,
            }
        )

    return {"items": out}


@app.get("/api/backfills/inflight")
async def api_backfills_inflight():

    res = await _ws_cmd(SERVER_AGENT_URL, {"type": "backfills_status_query"})

    items = (res or {}).get("data", {}).get("items", {})
    return JSONResponse({"ok": True, "items": items})


@app.get("/api/backfills/resume-info", response_class=JSONResponse)
async def api_backfills_resume_info(channel_id: int):
    try:
        cid = int(channel_id)
    except Exception:
        return JSONResponse(
            {"ok": False, "error": "invalid-channel_id"}, status_code=400
        )

    row = db.backfill_get_incomplete_for_channel(cid)

    payload = {
        "channel_id": str(cid),
        "active": bool(row is not None),
        "resumable": False,
        "run_id": None,
        "delivered": None,
        "expected_total": None,
        "checkpoint": {
            "last_orig_message_id": None,
            "last_orig_timestamp": None,
        },
        "clone_channel_id": None,
        "range": None,
        "started_at": None,
        "updated_at": None,
    }

    if row:
        payload.update(
            {
                "resumable": True,
                "run_id": row.get("run_id"),
                "delivered": row.get("delivered"),
                "expected_total": row.get("expected_total"),
                "checkpoint": {
                    "last_orig_message_id": row.get("last_orig_message_id"),
                    "last_orig_timestamp": row.get("last_orig_timestamp"),
                },
                "clone_channel_id": row.get("clone_channel_id"),
                "range": json.loads(row.get("range_json") or "{}"),
                "started_at": row.get("started_at"),
                "updated_at": row.get("updated_at"),
            }
        )

    return JSONResponse({"ok": True, "data": payload})


@app.post("/api/backfill/start", response_class=JSONResponse)
async def api_backfill_start(payload: dict = Body(...)):
    try:
        channel_id = int(payload.get("channel_id") or payload.get("clone_channel_id"))
    except Exception:
        return JSONResponse(
            {"ok": False, "error": "invalid-channel_id"}, status_code=400
        )

    st = await locks.status(channel_id)
    if st in ("launching", "running"):
        return JSONResponse(
            {
                "ok": False,
                "error": "backfill-already-running",
                "channel_id": channel_id,
                "state": st,
            },
            status_code=409,
        )

    ok = await locks.try_acquire_launching(channel_id)
    if not ok:
        return JSONResponse(
            {
                "ok": False,
                "error": "backfill-already-running",
                "channel_id": channel_id,
                "state": "launching",
            },
            status_code=409,
        )

    mode = payload.get("mode") or (payload.get("range") or {}).get("mode") or "all"
    after_iso = payload.get("since") or payload.get("after_iso")
    before_iso = (
        payload.get("before_iso")
        or (payload.get("range") or {}).get("before")
        or payload.get("until")
        or payload.get("to_iso")
    )
    last_n = payload.get("last_n")

    data = {"channel_id": channel_id}

    # Normal range params
    if after_iso:
        data["after_iso"] = str(after_iso)
    if before_iso:
        data["before_iso"] = str(before_iso)
    if last_n is not None:
        try:
            data["last_n"] = int(last_n)
        except Exception:
            await locks.release(channel_id)
            return JSONResponse(
                {"ok": False, "error": "invalid-last_n"}, status_code=400
            )

    if mode == "between":
        data["range"] = {
            "mode": mode,
            "value": {"after": after_iso, "before": before_iso},
        }
    else:
        rng_val = (
            after_iso
            if after_iso
            else (data.get("last_n") if "last_n" in data else None)
        )
        data["range"] = {"mode": mode, "value": rng_val} if mode else None

    if bool(payload.get("resume")):
        data["resume"] = True
        cp = payload.get("checkpoint") or {}
        after_id = cp.get("last_orig_message_id") or payload.get("after_id")
        after_ts = cp.get("last_orig_timestamp") or payload.get("after_ts")

        if after_id:
            data["after_id"] = str(after_id)
        if after_ts and not data.get("after_iso"):
            data["after_iso"] = str(after_ts)

    res = await _ws_cmd(CLIENT_AGENT_URL, {"type": "clone_messages", "data": data})

    if not res.get("ok", True):
        await locks.release(channel_id)
        return JSONResponse(
            {"ok": False, "error": res.get("error") or "client-agent-failed"},
            status_code=502,
        )

    return JSONResponse({"ok": True})


@app.get("/guilds")
async def guilds_page(request: Request):
    env = _read_env()
    return templates.TemplateResponse(
        "guilds.html",
        {
            "request": request,
            "title": APP_TITLE,
            "version": CURRENT_VERSION,
            "log_level": env.get("LOG_LEVEL", "INFO"),
        },
    )


@app.get("/api/guilds", response_class=JSONResponse)
async def guilds_api():
    """
    Return list of guilds for the UI.
    Shape:
      { items: [ { id, name, icon_url, member_count }, ... ] }
    """
    rows = db.get_all_guilds()
    items = []
    for r in rows:
        items.append(
            {
                "id": str(r.get("guild_id", "")),
                "name": r.get("name") or "Unknown guild",
                "icon_url": r.get("icon_url"),
                "member_count": r.get("member_count"),
            }
        )
    return {"items": items}


CLIENT_AGENT_TIMEOUT = int(os.getenv("CLIENT_AGENT_TIMEOUT", "10"))


@app.post("/api/scrape", response_class=JSONResponse)
async def api_scrape(request: Request):
    try:
        payload = await request.json()
        LOGGER.debug("SCRAPE request payload: %s", payload)
    except Exception as e:
        LOGGER.exception("Failed to parse JSON body: %s", e)
        return JSONResponse({"ok": False, "error": "invalid-json"}, status_code=400)

    include_username = bool(payload.get("include_username", False))
    include_avatar_url = bool(payload.get("include_avatar_url", False))
    include_bio = bool(payload.get("include_bio", False))
    include_roles = bool(payload.get("include_roles", False))

    if payload.get("include_names") and not (
        payload.get("include_username")
        or payload.get("include_avatar_url")
        or payload.get("include_bio")
    ):
        include_username = True
        include_avatar_url = True

    def clamp(v, lo, hi):
        try:
            return max(lo, min(hi, int(v)))
        except Exception:
            return lo

    ns = clamp(payload.get("num_sessions", 2), 1, 5)
    mpps_raw = payload.get("max_parallel_per_session")
    if mpps_raw is None:
        mpps = clamp(max(1, 8 // ns), 1, 5)
    else:
        mpps = clamp(mpps_raw, 1, 5)

    gid = payload.get("guild_id")

    LOGGER.debug(
        "Dispatching scrape to agent: gid=%s ns=%s mpps=%s username=%s avatar=%s bio=%s roles=%s",
        gid,
        ns,
        mpps,
        include_username,
        include_avatar_url,
        include_bio,
        include_roles,
    )

    try:
        res = await _ws_cmd(
            CLIENT_AGENT_URL,
            {
                "type": "scrape_members",
                "data": {
                    "guild_id": gid,
                    "num_sessions": ns,
                    "max_parallel_per_session": mpps,
                    "include_username": include_username,
                    "include_avatar_url": include_avatar_url,
                    "include_bio": include_bio,
                    "include_roles": include_roles,
                },
            },
            timeout=CLIENT_AGENT_TIMEOUT,
        )
        LOGGER.debug("Agent response: %s", res)
    except asyncio.TimeoutError:
        LOGGER.error("Timeout waiting for client agent (>%ss)", CLIENT_AGENT_TIMEOUT)
        return JSONResponse(
            {"ok": False, "error": "client-agent-timeout"},
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
        )
    except ConnectionRefusedError as e:
        LOGGER.error("Connection to client agent refused: %s", e)
        return JSONResponse(
            {"ok": False, "error": "client-agent-unreachable"},
            status_code=status.HTTP_502_BAD_GATEWAY,
        )
    except Exception as e:
        LOGGER.exception("Unexpected client agent error: %s", e)
        return JSONResponse(
            {"ok": False, "error": f"client-agent-error: {type(e).__name__}: {e}"},
            status_code=status.HTTP_502_BAD_GATEWAY,
        )

    if not res.get("ok", True):
        err = (res.get("error") or "").strip()

        if not err:
            LOGGER.warning(
                "Agent returned not-ok with empty error; returning 202 Accepted: %s",
                res,
            )
            return JSONResponse(
                {"ok": True, "accepted": True}, status_code=status.HTTP_202_ACCEPTED
            )

        if "already" in err and "running" in err:
            return JSONResponse(
                {"ok": False, "error": "scrape-already-running"}, status_code=409
            )

        return JSONResponse(
            {"ok": False, "error": err or "client-agent-failed"}, status_code=502
        )


@app.get("/api/scrape/state", response_class=JSONResponse)
async def api_scrape_state():
    try:
        res = await _ws_cmd(CLIENT_AGENT_URL, {"type": "scrape_status"}, timeout=2.0)
        if not res.get("ok", True):
            return {"running": False, "guild_id": None}
        return {"running": bool(res.get("running")), "guild_id": res.get("guild_id")}
    except Exception:
        return {"running": False, "guild_id": None}


@app.post("/api/scrape/cancel", response_class=JSONResponse)
async def api_scrape_cancel(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    gid = payload.get("guild_id")

    try:
        res = await _ws_cmd(
            CLIENT_AGENT_URL,
            {"type": "scrape_cancel", "data": {"guild_id": gid}},
            timeout=2.0,
        )
    except ConnectionRefusedError:
        return JSONResponse(
            {"ok": False, "error": "client-agent-unreachable"}, status_code=502
        )
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"client-agent-error: {type(e).__name__}: {e}"},
            status_code=502,
        )

    if not res.get("ok", True):
        return JSONResponse(
            {"ok": False, "error": res.get("error") or "client-agent-failed"},
            status_code=502,
        )

    return JSONResponse({"ok": True})


@app.get("/api/guilds/{guild_id}", response_class=JSONResponse)
async def guild_details(guild_id: str):
    """
    Return details for a single guild.
    Shape:
      { id, name, icon_url, member_count, ... }
    """
    try:
        rows = db.get_all_guilds()
        row = next((r for r in rows if str(r.get("guild_id")) == str(guild_id)), None)
        if not row:
            return JSONResponse({"ok": False, "error": "not-found"}, status_code=404)

        out = {
            "id": str(row.get("guild_id") or ""),
            "name": row.get("name") or "Unknown guild",
            "icon_url": row.get("icon_url"),
            "member_count": row.get("member_count"),
            "owner_id": row.get("owner_id"),
            "created_at": row.get("created_at"),
            "description": row.get("description"),
        }
        return {"ok": True, "item": out}
    except Exception as e:
        LOGGER.exception("guild_details failed for id=%s: %s", guild_id, e)
        return JSONResponse({"ok": False, "error": "server-error"}, status_code=500)


def _canon(s: str | None) -> str:
    if s is None:
        return ""
    return unicodedata.normalize("NFKC", str(s)).strip()


_DASHES_RE = re.compile(r"-{2,}")


def _discordify(s: str | None) -> str | None:
    """
    Convert free text to a Discord-safe channel name:
    - Normalize NFKC
    - Lowercase A–Z
    - Whitespace -> '-'
    - Allow emojis and most Unicode symbols (no aggressive stripping)
    - Collapse multiple '-'
    - Trim leading/trailing '-'
    - Enforce max length 100
    """
    if s is None:
        return None

    t = unicodedata.normalize("NFKC", str(s)).strip()
    if not t:
        return None

    t = t.lower()

    t = re.sub(r"\s+", "-", t)

    t = _DASHES_RE.sub("-", t).strip("-")

    if len(t) > 100:
        t = t[:100].rstrip("-") or None

    return t or None


@app.post("/api/channels/customize", response_class=JSONResponse)
async def api_channels_customize(payload: dict = Body(...)):
    """
    Set or clear a channel's custom clone name (Discord-safe).
    Rules:
      - Input is normalized to Discord format via _discordify()
      - Empty/null or same-as-original -> store NULL
      - Skip DB + WS nudge if nothing changes
      - Skip WS nudge when clearing because it's same-as-original
    """
    try:
        ocid = int(payload.get("original_channel_id"))
    except Exception:
        return JSONResponse(
            {"ok": False, "error": "invalid-original_channel_id"}, status_code=400
        )

    desired = _discordify(payload.get("clone_channel_name", None))

    try:
        orig = db.get_original_channel_name(ocid)
    except Exception:
        orig = None

    same_as_original = False
    if desired is not None and _canon(orig) == desired:
        desired = None
        same_as_original = True

    try:
        current_raw = db.get_clone_channel_name(ocid)
    except Exception:
        current_raw = None

    needs_update = (desired is None and current_raw is not None) or (
        desired is not None and current_raw != desired
    )

    if not needs_update:
        LOGGER.info(
            "Customize channel | original_id=%s no change (kept=%r)", ocid, current_raw
        )
        return JSONResponse(
            {"ok": True, "changed": False, "normalized": desired is not None}
        )

    try:
        db.set_channel_clone_name(ocid, desired)
        LOGGER.info(
            "Customize channel | original_id=%s updated to %r (orig=%r, was=%r)",
            ocid,
            desired,
            orig,
            current_raw,
        )
    except Exception as e:
        LOGGER.exception("Failed to set clone_channel_name: %s", e)
        return JSONResponse({"ok": False, "error": "db-failure"}, status_code=500)

    should_nudge = not (desired is None and same_as_original)
    if should_nudge:
        try:
            asyncio.create_task(
                _ws_cmd(CLIENT_AGENT_URL, {"type": "sitemap_request"}, timeout=1.0)
            )
        except Exception:
            LOGGER.debug("WS sitemap_request dispatch failed", exc_info=True)

    return JSONResponse(
        {
            "ok": True,
            "changed": True,
            "nudged": should_nudge,
            "normalized_name": desired,
        }
    )


@app.post("/api/categories/customize", response_class=JSONResponse)
async def api_categories_customize(payload: dict = Body(...)):
    """
    Set or clear a category's custom display name.
    """

    import unicodedata

    def _norm_display(s):
        if s is None:
            return None
        s = unicodedata.normalize("NFKC", str(s)).strip()
        return s if s else None

    ocid = None
    if "original_category_id" in payload:
        try:
            ocid = int(payload.get("original_category_id"))
        except Exception:
            return JSONResponse(
                {"ok": False, "error": "invalid-original_category_id"}, status_code=400
            )
    else:
        name = _norm_display(payload.get("category_name"))
        ocid = db.resolve_original_category_id_by_name(name) if name else None
        if not ocid:
            return JSONResponse(
                {"ok": False, "error": "missing-or-unresolvable-category"},
                status_code=400,
            )

    desired = _norm_display(
        payload.get("custom_category_name", payload.get("clone_category_name"))
    )

    try:
        orig = db.get_original_category_name(ocid)
    except Exception:
        orig = None

    same_as_original = False
    if desired is not None and _norm_display(orig) == _norm_display(desired):
        desired = None
        same_as_original = True

    try:
        current_raw = db.get_clone_category_name(ocid)
    except Exception:
        current_raw = None

    needs_update = _norm_display(current_raw) != _norm_display(desired)
    if not needs_update:
        LOGGER.info(
            "Customize category | original_id=%s no change (kept=%r)", ocid, current_raw
        )
        return JSONResponse(
            {"ok": True, "changed": False, "normalized": desired is not None}
        )

    try:
        db.set_category_clone_name(ocid, desired)
        LOGGER.info(
            "Customize category | original_id=%s updated to %r (orig=%r, was=%r)",
            ocid,
            desired,
            orig,
            current_raw,
        )
    except Exception as e:
        LOGGER.exception("Failed to set cloned_category_name: %s", e)
        return JSONResponse({"ok": False, "error": "db-failure"}, status_code=500)

    should_nudge = not (desired is None and same_as_original)
    if should_nudge:
        try:
            asyncio.create_task(
                _ws_cmd(CLIENT_AGENT_URL, {"type": "sitemap_request"}, timeout=1.0)
            )
        except Exception:
            LOGGER.debug("WS sitemap_request dispatch failed", exc_info=True)

    return JSONResponse(
        {
            "ok": True,
            "changed": True,
            "nudged": should_nudge,
            "normalized_name": desired,
        }
    )


@app.get("/version")
def get_version():
    current = CURRENT_VERSION or db.get_version()
    latest = db.get_config("latest_tag", "")
    url = db.get_config("latest_url", "")

    def norm(v: str):
        import re

        v = (v or "").strip()
        if v.lower().startswith("v"):
            v = v[1:]
        v = re.sub(r"[^0-9.]", "", v)
        parts = [p for p in v.split(".") if p.isdigit()]
        while len(parts) < 3:
            parts.append("0")
        return ".".join(parts[:3])

    ca = tuple(int(x) for x in norm(current).split("."))
    lb = tuple(int(x) for x in norm(latest).split(".")) if latest else (0, 0, 0)

    return {
        "current": current,
        "latest": latest or current,
        "url": url
        or f"https://github.com/Copycord/Copycord/releases/tag/{latest or current}",
        "update_available": bool(latest) and (lb > ca),
    }


async def _fetch_latest_release(session: aiohttp.ClientSession) -> dict | None:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "copycord-app",
    }

    etag = db.get_config("gh_releases_etag", "")
    if etag:
        headers["If-None-Match"] = etag

    url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
    async with session.get(url, headers=headers, timeout=20) as r:
        if r.status == 304:
            return None
        r.raise_for_status()
        data = await r.json()
        new_etag = r.headers.get("ETag") or ""
        if new_etag and new_etag != etag:
            db.set_config("gh_releases_etag", new_etag)

    tag = data.get("tag_name")
    html_url = data.get("html_url")
    published_at = data.get("published_at")
    if not tag or not html_url:
        return None
    return {"tag": tag, "url": html_url, "published_at": published_at}


async def _release_watch_loop():
    await asyncio.sleep(2)
    LOGGER.debug("Starting GitHub release watcher for %s", GITHUB_REPO)
    async with aiohttp.ClientSession() as session:
        while not shutdown_event.is_set():
            try:
                try:
                    recorded_ver = db.get_version()
                    if recorded_ver != CURRENT_VERSION:
                        db.set_version(CURRENT_VERSION)
                except AttributeError:
                    recorded_ver = db.get_config("current_version", "")
                    if recorded_ver != CURRENT_VERSION:
                        db.set_config("current_version", CURRENT_VERSION)

                rel = await _fetch_latest_release(session)
                if rel:
                    prev = db.get_config("latest_tag", "")
                    if rel["tag"] != prev:
                        db.set_config("latest_tag", rel["tag"])
                        db.set_config("latest_url", rel["url"])
                        if rel.get("published_at"):
                            db.set_config("latest_published_at", rel["published_at"])

                        LOGGER.info("Detected new release: %s", rel["tag"])
            except Exception:
                LOGGER.exception("release watcher error")

            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=RELEASE_POLL_SECONDS
                )
            except asyncio.TimeoutError:
                pass


@app.post("/api/export/messages", response_class=JSONResponse)
async def api_export_messages(request: Request):
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid-json"}, status_code=400)

    webhook = (payload.get("webhook_url") or "").strip() or None

    data = {
        "guild_id": payload.get("guild_id"),
        "channel_id": payload.get("channel_id"),
        "user_id": payload.get("user_id"),
        "webhook_url": webhook,
        "has_attachments": bool(payload.get("has_attachments", False)),
        "after_iso": payload.get("after_iso"),
        "before_iso": payload.get("before_iso"),
        "filters": payload.get("filters") or {},
    }

    try:
        res = await _ws_cmd(
            CLIENT_AGENT_URL,
            {"type": "export_messages", "data": data},
            timeout=CLIENT_AGENT_TIMEOUT,
        )
    except asyncio.TimeoutError:
        return JSONResponse(
            {"ok": False, "error": "client-agent-timeout"}, status_code=504
        )
    except ConnectionRefusedError:
        return JSONResponse(
            {"ok": False, "error": "client-agent-unreachable"}, status_code=502
        )
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"client-agent-error: {type(e).__name__}: {e}"},
            status_code=502,
        )

    if not res.get("ok", True):
        return JSONResponse(
            {"ok": False, "error": res.get("error") or "client-agent-failed"},
            status_code=502,
        )
    return JSONResponse({"ok": True, "accepted": True})


app = ConnCloseOnShutdownASGI(app)
