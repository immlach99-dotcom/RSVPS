import asyncio
import base64
import hmac
import html
import logging
import math
import os
import secrets
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from aiohttp import web
from fetcher import FetchError, check_subscription, fetch_config

from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    CopyTextButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

def _load_dotenv() -> None:
    """Read KEY=VALUE lines from a .env file next to this script (real env vars win)."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            v = v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
                v = v[1:-1]
            os.environ.setdefault(k.strip(), v)


_load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("vpnbot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_PASSWORD = os.environ["ADMIN_PASSWORD"]
SALES_CONTACT = os.environ.get("SALES_CONTACT", "")  # e.g. @youruser
DAILY_LIMIT = int(os.environ.get("DAILY_LIMIT", "1"))
CONFIG_TTL_HOURS = int(os.environ.get("CONFIG_TTL_HOURS", "24"))
_VOL = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
DB_PATH = os.environ.get("DB_PATH") or (os.path.join(_VOL, "bot.db") if _VOL else "bot.db")
MAX_ADMIN_FAILS = 5
ADMIN_LOCK_SECONDS = 15 * 60
GEN_COOLDOWN_S = int(os.environ.get("GEN_COOLDOWN_S", "300"))  # min gap between site runs
GEN_DAILY_MAX = int(os.environ.get("GEN_DAILY_MAX", "5"))  # personal-use cap per 24h
GEN_HEADLESS = os.environ.get("GEN_HEADLESS", "0") == "1"
GEN_PROXY = os.environ.get("GEN_PROXY") or None

CREDIT_NAME = os.environ.get("CREDIT_NAME", "Zero Day")
CREDIT_ID = os.environ.get("CREDIT_ID", "@rsoolmlach")
CREDIT_LINE = f"⚡️ ساخته‌شده توسط {CREDIT_NAME} • {CREDIT_ID}"
DEFAULT_COUNTRY = os.environ.get("DEFAULT_COUNTRY", "Canada")
GB_PER_CONFIG = float(os.environ.get("GB_PER_CONFIG", "2"))  # fixed by the site
MAX_CONFIGS_PER_PLAN = int(os.environ.get("MAX_CONFIGS_PER_PLAN", "4"))
WATCH_INTERVAL_S = int(os.environ.get("WATCH_INTERVAL_S", "600"))
RENEW_LEAD_MIN = int(os.environ.get("RENEW_LEAD_MIN", "120"))  # renew this long before expiry
RENEW_USAGE_PCT = float(os.environ.get("RENEW_USAGE_PCT", "90"))  # renew at this % used
TZ = ZoneInfo(os.environ.get("DISPLAY_TZ", "Asia/Tehran"))
MAX_PLAN_DAYS = int(os.environ.get("MAX_PLAN_DAYS", "30"))
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")  # e.g. https://sub.example.com
_RAILWAY_DOMAIN = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
if not PUBLIC_BASE_URL and _RAILWAY_DOMAIN:
    PUBLIC_BASE_URL = f"https://{_RAILWAY_DOMAIN}"
WEB_HOST = os.environ.get("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.environ.get("PORT") or os.environ.get("WEB_PORT", "8080"))

router = Router()
_gen_lock = asyncio.Lock()
_gen_times: list[float] = []
_fails: dict[int, tuple[int, float]] = {}
_tasks: set[asyncio.Task] = set()
_pstat_last: dict[int, float] = {}


class GenLimit(Exception):
    """Raised when the cooldown or the daily cap blocks a new run."""

    def __init__(self, msg: str, wait_s: int = 0):
        super().__init__(msg)
        self.wait_s = wait_s


# ---------------------------------------------------------------- database
def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    with closing(db()) as con:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS users(
                id INTEGER PRIMARY KEY, username TEXT, joined INTEGER);
            CREATE TABLE IF NOT EXISTS admins(id INTEGER PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS configs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country TEXT NOT NULL, link TEXT NOT NULL UNIQUE,
                added INTEGER NOT NULL, expires INTEGER NOT NULL,
                used_by INTEGER, used_ts INTEGER);
            CREATE TABLE IF NOT EXISTS claims(
                user_id INTEGER, ts INTEGER, config_id INTEGER);
            CREATE TABLE IF NOT EXISTS subs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sub_url TEXT NOT NULL UNIQUE, country TEXT NOT NULL,
                owner INTEGER NOT NULL, created INTEGER NOT NULL,
                est_expiry INTEGER NOT NULL,
                used_gib REAL, total_gib REAL, status TEXT, last_check INTEGER,
                active INTEGER NOT NULL DEFAULT 1,
                warned INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS plans(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner INTEGER NOT NULL, token TEXT NOT NULL UNIQUE,
                country TEXT NOT NULL, gb REAL NOT NULL, days INTEGER NOT NULL,
                started INTEGER NOT NULL, ends INTEGER NOT NULL,
                active INTEGER NOT NULL DEFAULT 1);
            """
        )
        for ddl in (
            "ALTER TABLE subs ADD COLUMN plan_id INTEGER",
            "ALTER TABLE subs ADD COLUMN config_link TEXT",
        ):
            try:
                con.execute(ddl)
            except sqlite3.OperationalError:
                pass  # column already there
        con.commit()


def is_admin(uid: int) -> bool:
    with closing(db()) as con:
        return con.execute("SELECT 1 FROM admins WHERE id=?", (uid,)).fetchone() is not None


def upsert_user(m: Message):
    with closing(db()) as con:
        con.execute(
            "INSERT INTO users(id,username,joined) VALUES(?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET username=excluded.username",
            (m.from_user.id, m.from_user.username, int(time.time())),
        )
        con.commit()


def stock_by_country():
    now = int(time.time())
    with closing(db()) as con:
        return con.execute(
            "SELECT country, COUNT(*) n FROM configs "
            "WHERE used_by IS NULL AND expires>? GROUP BY country ORDER BY country",
            (now,),
        ).fetchall()


def claims_today(uid: int) -> int:
    since = int(time.time()) - 86400
    with closing(db()) as con:
        return con.execute(
            "SELECT COUNT(*) FROM claims WHERE user_id=? AND ts>?", (uid, since)
        ).fetchone()[0]


def claim_config(uid: int, country: str):
    """Atomically reserve one unused, unexpired config for the user."""
    now = int(time.time())
    con = db()
    try:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute(
            "SELECT id, link FROM configs WHERE country=? AND used_by IS NULL "
            "AND expires>? ORDER BY id LIMIT 1",
            (country, now),
        ).fetchone()
        if not row:
            con.execute("ROLLBACK")
            return None
        con.execute(
            "UPDATE configs SET used_by=?, used_ts=? WHERE id=?", (uid, now, row["id"])
        )
        con.execute("INSERT INTO claims VALUES(?,?,?)", (uid, now, row["id"]))
        con.execute("COMMIT")
        return row["link"]
    finally:
        con.close()


# ---------------------------------------------------------------- keyboards
def main_kb(admin: bool = False):
    # Frosted "glass" look: one consistent 🧊/💎 icon set. Telegram draws inline buttons
    # semi-transparent over the chat wallpaper, so the rest comes from the client theme.
    rows = [
        [InlineKeyboardButton(text="🧊 دریافت کانفیگ رایگان • ۲ گیگ / ۱ روز", callback_data="free")],
        [
            InlineKeyboardButton(text="💎 خرید سرویس", callback_data="buy"),
            InlineKeyboardButton(text="🛠 گزارش مشکل", callback_data="report"),
        ],
        [InlineKeyboardButton(text="✨ درباره سازنده", callback_data="about")],
    ]
    if admin:
        rows.append([InlineKeyboardButton(text="⚙️ پنل ادمین", callback_data="adminpanel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def mine_kb():
    rows = [
        [
            InlineKeyboardButton(text="🧊 ۲ گیگ • ۱ روز", callback_data="plan:2:1"),
            InlineKeyboardButton(text="🧊 ۴ گیگ • ۵ روز", callback_data="plan:4:5"),
        ],
        [InlineKeyboardButton(text="🧊 ۶ گیگ • ۵ روز", callback_data="plan:6:5")],
        [
            InlineKeyboardButton(text="📦 پلن‌ها", callback_data="plans"),
            InlineKeyboardButton(text="📡 اشتراک‌ها", callback_data="subs"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------------------------------------------------------------- user side
@router.message(CommandStart())
async def start(m: Message):
    upsert_user(m)
    await m.answer(
        "سلام 👋\nبه ربات VPN خوش اومدی. یکی از گزینه‌ها رو انتخاب کن:\n\n" + CREDIT_LINE,
        reply_markup=main_kb(is_admin(m.from_user.id)),
    )


@router.callback_query(F.data == "about")
async def about(c: CallbackQuery):
    await c.message.edit_text(
        f"✨ <b>{html.escape(CREDIT_NAME)}</b>\n"
        f"سازنده و ادمین ربات: {html.escape(CREDIT_ID)}\n\n"
        "برای خرید، پشتیبانی یا ساخت ربات اختصاصی پیام بده.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ برگشت", callback_data="home")]]
        ),
    )
    await c.answer()


@router.callback_query(F.data == "home")
async def home(c: CallbackQuery):
    await c.message.edit_text("منوی اصلی:", reply_markup=main_kb(is_admin(c.from_user.id)))
    await c.answer()


@router.callback_query(F.data == "free")
async def free(c: CallbackQuery):
    if claims_today(c.from_user.id) >= DAILY_LIMIT and not is_admin(c.from_user.id):
        await c.answer("سهمیه امروزت تموم شده. فردا دوباره امتحان کن.", show_alert=True)
        return
    if not stock_by_country():
        await c.answer("فعلاً موجودی نداریم. بعداً سر بزن.", show_alert=True)
        return
    await c.message.edit_text("کشور مورد نظر رو انتخاب کن:", reply_markup=countries_kb())
    await c.answer()


@router.callback_query(F.data.startswith("c:"))
async def pick_country(c: CallbackQuery):
    uid = c.from_user.id
    if claims_today(uid) >= DAILY_LIMIT and not is_admin(uid):
        await c.answer("سهمیه امروزت تموم شده.", show_alert=True)
        return
    link = claim_config(uid, c.data[2:])
    if not link:
        await c.answer("موجودی این کشور تموم شد.", show_alert=True)
        await c.message.edit_reply_markup(reply_markup=countries_kb())
        return
    await c.message.answer(
        "✅ کانفیگ شما (۲ گیگ، اعتبار ۱ روز):\n\n"
        f"<code>{html.escape(link)}</code>\n\n"
        "روی متن بزن تا کپی بشه و داخل v2rayNG / Streisand / Hiddify وارد کن.\n\n"
        + CREDIT_LINE,
        parse_mode="HTML",
    )
    await c.answer("ارسال شد")


@router.callback_query(F.data == "buy")
async def buy(c: CallbackQuery):
    if SALES_CONTACT:
        txt = f"برای خرید سرویس با آیدی زیر در ارتباط باش:\n{SALES_CONTACT}"
    else:
        txt = "برای خرید با ادمین در ارتباط باش."
    await c.message.edit_text(
        txt,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ برگشت", callback_data="home")]]
        ),
    )
    await c.answer()


class Report(StatesGroup):
    text = State()


@router.callback_query(F.data == "report")
async def report(c: CallbackQuery, state: FSMContext):
    await state.set_state(Report.text)
    await c.message.answer("مشکلت رو در یک پیام بنویس (لغو: /cancel):")
    await c.answer()


@router.message(Command("cancel"))
async def cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("لغو شد.", reply_markup=main_kb(is_admin(m.from_user.id)))


@router.message(Report.text, F.text)
async def report_text(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    with closing(db()) as con:
        admins = [r["id"] for r in con.execute("SELECT id FROM admins")]
    who = f"@{m.from_user.username}" if m.from_user.username else str(m.from_user.id)
    for a in admins:
        try:
            await bot.send_message(a, f"🛠 گزارش مشکل از {who} ({m.from_user.id}):\n\n{m.text}")
        except Exception:
            log.exception("report forward failed")
    await m.answer("گزارشت ثبت شد، به‌زودی بررسی می‌کنیم. 🙏")


# ---------------------------------------------------------------- admin side
@router.message(Command("admin"))
async def admin_login(m: Message, command: CommandObject):
    uid = m.from_user.id
    try:
        await m.delete()  # remove the message containing the password
    except Exception:
        pass
    n, ts = _fails.get(uid, (0, 0.0))
    if n >= MAX_ADMIN_FAILS and time.time() - ts < ADMIN_LOCK_SECONDS:
        await m.answer("⛔️ تلاش بیش از حد. بعداً امتحان کن.")
        return
    pw = (command.args or "").strip()
    if pw and hmac.compare_digest(pw.encode(), ADMIN_PASSWORD.encode()):
        _fails.pop(uid, None)
        with closing(db()) as con:
            con.execute("INSERT OR IGNORE INTO admins VALUES(?)", (uid,))
            con.commit()
        await m.answer(ADMIN_HELP, parse_mode="HTML", reply_markup=main_kb(True))
    else:
        _fails[uid] = (n + 1 if time.time() - ts < ADMIN_LOCK_SECONDS else 1, time.time())
        await m.answer("رمز اشتباهه.")


ADMIN_HELP = (
    "✅ وارد شدی.\n\n"
    "<b>دستورات ادمین</b>\n"
    "/mine Canada 4 5 — پلن ۴ گیگ ۵ روزه (۲ کانفیگ) با لینک اشتراک اختصاصی ربات و تمدید خودکار\n"
    "/plans — کارت پلن‌های فعال (مصرف، تاریخ پایان، دکمه کپی)\n"
    "/watch &lt;ساب‌لینک&gt; [کشور] — زیر نظر گرفتن یک ساب‌لینک موجود\n"
    "/subs — وضعیت اشتراک‌های تحت نظر\n"
    "/unwatch &lt;id&gt; — توقف نظارت\n"
    "/gen Canada — گرفتن یک کانفیگ از سایت و ذخیره در انبار فروش\n"
    "/add Canada &lt;لینک&gt; — افزودن دستی کانفیگ (چند لینک در چند خط هم ممکنه)\n"
    "/stock — موجودی هر کشور\n"
    "/purge — حذف کانفیگ‌های منقضی\n"
    "/stats — آمار\n"
    "/broadcast متن — ارسال همگانی\n"
    "/logout — خروج"
)


@router.callback_query(F.data == "adminpanel")
async def admin_panel(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("دسترسی نداری", show_alert=True)
        return
    await c.message.answer(ADMIN_HELP, parse_mode="HTML", reply_markup=mine_kb())
    await c.answer()


def admin_only(fn):
    async def wrapper(m: Message, *a, **kw):
        if not is_admin(m.from_user.id):
            return
        return await fn(m, *a, **kw)
    wrapper.__name__ = fn.__name__
    return wrapper


@router.message(Command("add"))
@admin_only
async def add(m: Message, command: CommandObject):
    if not command.args:
        await m.answer("فرمت: /add Canada vless://...")
        return
    first, _, rest = command.args.partition(" ")
    country = first.strip()
    links = [x.strip() for x in rest.replace("\n", " ").split() if "://" in x]
    if not country or not links:
        await m.answer("فرمت: /add Canada vless://...")
        return
    now = int(time.time())
    added = 0
    with closing(db()) as con:
        for l in links:
            try:
                con.execute(
                    "INSERT INTO configs(country,link,added,expires) VALUES(?,?,?,?)",
                    (country, l, now, now + CONFIG_TTL_HOURS * 3600),
                )
                added += 1
            except sqlite3.IntegrityError:
                pass
        con.commit()
    await m.answer(f"{added} کانفیگ برای {country} اضافه شد.")


@router.message(Command("stock"))
@admin_only
async def stock(m: Message):
    rows = stock_by_country()
    await m.answer("\n".join(f"{r['country']}: {r['n']}" for r in rows) or "انبار خالیه.")


@router.message(Command("purge"))
@admin_only
async def purge(m: Message):
    with closing(db()) as con:
        n = con.execute("DELETE FROM configs WHERE expires<=?", (int(time.time()),)).rowcount
        con.commit()
    await m.answer(f"{n} کانفیگ منقضی حذف شد.")


@router.message(Command("stats"))
@admin_only
async def stats(m: Message):
    since = int(time.time()) - 86400
    with closing(db()) as con:
        users = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        today = con.execute("SELECT COUNT(*) FROM claims WHERE ts>?", (since,)).fetchone()[0]
        total = con.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
    await m.answer(f"کاربران: {users}\nدریافت امروز: {today}\nکل دریافت‌ها: {total}")


@router.message(Command("broadcast"))
@admin_only
async def broadcast(m: Message, command: CommandObject, bot: Bot):
    if not command.args:
        await m.answer("فرمت: /broadcast متن")
        return
    with closing(db()) as con:
        ids = [r["id"] for r in con.execute("SELECT id FROM users")]
    ok = 0
    for uid in ids:
        try:
            await bot.send_message(uid, command.args)
            ok += 1
        except Exception:
            pass
        await asyncio.sleep(0.05)
    await m.answer(f"ارسال شد به {ok} از {len(ids)} نفر.")


@router.message(Command("logout"))
@admin_only
async def logout(m: Message):
    with closing(db()) as con:
        con.execute("DELETE FROM admins WHERE id=?", (m.from_user.id,))
        con.commit()
    await m.answer("خارج شدی.")


# ---------------------------------------------------------------- generation core
GIB = 2**30


def fmt_dt(ts: float) -> str:
    return datetime.fromtimestamp(ts, TZ).strftime("%Y-%m-%d %H:%M")


def spawn(coro) -> None:
    t = asyncio.create_task(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


async def generate_one(country: str, wait: bool):
    """Run the site flow once.

    Serialised by a lock, spaced by GEN_COOLDOWN_S and capped at GEN_DAILY_MAX per 24h.
    wait=True sleeps out the cooldown (background jobs); wait=False raises GenLimit instead.
    Every attempt counts toward the cap, including failed ones.
    """
    async with _gen_lock:
        now = time.time()
        _gen_times[:] = [t for t in _gen_times if now - t < 86400]
        if len(_gen_times) >= GEN_DAILY_MAX:
            raise GenLimit(
                f"سقف روزانه ({GEN_DAILY_MAX}) پر شده.",
                wait_s=int(86400 - (now - _gen_times[0])),
            )
        if _gen_times:
            gap = GEN_COOLDOWN_S - (now - _gen_times[-1])
            if gap > 0:
                if not wait:
                    raise GenLimit(
                        f"باید {math.ceil(gap)} ثانیه دیگه صبر کنی.", wait_s=math.ceil(gap)
                    )
                await asyncio.sleep(gap)
        _gen_times.append(time.time())
        return await fetch_config(
            country=country, protocol="vless", headless=GEN_HEADLESS, proxy=GEN_PROXY
        )


def estimate_expiry(info, created_ts: float) -> float:
    """Best expiry guess. The page shows a date only, so prefer the relative header
    ("Expires in 5 hours") once it is more precise than the coarse "in a day" bucket."""
    if info is not None and info.rel_minutes is not None and info.rel_minutes < 1380:
        return time.time() + info.rel_minutes * 60
    return created_ts + CONFIG_TTL_HOURS * 3600


def register_sub(
    owner: int, sub_url: str, country: str, est_expiry: float, info=None,
    plan_id=None, config_link=None,
) -> None:
    now = int(time.time())
    with closing(db()) as con:
        con.execute(
            "INSERT OR IGNORE INTO subs(sub_url,country,owner,created,est_expiry,"
            "used_gib,total_gib,status,last_check,plan_id,config_link) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                sub_url, country, owner, now, int(est_expiry),
                info.used_gib if info else None,
                info.total_gib if info else None,
                info.status if info else None,
                now if info else None,
                plan_id, config_link,
            ),
        )
        con.commit()


def sub_message(title, country, link, sub_url, info, est_expiry) -> str:
    total = info.total_gib if info and info.total_gib else GB_PER_CONFIG
    site_date = info.expires_on.isoformat() if info and info.expires_on else "—"
    lines = [
        f"{title} • {html.escape(country)}",
        f"📅 تاریخ انقضا (طبق سایت): {site_date}",
        f"⏳ انقضای تقریبی: {fmt_dt(est_expiry)}",
        f"📦 حجم: {total:.2f} GiB",
    ]
    if link:
        lines += ["", f"<code>{html.escape(link)}</code>"]
    if sub_url and sub_url != link:
        lines += ["", "🔗 لینک اشتراک سایت:", f"<code>{html.escape(sub_url)}</code>"]
    lines += ["", CREDIT_LINE]
    return "\n".join(lines)


def config_kb(link):
    """Telegram-side copy button for a single config (copy_text is capped at 256 chars)."""
    if link and len(link) <= 256:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📋 کپی کانفیگ", copy_text=CopyTextButton(text=link))]
            ]
        )
    return None


async def issue_config(
    bot: Bot, owner: int, country: str, wait: bool, title: str, plan_id=None, note: str = ""
) -> None:
    """Generate one personal config, register it for watching and send it to the owner."""
    res = await generate_one(country, wait)
    sub_url = res.subscription_url
    link = res.config_link or sub_url
    if not link:
        raise FetchError("no config returned")
    if plan_id and not (sub_url and res.config_link):
        raise FetchError("config or subscription link missing; cannot add it to the plan")
    info = None
    if sub_url:
        try:
            info = await check_subscription(sub_url, headless=GEN_HEADLESS, proxy=GEN_PROXY)
        except Exception:
            log.warning("post-generation subscription check failed", exc_info=True)
    est = estimate_expiry(info, time.time())
    if sub_url:
        register_sub(owner, sub_url, country, est, info, plan_id, res.config_link)
    text = sub_message(title, country, res.config_link, sub_url, info, est)
    if note:
        text += "\n" + note
    await bot.send_message(owner, text, parse_mode="HTML", reply_markup=config_kb(res.config_link))


# ---------------------------------------------------------------- plans + own subscription link
def sub_link_for(token: str):
    return f"{PUBLIC_BASE_URL}/sub/{token}" if PUBLIC_BASE_URL else None


def bar(frac: float, width: int = 10) -> str:
    n = round(max(0.0, min(1.0, frac)) * width)
    return "▰" * n + "▱" * (width - n)


def left_text(seconds: float) -> str:
    if seconds <= 0:
        return "تمام شده"
    d, rem = divmod(int(seconds), 86400)
    h, m = rem // 3600, (rem % 3600) // 60
    return f"{d} روز و {h} ساعت" if d else f"{h} ساعت و {m} دقیقه"


def get_plan(pid):
    if not pid:
        return None
    with closing(db()) as con:
        return con.execute("SELECT * FROM plans WHERE id=?", (pid,)).fetchone()


def create_plan(owner: int, country: str, gb: float, days: int):
    now = int(time.time())
    token = secrets.token_urlsafe(18)
    with closing(db()) as con:
        cur = con.execute(
            "INSERT INTO plans(owner,token,country,gb,days,started,ends) VALUES(?,?,?,?,?,?,?)",
            (owner, token, country, gb, days, now, now + days * 86400),
        )
        con.commit()
        pid = cur.lastrowid
    return get_plan(pid)


def plan_stats(pid: int):
    with closing(db()) as con:
        rows = con.execute("SELECT * FROM subs WHERE plan_id=?", (pid,)).fetchall()
    active = [r for r in rows if r["active"]]
    cur_used = sum(r["used_gib"] or 0 for r in active)
    cur_total = sum((r["total_gib"] or GB_PER_CONFIG) for r in active)
    all_used = sum(r["used_gib"] or 0 for r in rows)
    return active, cur_used, cur_total, all_used


def plan_text(plan) -> str:
    active, cur_used, cur_total, all_used = plan_stats(plan["id"])
    frac = cur_used / cur_total if cur_total else 0.0
    link = sub_link_for(plan["token"])
    state = "فعال" if plan["active"] else "پایان‌یافته"
    lines = [
        f"🧊 <b>پلن {plan['gb']:g} گیگ • {plan['days']} روز</b> • "
        f"{html.escape(plan['country'])} • {state}",
        "",
        f"📊 مصرف فعلی: {bar(frac)}  {cur_used:.2f} / {cur_total:.2f} GiB",
        f"📈 مجموع مصرف از شروع پلن: {all_used:.2f} GiB",
        f"⏳ پایان پلن: {fmt_dt(plan['ends'])} • مانده: {left_text(plan['ends'] - time.time())}",
        f"🔁 کانفیگ فعال: {len(active)}",
        "",
    ]
    if link:
        lines += ["🔗 لینک اشتراک ربات (ثابت می‌مونه، خودکار به‌روز می‌شه):", f"<code>{html.escape(link)}</code>"]
    else:
        lines += ["⚠️ PUBLIC_BASE_URL تنظیم نشده؛ لینک اشتراک ربات ساخته نمی‌شه."]
    lines += ["", CREDIT_LINE]
    return "\n".join(lines)


def plan_kb(plan) -> InlineKeyboardMarkup:
    rows = []
    link = sub_link_for(plan["token"])
    if link and len(link) <= 256:
        rows.append(
            [InlineKeyboardButton(text="📋 کپی لینک اشتراک", copy_text=CopyTextButton(text=link))]
        )
    rows.append([InlineKeyboardButton(text="🔄 به‌روزرسانی مصرف", callback_data=f"pstat:{plan['id']}")])
    active, *_ = plan_stats(plan["id"])
    for i, r in enumerate(active, 1):
        cl = r["config_link"]
        if cl and len(cl) <= 256:
            rows.append(
                [InlineKeyboardButton(text=f"📋 کانفیگ {i}", copy_text=CopyTextButton(text=cl))]
            )
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def refresh_plan_usage(pid: int) -> None:
    with closing(db()) as con:
        rows = con.execute("SELECT * FROM subs WHERE plan_id=? AND active=1", (pid,)).fetchall()

    async def one(r):
        try:
            info = await check_subscription(r["sub_url"], headless=GEN_HEADLESS, proxy=GEN_PROXY)
        except Exception:
            return
        with closing(db()) as con:
            con.execute(
                "UPDATE subs SET used_gib=?, total_gib=?, status=?, last_check=? WHERE id=?",
                (info.used_gib, info.total_gib, info.status, int(time.time()), r["id"]),
            )
            con.commit()

    await asyncio.gather(*(one(r) for r in rows))


async def finish_plan_if_done(bot: Bot, pid: int) -> None:
    plan = get_plan(pid)
    if not plan or not plan["active"]:
        return
    active, _, _, all_used = plan_stats(pid)
    if active or plan["ends"] > time.time():
        return
    with closing(db()) as con:
        con.execute("UPDATE plans SET active=0 WHERE id=?", (pid,))
        con.commit()
    await bot.send_message(
        plan["owner"],
        f"✅ پلن {plan['gb']:g} گیگ • {plan['days']} روزه تموم شد.\n"
        f"📈 مجموع مصرف: {all_used:.2f} GiB\n\n{CREDIT_LINE}",
    )


async def run_plan(bot: Bot, owner: int, country: str, gb: float, days: int) -> None:
    wanted = max(1, math.ceil(gb / GB_PER_CONFIG))
    n = min(wanted, MAX_CONFIGS_PER_PLAN)
    note = f" (سقف {MAX_CONFIGS_PER_PLAN} کانفیگ)" if n < wanted else ""
    plan = create_plan(owner, country, gb, days)
    await bot.send_message(
        owner,
        f"🧊 پلن {gb:g} گیگ • {days} روز: {n} کانفیگ {country}{note}.\n"
        f"بین هر کانفیگ حدود {max(1, GEN_COOLDOWN_S // 60)} دقیقه فاصله می‌افته.",
    )
    done = 0
    for i in range(n):
        try:
            await issue_config(
                bot, owner, country, wait=True, title=f"✅ کانفیگ {i + 1} از {n}",
                plan_id=plan["id"],
            )
            done += 1
        except GenLimit as e:
            await bot.send_message(owner, f"⛔️ {e}")
            break
        except FetchError as e:
            await bot.send_message(owner, f"❌ {e}")
            break
        except Exception:
            log.exception("plan step failed")
            await bot.send_message(owner, "❌ خطای غیرمنتظره، لاگ سرور رو ببین.")
            break
    if done == 0:
        with closing(db()) as con:
            con.execute("UPDATE plans SET active=0 WHERE id=?", (plan["id"],))
            con.commit()
        await bot.send_message(owner, "هیچ کانفیگی ساخته نشد؛ پلن لغو شد.")
        return
    plan = get_plan(plan["id"])
    await bot.send_message(owner, plan_text(plan), parse_mode="HTML", reply_markup=plan_kb(plan))


def parse_mine_args(args: str):
    """Returns (country, gb, days). Numbers are read in order: first GB, then days."""
    country, nums = DEFAULT_COUNTRY, []
    for tok in (args or "").split():
        raw = tok.lower()
        for suf in ("gib", "gb", "days", "day", "d", "روز"):
            if raw.endswith(suf):
                raw = raw[: -len(suf)]
                break
        try:
            nums.append(float(raw))
        except ValueError:
            country = tok
    gb = nums[0] if nums else GB_PER_CONFIG
    days = int(nums[1]) if len(nums) > 1 else 1
    return country, gb, days


@router.message(Command("mine"))
@admin_only
async def mine(m: Message, command: CommandObject, bot: Bot):
    country, gb, days = parse_mine_args(command.args)
    if not (0 < gb <= 100 and 1 <= days <= MAX_PLAN_DAYS):
        await m.answer(f"مقدار نامعتبره. مثال: /mine Canada 4 5  (حداکثر {MAX_PLAN_DAYS} روز)")
        return
    spawn(run_plan(bot, m.from_user.id, country, gb, days))
    await m.answer("شروع شد. نتیجه هر کانفیگ و بعدش کارت پلن برات میاد.")


@router.callback_query(F.data.startswith("plan:"))
async def plan_cb(c: CallbackQuery, bot: Bot):
    if not is_admin(c.from_user.id):
        await c.answer("دسترسی نداری", show_alert=True)
        return
    _, gb, days = c.data.split(":")
    spawn(run_plan(bot, c.from_user.id, DEFAULT_COUNTRY, float(gb), int(days)))
    await c.answer("شروع شد")


async def send_plan_cards(target: Message, owner: int) -> None:
    with closing(db()) as con:
        plans = con.execute(
            "SELECT * FROM plans WHERE active=1 AND owner=? ORDER BY id DESC LIMIT 5", (owner,)
        ).fetchall()
    if not plans:
        await target.answer("پلن فعالی نداری. مثال: /mine Canada 4 5")
        return
    for p in plans:
        await target.answer(plan_text(p), parse_mode="HTML", reply_markup=plan_kb(p))


@router.message(Command("plans"))
@admin_only
async def plans_cmd(m: Message):
    await send_plan_cards(m, m.from_user.id)


@router.callback_query(F.data == "plans")
async def plans_cb(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("دسترسی نداری", show_alert=True)
        return
    await send_plan_cards(c.message, c.from_user.id)
    await c.answer()


@router.callback_query(F.data.startswith("pstat:"))
async def pstat_cb(c: CallbackQuery):
    plan = get_plan(int(c.data[6:]))
    if not plan or plan["owner"] != c.from_user.id or not is_admin(c.from_user.id):
        await c.answer("دسترسی نداری", show_alert=True)
        return
    now = time.time()
    if now - _pstat_last.get(plan["id"], 0) > 30:
        _pstat_last[plan["id"]] = now
        await c.answer("در حال خوندن مصرف ...")
        await refresh_plan_usage(plan["id"])
    else:
        await c.answer("همین الان به‌روز شده.")
    plan = get_plan(plan["id"])
    try:
        await c.message.edit_text(plan_text(plan), parse_mode="HTML", reply_markup=plan_kb(plan))
    except TelegramBadRequest:
        pass  # content unchanged


async def sub_handler(request: web.Request) -> web.Response:
    """Serve the plan's currently active configs as a standard base64 subscription."""
    token = request.match_info["token"]
    with closing(db()) as con:
        plan = con.execute("SELECT * FROM plans WHERE token=?", (token,)).fetchone()
        if not plan:
            raise web.HTTPNotFound()
        rows = con.execute(
            "SELECT config_link, used_gib, total_gib FROM subs "
            "WHERE plan_id=? AND active=1 AND config_link IS NOT NULL ORDER BY id",
            (plan["id"],),
        ).fetchall()
    body = base64.b64encode("\n".join(r["config_link"] for r in rows).encode()).decode()
    used = sum(r["used_gib"] or 0 for r in rows)
    total = sum(r["total_gib"] or GB_PER_CONFIG for r in rows)
    title = f"{CREDIT_NAME} {plan['gb']:g}GB {plan['days']}d"
    if not title.isascii():
        title = "base64:" + base64.b64encode(title.encode()).decode()
    headers = {
        # standard header: client apps draw a usage bar and the plan end date from it
        "Subscription-Userinfo": (
            f"upload=0; download={int(used * GIB)}; total={int(total * GIB)}; expire={int(plan['ends'])}"
        ),
        "Profile-Title": title,
        "Profile-Update-Interval": "1",
        "Cache-Control": "no-store",
    }
    return web.Response(text=body, content_type="text/plain", headers=headers)


async def start_web() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/sub/{token}", sub_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, WEB_HOST, WEB_PORT).start()
    log.info("subscription endpoint on %s:%s", WEB_HOST, WEB_PORT)
    return runner


# ---------------------------------------------------------------- watching
def subs_text(owner: int) -> str:
    with closing(db()) as con:
        rows = con.execute(
            "SELECT * FROM subs WHERE active=1 AND owner=? ORDER BY id", (owner,)
        ).fetchall()
    if not rows:
        return "هیچ اشتراکی تحت نظر نیست."
    out = []
    for r in rows:
        if r["total_gib"]:
            vol = f"{(r['used_gib'] or 0):.2f}/{r['total_gib']:.2f} GiB"
        else:
            vol = "حجم نامشخص"
        out.append(
            f"#{r['id']} • {html.escape(r['country'])} • {vol} • "
            f"{html.escape(r['status'] or '?')} • تا {fmt_dt(r['est_expiry'])}"
        )
    return "\n".join(out)


@router.message(Command("subs"))
@admin_only
async def subs_cmd(m: Message):
    await m.answer(subs_text(m.from_user.id), parse_mode="HTML")


@router.callback_query(F.data == "subs")
async def subs_cb(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("دسترسی نداری", show_alert=True)
        return
    await c.message.answer(subs_text(c.from_user.id), parse_mode="HTML")
    await c.answer()


@router.message(Command("watch"))
@admin_only
async def watch(m: Message, command: CommandObject):
    parts = (command.args or "").split()
    if not parts or not parts[0].startswith("https://sub.vpnserverhub.com/"):
        await m.answer("فرمت: /watch https://sub.vpnserverhub.com/... [Canada]")
        return
    url = parts[0]
    country = parts[1] if len(parts) > 1 else DEFAULT_COUNTRY
    await m.answer("در حال خوندن صفحه اشتراک ...")
    try:
        info = await check_subscription(url, headless=GEN_HEADLESS, proxy=GEN_PROXY)
    except FetchError as e:
        await m.answer(f"❌ {e}")
        return
    est = estimate_expiry(info, time.time())
    register_sub(m.from_user.id, url, country, est, info)
    await m.answer(
        sub_message("👁 تحت نظر گرفته شد", country, None, None, info, est)
        + f"\nوضعیت: {html.escape(info.status or '?')} • مصرف: {(info.used_gib or 0):.2f} GiB",
        parse_mode="HTML",
    )


@router.message(Command("unwatch"))
@admin_only
async def unwatch(m: Message, command: CommandObject):
    try:
        sid = int((command.args or "").strip())
    except ValueError:
        await m.answer("فرمت: /unwatch <id>")
        return
    with closing(db()) as con:
        n = con.execute(
            "UPDATE subs SET active=0 WHERE id=? AND owner=?", (sid, m.from_user.id)
        ).rowcount
        con.commit()
    await m.answer("متوقف شد." if n else "پیدا نشد.")


REASON_FA = {
    "expiry": "نزدیک انقضا",
    "usage": "حجم رو به اتمام",
    "status": "اشتراک غیرفعال شد",
}


def due_reason(status, used, total, est_expiry, now):
    if status and status.lower() != "active":
        return "status"
    if total and used is not None and used / total * 100 >= RENEW_USAGE_PCT:
        return "usage"
    if est_expiry - now <= RENEW_LEAD_MIN * 60:
        return "expiry"
    return None


async def watch_tick(bot: Bot) -> None:
    with closing(db()) as con:
        rows = con.execute("SELECT * FROM subs WHERE active=1").fetchall()
    for row in rows:
        try:
            info = await check_subscription(row["sub_url"], headless=GEN_HEADLESS, proxy=GEN_PROXY)
        except Exception as e:
            log.warning("check failed for sub #%s: %s", row["id"], e)
            continue
        now = time.time()
        est = row["est_expiry"]
        if info.rel_minutes is not None and info.rel_minutes < 1380:
            est = now + info.rel_minutes * 60
        with closing(db()) as con:
            con.execute(
                "UPDATE subs SET used_gib=?, total_gib=?, status=?, est_expiry=?, last_check=? WHERE id=?",
                (info.used_gib, info.total_gib, info.status, int(est), int(now), row["id"]),
            )
            con.commit()
        reason = due_reason(info.status, info.used_gib, info.total_gib, est, now)
        plan = get_plan(row["plan_id"])
        if plan and plan["ends"] <= now:
            # the plan is over: let this config run out, do not renew
            if reason == "status" or est <= now:
                with closing(db()) as con:
                    con.execute("UPDATE subs SET active=0 WHERE id=?", (row["id"],))
                    con.commit()
                await finish_plan_if_done(bot, plan["id"])
            continue
        if not reason:
            continue
        try:
            await issue_config(
                bot, row["owner"], row["country"], wait=True,
                title=f"🔄 جایگزین جدید ({REASON_FA[reason]})",
                plan_id=row["plan_id"],
                note="🔗 لینک اشتراک ربات خودکار به‌روز شد؛ توی اپ آپدیت اشتراک رو بزن." if plan else "",
            )
        except (GenLimit, FetchError) as e:
            log.warning("renewal of sub #%s blocked: %s", row["id"], e)
            if not row["warned"]:
                with closing(db()) as con:
                    con.execute("UPDATE subs SET warned=1 WHERE id=?", (row["id"],))
                    con.commit()
                await bot.send_message(
                    row["owner"],
                    f"⚠️ اشتراک #{row['id']} ({REASON_FA[reason]}) ولی تمدید نشد: {e}\n"
                    "ربات هر دوره دوباره تلاش می‌کنه.",
                )
            continue
        except Exception:
            log.exception("renewal failed for sub #%s", row["id"])
            continue
        with closing(db()) as con:
            con.execute("UPDATE subs SET active=0 WHERE id=?", (row["id"],))
            con.commit()


async def watch_loop(bot: Bot) -> None:
    await asyncio.sleep(30)
    while True:
        try:
            await watch_tick(bot)
        except Exception:
            log.exception("watch tick failed")
        await asyncio.sleep(WATCH_INTERVAL_S)


# ---------------------------------------------------------------- stock generation
@router.message(Command("gen"))
@admin_only
async def gen(m: Message, command: CommandObject):
    country = (command.args or DEFAULT_COUNTRY).strip()
    if _gen_lock.locked():
        await m.answer("یک اجرا در حال انجامه.")
        return
    await m.answer(f"در حال گرفتن کانفیگ {country} ... (تا ~۲ دقیقه)")
    try:
        res = await generate_one(country, wait=False)
    except GenLimit as e:
        await m.answer(f"⛔️ {e}")
        return
    except FetchError as e:
        await m.answer(f"❌ {e}")
        return
    except Exception:
        log.exception("gen failed")
        await m.answer("❌ خطای غیرمنتظره، لاگ سرور رو ببین.")
        return

    link = res.config_link or res.subscription_url
    if not link:
        await m.answer("❌ کانفیگی پیدا نشد.")
        return
    ts = int(time.time())
    with closing(db()) as con:
        try:
            con.execute(
                "INSERT INTO configs(country,link,added,expires) VALUES(?,?,?,?)",
                (country, link, ts, ts + CONFIG_TTL_HOURS * 3600),
            )
            con.commit()
        except sqlite3.IntegrityError:
            await m.answer("این کانفیگ قبلاً تو انبار بوده.")
            return
    kind = "کانفیگ کامل" if res.config_link else "لینک اشتراک"
    await m.answer(f"✅ {kind} به انبار {country} اضافه شد.")


# ---------------------------------------------------------------- main
async def main():
    init_db()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    runner = await start_web()
    spawn(watch_loop(bot))
    try:
        await dp.start_polling(bot)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
