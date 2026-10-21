"""Fetch one free V2Ray config from vpnserverhub.com using a plain, visible browser.

Design rules:
- The Cloudflare Turnstile widget is never touched. We only wait for the site's own
  widget to finish and publish its token. No solver, no stealth patches, no spoofing.
- If the widget does not pass within TURNSTILE_TIMEOUT_S, the run aborts with an error.
- One run produces at most one config. Rate limiting is enforced by the caller.
"""
import asyncio
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

from playwright.async_api import TimeoutError as PWTimeout
from playwright.async_api import async_playwright

SITE_URL = "https://vpnserverhub.com/free-v2ray"
TURNSTILE_TIMEOUT_S = 60
STEP_TIMEOUT_MS = 20_000

LINK_RE = re.compile(r"(?:vless|vmess|trojan|ss)://[^\s\"'<>]+")


class FetchError(Exception):
    """Raised with a short, human-readable reason when a step fails."""


@dataclass
class FetchResult:
    config_link: Optional[str]
    subscription_url: Optional[str]


def _clean(link: str) -> str:
    return link.strip().rstrip(".,;)")


SUB_RE = re.compile(r"https?://sub\.vpnserverhub\.com/[^\s\"'<>]+")


async def _via_button(page, selector: str, pattern) -> Optional[str]:
    """Click a copy button and read what it put on the clipboard (None if unreadable)."""
    try:
        await page.evaluate("() => navigator.clipboard.writeText('')")  # drop stale value
        await page.locator(selector).first.click()
        await page.wait_for_timeout(300)
        txt = await page.evaluate("() => navigator.clipboard.readText()")
    except Exception:
        return None
    m = pattern.search(txt or "")
    return _clean(m.group(0)) if m else None


async def _scan_dom(page, pattern) -> Optional[str]:
    """Fallback: look for a match in field values, text blocks and link targets."""
    blobs = await page.evaluate(
        """() => {
            const out = [];
            document.querySelectorAll('textarea, input, pre, code, a[href]').forEach(e => {
                out.push(e.value || e.innerText || '');
                if (e.href) out.push(e.href);
            });
            return out;
        }"""
    )
    for b in blobs:
        m = pattern.search(b or "")
        if m:
            return _clean(m.group(0))
    return None


async def fetch_config(
    country: str = "Canada",
    protocol: str = "vless",
    headless: bool = False,
    proxy: Optional[str] = None,
) -> FetchResult:
    """Run the page flow once and return the config.

    headless=False is the default on purpose: the widget decides for itself,
    and a normal headed browser is what the site expects. On a server without a
    display, run under `xvfb-run -a python bot.py`.
    """
    async with async_playwright() as pw:
        launch_kwargs = {"headless": headless}
        if proxy:
            launch_kwargs["proxy"] = {"server": proxy}
        browser = await pw.chromium.launch(**launch_kwargs)
        try:
            ctx = await browser.new_context(
                viewport={"width": 1280, "height": 900},
                permissions=["clipboard-read", "clipboard-write"],
            )
            page = await ctx.new_page()
            page.set_default_timeout(STEP_TIMEOUT_MS)

            try:
                await page.goto(SITE_URL, wait_until="load")
            except PWTimeout:
                raise FetchError("site did not load")

            # 1) country
            card = page.locator(".country-card").filter(
                has=page.get_by_text(country, exact=True)
            )
            if await card.count() == 0:
                raise FetchError(f"country '{country}' not found on page")
            await card.first.click()

            # 2) protocol
            proto = page.locator(f'.protocol-card[data-protocol="{protocol}"]')
            if await proto.count() == 0:
                raise FetchError(f"protocol '{protocol}' not found on page")
            await proto.first.click()

            # 3) wait for the widget to finish by itself (token appears in hidden input)
            try:
                await page.wait_for_function(
                    """() => {
                        const el = document.querySelector('input[name="cf-turnstile-response"]');
                        return !!(el && el.value && el.value.length > 10);
                    }""",
                    timeout=TURNSTILE_TIMEOUT_S * 1000,
                )
            except PWTimeout:
                raise FetchError(
                    "Cloudflare check did not pass on its own; aborted (no bypass attempted)"
                )

            # 4) generate
            await page.locator("#createConfig").click()

            # 5) result card with the subscription URL
            try:
                await page.wait_for_selector("#configResult #configUrl", timeout=40_000)
            except PWTimeout:
                raise FetchError("config was not generated (site refused or limit reached)")
            sub_url = await page.locator("#configUrl").first.get_attribute(
                "data-subscription_url"
            )

            # 6) open the details. Button ids are stable, config text is not, so the
            #    copy buttons are the primary source and DOM scanning is the fallback.
            await page.locator("#configUrl").first.click()
            try:
                await page.wait_for_selector("#copyDecodedConfig", timeout=15_000)
            except PWTimeout:
                # details panel did not open; subscription URL alone is still usable
                return FetchResult(config_link=None, subscription_url=sub_url)

            link = await _via_button(page, "#copyDecodedConfig", LINK_RE)
            if not link:
                link = await _scan_dom(page, LINK_RE)
            sub_full = None
            if await page.locator("#copyOriginalLink").count():
                sub_full = await _via_button(page, "#copyOriginalLink", SUB_RE)
            if not sub_full:
                sub_full = await _scan_dom(page, SUB_RE)
            return FetchResult(config_link=link, subscription_url=sub_full or sub_url)
        finally:
            await browser.close()


# ------------------------------------------------------------------ subscription page
@dataclass
class SubInfo:
    username: Optional[str]
    status: Optional[str]
    expires_on: Optional[date]  # date only; the page shows no time of day
    used_gib: Optional[float]
    total_gib: Optional[float]
    rel_minutes: Optional[int]  # from "Expires in ..." header; None if unreadable


_UNIT_GIB = {"kib": 1 / 1024**2, "kb": 1 / 1024**2, "mib": 1 / 1024, "mb": 1 / 1024,
             "gib": 1.0, "gb": 1.0, "tib": 1024.0, "tb": 1024.0}
_BW_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(KiB|KB|MiB|MB|GiB|GB|TiB|TB)?\s*/\s*(\d+(?:\.\d+)?)\s*(KiB|KB|MiB|MB|GiB|GB|TiB|TB)",
    re.I,
)
_DATE_RE = re.compile(r"Expires\s+(\d{1,2})\s+([A-Za-z]+),?\s+(\d{4})")
_REL_RE = re.compile(
    r"Expires\s+in\s+(a few|an?|\d+)\s+(second|minute|hour|day|week|month)s?", re.I
)
_UNIT_MIN = {"second": 0, "minute": 1, "hour": 60, "day": 1440, "week": 10080, "month": 43200}


def _to_gib(value: str, unit: Optional[str], fallback_unit: str) -> float:
    return float(value) * _UNIT_GIB[(unit or fallback_unit).lower()]


def parse_sub_text(text: str) -> SubInfo:
    """Parse the visible text of a subscription info page. Missing fields stay None."""
    t = re.sub(r"[ \t\r\f\v]+", " ", text or "")

    used = total = None
    m = _BW_RE.search(t)
    if m:
        total_unit = m.group(4)
        used = _to_gib(m.group(1), m.group(2), total_unit)
        total = _to_gib(m.group(3), total_unit, total_unit)

    exp_date = None
    m = _DATE_RE.search(t)
    if m:
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                exp_date = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", fmt).date()
                break
            except ValueError:
                continue

    rel = None
    m = _REL_RE.search(t)
    if m:
        n = m.group(1).lower()
        count = 1 if n in ("a", "an") else (0 if n == "a few" else int(n))
        rel = count * _UNIT_MIN[m.group(2).lower()]
    elif re.search(r"\bExpired\b", t):
        rel = 0

    status = None
    m = re.search(r"Status\s+([A-Za-z]+)", t)
    if m:
        status = m.group(1)
    username = None
    m = re.search(r"Username\s+(\S+)", t)
    if m:
        username = m.group(1)

    return SubInfo(username, status, exp_date, used, total, rel)


async def check_subscription(
    url: str, headless: bool = False, proxy: Optional[str] = None
) -> SubInfo:
    """Open a subscription info page and read username/status/expiry/bandwidth."""
    async with async_playwright() as pw:
        launch_kwargs = {"headless": headless}
        if proxy:
            launch_kwargs["proxy"] = {"server": proxy}
        browser = await pw.chromium.launch(**launch_kwargs)
        try:
            page = await (await browser.new_context()).new_page()
            page.set_default_timeout(STEP_TIMEOUT_MS)
            try:
                await page.goto(url, wait_until="load")
                await page.wait_for_function(
                    "() => /Bandwidth|Username/i.test(document.body.innerText)",
                    timeout=30_000,
                )
            except PWTimeout:
                raise FetchError("subscription page did not load")
            info = parse_sub_text(await page.inner_text("body"))
            if info.used_gib is None and info.expires_on is None and info.status is None:
                raise FetchError("subscription page layout not recognised")
            return info
        finally:
            await browser.close()


if __name__ == "__main__":
    import sys

    c = sys.argv[1] if len(sys.argv) > 1 else "Canada"
    r = asyncio.run(fetch_config(country=c))
    print(r)
