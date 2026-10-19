"""docs/index.html fits a 1920x1080 screen: no tab, chip or story step makes the page scroll.

Runs the page in Chromium (Playwright) against a local copy of docs/. Pages publishes only when this
passes (.github/workflows/pages.yml). Without Playwright and its Chromium the test skips, unless
LAYOUT_REQUIRED=1, as in CI, where a skip would let a scrolling page through.
"""

from __future__ import annotations

import functools
import http.server
import os
import threading
from pathlib import Path

import pytest

DOCS = Path(__file__).parent.parent / "docs"
VIEW = {"width": 1920, "height": 930}  # a 1080p screen minus the browser's own bars
REQUIRED = os.environ.get("LAYOUT_REQUIRED") == "1"
LOADED = "![...document.querySelectorAll('.empty')].some(e => /Loading/.test(e.textContent))"
# e.g. LAYOUT_CHANNEL=chrome: an installed Chrome instead of Playwright's Chromium
CHANNEL = os.environ.get("LAYOUT_CHANNEL")

try:
    from playwright.sync_api import Error, sync_playwright
except ImportError:
    if REQUIRED:
        raise
    pytest.skip("playwright is not installed", allow_module_level=True)


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def base():
    handler = functools.partial(Quiet, directory=DOCS)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}/"
    server.shutdown()


@pytest.fixture(scope="module")
def page(base):
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel=CHANNEL) if CHANNEL else p.chromium.launch()
        except Error as e:
            if REQUIRED:
                raise
            pytest.skip(f"no Chromium for Playwright: {e}")
        yield browser.new_page(viewport=VIEW)
        browser.close()


def settle(page):
    """Wait for the data, then put a Query plans race at its end: its tallest state."""
    page.wait_for_load_state("networkidle")
    page.wait_for_function(LOADED)
    if page.locator("#plans:not([hidden]) #pl-play").count():
        page.wait_for_function("/Skip|Replay/.test(document.getElementById('pl-play').textContent)")
        if "Skip" in page.inner_text("#pl-play"):
            page.click("#pl-play")
    page.wait_for_timeout(150)


def overflow(page, where, failures):
    h, w = page.evaluate(
        "[document.documentElement.scrollHeight - innerHeight,"
        " document.documentElement.scrollWidth - innerWidth]"
    )
    if h > 0:
        failures.append(f"{where}: {VIEW['height'] + h}px tall > {VIEW['height']}px")
    if w > 0:
        failures.append(f"{where}: {VIEW['width'] + w}px wide > {VIEW['width']}px")


def test_no_tab_scrolls_on_a_1080p_screen(base, page):
    page.goto(base)
    tabs = page.evaluate("GROUPS.flatMap(g => g.subs.map(s => s.id))")
    failures = []
    for tab in tabs:
        page.goto("about:blank")
        page.goto(f"{base}#{tab}")
        settle(page)
        overflow(page, tab, failures)
        # Every chip on the tab (scale factors, Iceberg groups, plan stories), re-found each time.
        chips = "#chips button, main > section:not([hidden]) .chips:not([hidden]) button"
        for i in range(page.locator(chips).count()):
            button = page.locator(chips).nth(i)
            label = button.inner_text()
            button.click()
            settle(page)
            overflow(page, f"{tab} › {label}", failures)
        # Every step of a stepped story (Concurrency, Anatomy, File pruning).
        step = page.locator("main > section:not([hidden]) button[id$='-step']")
        for n in range(1, 30):
            if not step.count() or step.is_disabled():
                break
            step.click()
            page.wait_for_timeout(50)
            overflow(page, f"{tab} › step {n}", failures)
    assert not failures, "the page scrolls:\n" + "\n".join(failures)
