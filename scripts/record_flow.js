// Records 7 s of the GHOST flow page (the whole workspace, Continuous trace running) as PNG frames + a concat list with real
// frame durations, via a CDP screencast. Dev-only: needs node and a Playwright install (PW=/path/to/node_modules/playwright
// if it is not resolvable), Chrome, and the site served at 127.0.0.1:8765. Then: python3 scripts/readme_media.py --flow-frames DIR
const path = require('path'), fs = require('fs');
const { chromium } = require(process.env.PW || 'playwright');
const OUT = process.argv[2];
(async () => {
  let browser;
  try { browser = await chromium.launch({ channel: 'chrome' }); } catch { browser = await chromium.launch(); }
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 });
  await page.goto('http://127.0.0.1:8765/flow.html');
  await page.waitForTimeout(2500);
  const close = page.locator('#inspector button[aria-label*="lose"], #inspector button:has-text("✕"), #inspector button:has-text("×")').first();
  if (await close.count()) await close.click().catch(() => {});
  await page.waitForTimeout(800);
  const cdp = await page.context().newCDPSession(page);
  const frames = []; let n = 0, on = false;
  cdp.on('Page.screencastFrame', async ({ data, metadata, sessionId }) => {
    if (on) { const f = path.join(OUT, `f-${String(n++).padStart(4, '0')}.png`); fs.writeFileSync(f, Buffer.from(data, 'base64')); frames.push([f, metadata.timestamp]); }
    await cdp.send('Page.screencastFrameAck', { sessionId }).catch(() => {});
  });
  await cdp.send('Page.startScreencast', { format: 'png', everyNthFrame: 1, maxWidth: 1440, maxHeight: 900 });
  on = true;
  await page.waitForTimeout(7000);
  on = false;
  await cdp.send('Page.stopScreencast');
  await browser.close();
  const lines = [];
  for (let i = 0; i < frames.length; i++) {
    const dur = i + 1 < frames.length ? frames[i + 1][1] - frames[i][1] : 0.05;
    lines.push(`file '${frames[i][0]}'`, `duration ${Math.max(dur, 0.005).toFixed(4)}`);
  }
  lines.push(`file '${frames[frames.length - 1][0]}'`);
  fs.writeFileSync(path.join(OUT, 'list.txt'), lines.join('\n') + '\n');
  const span = frames[frames.length - 1][1] - frames[0][1];
  console.log(JSON.stringify({ frames: frames.length, seconds: +span.toFixed(2), fps: +(frames.length / span).toFixed(1) }));
})();
