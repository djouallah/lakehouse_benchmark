// TEMPORARY: screenshots of the Concurrency tab. Deleted after use.
import { chromium } from "playwright";
const browser = await chromium.launch();
let errors = 0;
for (const [w, dark] of [[1280, false], [390, true]]) {
  const page = await browser.newPage({ viewport: { width: w, height: 1000 }, colorScheme: dark ? "dark" : "light" });
  page.on("pageerror", e => { errors++; console.log(`pageerror: ${e.message}`); });
  await page.goto("http://localhost:8000/#concurrency");
  await page.waitForSelector("#ar-fig svg");
  await page.click("#ar-play");
  await page.waitForTimeout(20000);
  const st = () => page.evaluate(() => [$("ar-fig").hidden, $("ar-say").hidden, $("ar-next").hidden, $("ar-play").textContent]);
  console.log(w, "step 16 (fig, say, note hidden; button):", JSON.stringify(await st()));
  await page.waitForTimeout(2500);
  console.log(w, "end:", JSON.stringify(await st()));
  await page.screenshot({ path: `shots/end-${w}.png`, fullPage: true });
  await page.click("#ar-play");
  await page.waitForTimeout(400);
  console.log(w, "replay:", JSON.stringify(await st()));
  await page.close();
}
await browser.close();
console.log(`page errors: ${errors}`);
process.exit(errors ? 1 : 0);
