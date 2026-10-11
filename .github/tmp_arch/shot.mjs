// TEMPORARY: screenshots of the Concurrency tab. Deleted after use.
import { chromium } from "playwright";
const browser = await chromium.launch();
let errors = 0;
const open = async (w, dark) => {
  const page = await browser.newPage({ viewport: { width: w, height: 1000 }, colorScheme: dark ? "dark" : "light" });
  page.on("pageerror", e => { errors++; console.log(`pageerror: ${e.message}`); });
  await page.goto("http://localhost:8000/#concurrency");
  await page.waitForSelector("#ar-fig svg");
  return page;
};
const shots = [
  { name: "wide-light-04", w: 1280, dark: false, t: 3 },
  { name: "wide-light-09", w: 1280, dark: false, t: 8 },
  { name: "wide-dark-14", w: 1280, dark: true, t: 13 },
  { name: "wide-light-16", w: 1280, dark: false, t: 15 },
  { name: "phone-light-16", w: 390, dark: false, t: 15 },
];
for (const s of shots) {
  const page = await open(s.w, s.dark);
  await page.evaluate(n => { arch.t = n; drawArch(); }, s.t);
  await page.waitForTimeout(1000);
  await page.screenshot({ path: `shots/${s.name}.png`, fullPage: true });
  await page.close();
}
// The real run: one button through both scenarios, the box only at the end.
const page = await open(1280, false);
console.log("box hidden at start:", await page.evaluate(() => $("ar-next").hidden), "|", await page.textContent("#ar-play"));
await page.click("#ar-play");
await page.waitForTimeout(12000);
console.log("mid run:", await page.textContent("#ar-play"), "| box hidden:", await page.evaluate(() => $("ar-next").hidden));
await page.waitForTimeout(10000);
console.log("end:", await page.textContent("#ar-play"), "| box hidden:", await page.evaluate(() => $("ar-next").hidden));
await page.click("#ar-play");
await page.waitForTimeout(500);
console.log("replay:", await page.textContent("#ar-play"), "| box hidden:", await page.evaluate(() => $("ar-next").hidden));
await browser.close();
console.log(`page errors: ${errors}`);
process.exit(errors ? 1 : 0);
