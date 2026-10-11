// TEMPORARY: screenshots of the Architecture tab. Deleted after use.
import { chromium } from "playwright";
const browser = await chromium.launch();
const shots = [
  { name: "wide-light-3", w: 1280, dark: false, t: 3 },
  { name: "wide-light-6", w: 1280, dark: false, t: 6 },
  { name: "wide-dark-7", w: 1280, dark: true, t: 7 },
  { name: "wide-light-7", w: 1280, dark: false, t: 7 },
  { name: "phone-light-4", w: 390, dark: false, t: 4 },
  { name: "phone-dark-6", w: 390, dark: true, t: 6 },
];
let errors = 0;
for (const s of shots) {
  const page = await browser.newPage({ viewport: { width: s.w, height: 900 }, colorScheme: s.dark ? "dark" : "light" });
  page.on("pageerror", e => { errors++; console.log(`${s.name} pageerror: ${e.message}`); });
  await page.goto("http://localhost:8000/");
  await page.waitForSelector("#ar-fig svg");
  await page.evaluate(n => { arch.t = n; drawArch(); }, s.t);
  await page.waitForTimeout(800);
  await page.screenshot({ path: `shots/${s.name}.png` });
  await page.close();
}
const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
page.on("pageerror", e => { errors++; console.log(`play pageerror: ${e.message}`); });
await page.goto("http://localhost:8000/");
console.log("before:", await page.textContent("#ar-play"));
await page.click("#ar-play");
await page.waitForTimeout(3000);
console.log("running:", await page.textContent("#ar-play"));
await page.screenshot({ path: "shots/running.png" });
await page.waitForTimeout(12000);
console.log("after:", await page.textContent("#ar-play"), "|", await page.textContent("#ar-say"));
await browser.close();
console.log(`page errors: ${errors}`);
process.exit(errors ? 1 : 0);
