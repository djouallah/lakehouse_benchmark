// TEMPORARY: screenshots of the Architecture tab. Deleted after use.
import { chromium } from "playwright";
const browser = await chromium.launch();
const shots = [
  { name: "wide-light-1", w: 1280, dark: false, step: 1 },
  { name: "wide-light-3", w: 1280, dark: false, step: 3 },
  { name: "wide-dark-4", w: 1280, dark: true, step: 4 },
  { name: "wide-light-6", w: 1280, dark: false, step: 6 },
  { name: "phone-light-4", w: 390, dark: false, step: 4 },
  { name: "phone-dark-5", w: 390, dark: true, step: 5 },
];
let errors = 0;
for (const s of shots) {
  const page = await browser.newPage({ viewport: { width: s.w, height: 900 }, colorScheme: s.dark ? "dark" : "light" });
  page.on("pageerror", e => { errors++; console.log(`${s.name} pageerror: ${e.message}`); });
  await page.goto("http://localhost:8000/");
  await page.waitForSelector("#ar-fig svg");
  await page.evaluate(n => { arch.step = n; drawArch(); }, s.step);
  await page.waitForTimeout(800);
  await page.screenshot({ path: `shots/${s.name}.png`, fullPage: false });
  await page.close();
}
// Play runs to the end without errors.
const page = await browser.newPage();
page.on("pageerror", e => { errors++; console.log(`play pageerror: ${e.message}`); });
await page.goto("http://localhost:8000/");
await page.click("#ar-play");
await page.waitForTimeout(16000);
console.log("after play:", await page.textContent("#ar-say"));
await browser.close();
console.log(`page errors: ${errors}`);
process.exit(errors ? 1 : 0);
