// TEMPORARY: screenshots of the Architecture tab. Deleted after use.
import { chromium } from "playwright";
const browser = await chromium.launch();
let errors = 0;
const open = async (w, dark) => {
  const page = await browser.newPage({ viewport: { width: w, height: 1000 }, colorScheme: dark ? "dark" : "light" });
  page.on("pageerror", e => { errors++; console.log(`pageerror: ${e.message}`); });
  await page.goto("http://localhost:8000/");
  await page.waitForSelector("#ar-fig svg");
  return page;
};
// Demo 2 is hidden at first.
let page = await open(1280, false);
console.log("duo hidden before:", await page.evaluate(() => $("duo").hidden));
await page.screenshot({ path: "shots/before.png" });
await page.close();
// Each demo-2 step, shown after demo 1.
const shots = [
  { name: "wide-light-3", w: 1280, dark: false, t: 2 },
  { name: "wide-light-5", w: 1280, dark: false, t: 4 },
  { name: "wide-dark-6", w: 1280, dark: true, t: 5 },
  { name: "wide-light-7", w: 1280, dark: false, t: 6 },
  { name: "phone-light-6", w: 390, dark: false, t: 5 },
  { name: "phone-dark-7", w: 390, dark: true, t: 6 },
];
for (const s of shots) {
  page = await open(s.w, s.dark);
  await page.evaluate(n => { arch.t = 7; arch.shown = true; drawArch(); duo.t = n; drawDuo(); }, s.t);
  await page.waitForTimeout(800);
  await page.screenshot({ path: `shots/${s.name}.png`, fullPage: true });
  await page.close();
}
// Real play: demo 1 to the end shows demo 2; demo 2 plays to the end.
page = await open(1280, false);
await page.click("#ar-play");
await page.waitForTimeout(11000);
console.log("duo hidden after demo 1:", await page.evaluate(() => $("duo").hidden), "|", await page.textContent("#ar-play"));
await page.click("#duo-play");
await page.waitForTimeout(2500);
console.log("duo running:", await page.textContent("#duo-play"));
await page.waitForTimeout(9000);
console.log("duo after:", await page.textContent("#duo-play"), "|", await page.textContent("#duo-say"));
await browser.close();
console.log(`page errors: ${errors}`);
process.exit(errors ? 1 : 0);
