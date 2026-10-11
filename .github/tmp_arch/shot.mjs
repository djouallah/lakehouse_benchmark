// TEMPORARY: screenshots of the Architecture tab. Deleted after use.
import { chromium } from "playwright";
const browser = await chromium.launch();
const shots = [
  { name: "wide-light-start", w: 1280, dark: false, act: [] },
  { name: "wide-light-commit", w: 1280, dark: false, act: ["#ar-steps li[data-i='5']"] },
  { name: "wide-dark-out-two", w: 1280, dark: true, act: ["button[data-v='out']", "button[data-k='two']", "#ar-steps li[data-i='2']"] },
  { name: "wide-light-up-two-stop", w: 1280, dark: false, act: ["button[data-v='up']", "button[data-k='two']", "#ar-steps li[data-i='6']", "button[data-v='always']"] },
  { name: "phone-light-start", w: 390, dark: false, act: [] },
  { name: "phone-dark-out-two", w: 390, dark: true, act: ["button[data-v='out']", "button[data-k='two']", "#ar-steps li[data-i='4']"] },
];
let errors = 0;
for (const s of shots) {
  const page = await browser.newPage({ viewport: { width: s.w, height: 900 }, colorScheme: s.dark ? "dark" : "light" });
  page.on("pageerror", e => { errors++; console.log(`${s.name} pageerror: ${e.message}`); });
  page.on("console", m => { if (m.type() === "error") console.log(`${s.name} console: ${m.text()}`); });
  await page.goto("http://localhost:8000/");
  await page.waitForSelector("#ar-fig svg");
  for (const sel of s.act) await page.click(sel);
  await page.waitForTimeout(800);
  await page.screenshot({ path: `shots/${s.name}.png`, fullPage: true });
  await page.close();
}
// Other tabs still render.
const page = await browser.newPage();
page.on("pageerror", e => { errors++; console.log(`tabs pageerror: ${e.message}`); });
for (const h of ["performance", "features", "issues", "about"]) {
  await page.goto(`http://localhost:8000/#${h}`);
  await page.waitForTimeout(1500);
}
await page.screenshot({ path: "shots/about.png", fullPage: false });
await browser.close();
console.log(`page errors: ${errors}`);
process.exit(errors ? 1 : 0);
