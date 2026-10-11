import { chromium } from "playwright";
import { readFileSync, writeFileSync } from "fs";
const scene = JSON.parse(readFileSync(process.argv[2], "utf8"));
const browser = await chromium.launch();
const page = await browser.newPage();
page.on("console", m => console.log("page:", m.text()));
await page.setContent(`<html><body><script type="module">
  window.go = async (scene) => {
    const { exportToBlob } = await import("https://esm.sh/@excalidraw/utils@0.1.2");
    const blob = await exportToBlob({ elements: scene.elements, appState: { ...(scene.appState||{}), exportBackground: true, viewBackgroundColor: "#ffffff", exportScale: 2 }, files: scene.files || {}, mimeType: "image/png" });
    const buf = new Uint8Array(await blob.arrayBuffer());
    let s = ""; for (const b of buf) s += String.fromCharCode(b); return btoa(s);
  };
</script></body></html>`);
await page.waitForFunction(() => window.go);
const b64 = await page.evaluate(s => window.go(s), scene);
writeFileSync(process.argv[3], Buffer.from(b64, "base64"));
await browser.close();
