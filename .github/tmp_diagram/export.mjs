import { chromium } from "playwright";
import { readFileSync, writeFileSync } from "fs";
const scene = JSON.parse(readFileSync(process.argv[2], "utf8"));
const browser = await chromium.launch();
const page = await browser.newPage();
page.on("console", m => console.log("page:", m.text()));
await page.setContent(`<html><body><script type="module">
  window.go = async (scene) => {
    window.EXCALIDRAW_ASSET_PATH = "https://unpkg.com/@excalidraw/excalidraw@0.17.6/dist/";
    const mod = await import("https://esm.sh/@excalidraw/excalidraw@0.17.6?deps=react@18.2.0,react-dom@18.2.0");
    const exportToBlob = mod.exportToBlob ?? mod.default?.exportToBlob;
    console.log("keys", Object.keys(mod).slice(0, 40).join(","));
    await document.fonts.ready;
    const blob = await exportToBlob({ elements: scene.elements, appState: { ...(scene.appState||{}), exportBackground: true, viewBackgroundColor: "#ffffff", exportScale: 2 }, files: scene.files || {}, mimeType: "image/png" });
    const buf = new Uint8Array(await blob.arrayBuffer());
    let s = ""; for (const b of buf) s += String.fromCharCode(b); return btoa(s);
  };
</script></body></html>`);
await page.waitForFunction(() => window.go);
const b64 = await page.evaluate(s => window.go(s), scene);
writeFileSync(process.argv[3], Buffer.from(b64, "base64"));
await browser.close();
