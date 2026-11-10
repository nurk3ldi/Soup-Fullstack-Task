"""Not a test: takes screenshots of the UI for a visual check. Run: python tests/_screens.py"""
import asyncio, math, os, uuid
import httpx
from playwright.async_api import async_playwright

API = os.environ["API_URL"]; ADMIN = os.environ.get("ADMIN_TOKEN", "dev-admin-token")

async def main():
    async with httpx.AsyncClient(base_url=API) as c:
        proj = (await c.post("/projects", json={"name": "demo"}, headers={"Authorization": f"Bearer {ADMIN}"})).json()
        key = proj["api_key"]["key"]; h = {"Authorization": f"Bearer {key}"}
        run = (await c.post("/runs", json={"name": "resnet50-lr3e-4"}, headers=h)).json()
        await c.post("/runs", json={"name": "baseline-sgd"}, headers=h)
        pts = [{"name": "loss", "step": s, "value": round(2.5 * math.exp(-s / 40) + 0.05 * math.sin(s / 3), 4)} for s in range(120)]
        pts += [{"name": "accuracy", "step": s, "value": round(1 - math.exp(-s / 30), 4)} for s in range(0, 120, 4)]
        await c.post(f"/runs/{run['id']}/metrics", json={"batch_id": str(uuid.uuid4()), "points": pts}, headers=h)
    async with async_playwright() as p:
        b = await p.chromium.launch()
        for name, vp, scheme in (("desktop", {"width": 1100, "height": 800}, "light"), ("mobile", {"width": 390, "height": 800}, "dark")):
            ctx = await b.new_context(viewport=vp, color_scheme=scheme); pg = await ctx.new_page()
            await pg.goto(API + "/"); await pg.screenshot(path=f"results/screens/{name}_login.png")
            await pg.fill("#key-input", key); await pg.click("#login-form button"); await pg.locator("#runs-body a").first.wait_for()
            await pg.screenshot(path=f"results/screens/{name}_runs.png")
            await pg.goto(f"{API}/runs/{run['id']}/view"); await pg.wait_for_timeout(1500)
            await pg.screenshot(path=f"results/screens/{name}_run.png")
            print(name, "scrollWidth", await pg.evaluate("document.documentElement.scrollWidth"), "viewport", vp["width"])
        await b.close()
asyncio.run(main())
