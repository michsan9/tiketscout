"""
TiketScout - scraping & perbandingan rute Singaraja -> Pekanbaru (mudik Idul Fitri 2027)

Install:
    pip install playwright && playwright install chromium

Pakai:
    python tiketscout.py once      # scrape sekali seluruh rentang tanggal
    python tiketscout.py watch     # scrape terus-menerus tiap INTERVAL_JAM
    python tiketscout.py report    # bandingkan skenario dari data terakhir di DB

Catatan: 1 Syawal 1448 H diperkirakan ~9-10 Maret 2027 (cek sidang isbat).
"""
import asyncio, json, os, random, re, sqlite3, sys, time, urllib.request, datetime as dt
from urllib.parse import quote
from playwright.async_api import async_playwright

DB = "tiket.db"
INGEST_URL = os.environ.get("INGEST_URL")   # https://domain/index.php?a=ingest
INGEST_KEY = os.environ.get("INGEST_KEY")
TGL_MULAI = dt.date(2027, 2, 22)     # tanggal berangkat pertama
TGL_AKHIR = dt.date(2027, 3, 28)     # tanggal berangkat terakhir
INTERVAL_JAM = 3
HEADLESS = True

# ---- Definisi skenario: tiap leg = (moda, asal, tujuan, offset_hari dari tgl berangkat)
# Offset ke leg berikutnya perlu disesuaikan dengan jam tiba (bus/kereta malam = +1).
SKENARIO = {
    "1. Bus>DPS + Pesawat DPS-PKU": [
        ("bus", "Singaraja", "Denpasar", 0), ("flight", "DPS", "PKU", 0)],
    "2. Bus>Ketapang + KA>Pasarsenen + Pesawat CGK-PKU": [
        ("bus", "Singaraja", "Ketapang", 0), ("train", "KTG", "PSE", 0), ("flight", "CGK", "PKU", 2)],
    "3. Bus>Surabaya + Pesawat SUB-PKU": [
        ("bus", "Singaraja", "Surabaya", 0), ("flight", "SUB", "PKU", 1)],
    "4. Bus>Jakarta + Pesawat CGK-PKU": [
        ("bus", "Singaraja", "Jakarta", 0), ("flight", "CGK", "PKU", 2)],
    # --- skenario tambahan ---
    "5. Bus>DPS + Pesawat DPS-CGK + CGK-PKU (tiket terpisah)": [
        ("bus", "Singaraja", "Denpasar", 0), ("flight", "DPS", "CGK", 0), ("flight", "CGK", "PKU", 0)],
    "6. Bus>DPS + Pesawat DPS-SUB + SUB-PKU (tiket terpisah)": [
        ("bus", "Singaraja", "Denpasar", 0), ("flight", "DPS", "SUB", 0), ("flight", "SUB", "PKU", 0)],
}

# Estimasi jika scraping leg gagal (Rp) - ditandai '*' di laporan. Ubah sesuai kenyataan.
FALLBACK = {("bus", "Singaraja", "Denpasar"): 100_000}

# ---- URL template. Situs bus/kereta sering berubah: cek manual di browser lalu sesuaikan.
def url_for(mode, o, d, date):
    s = date.isoformat()
    if mode == "flight":
        q = quote(f"Flights from {o} to {d} on {s} one way")
        return f"https://www.google.com/travel/flights?q={q}&curr=IDR&hl=id"
    if mode == "train":
        return (f"https://www.tiket.com/kereta-api/cari?d={o}&dt=STATION&a={d}"
                f"&at=STATION&date={s}&adult=1&infant=0")
    if mode == "bus":
        return (f"https://www.redbus.id/tiket-bus/{o.lower()}-ke-{d.lower()}"
                f"?onward={date.strftime('%d-%b-%Y')}")

MIN_HARGA = {"flight": 300_000, "train": 50_000, "bus": 30_000}
PRICE_RE = re.compile(r"Rp[\s\xa0]*([\d]{1,3}(?:\.\d{3})+)")

def init_db():
    c = sqlite3.connect(DB)
    c.execute("""CREATE TABLE IF NOT EXISTS prices(
        ts TEXT, mode TEXT, o TEXT, d TEXT, date TEXT, price INTEGER, n INTEGER)""")
    c.commit(); return c

def needed_legs():
    out = set()
    hari = (TGL_AKHIR - TGL_MULAI).days
    for i in range(hari + 1):
        dep = TGL_MULAI + dt.timedelta(days=i)
        for legs in SKENARIO.values():
            for mode, o, d, off in legs:
                out.add((mode, o, d, dep + dt.timedelta(days=off)))
    return sorted(out, key=lambda x: (x[3], x[0]))

async def scrape_leg(page, mode, o, d, date):
    await page.goto(url_for(mode, o, d, date), wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(7000)  # tunggu hasil dimuat (JS)
    text = await page.inner_text("body")
    prices = [int(m.replace(".", "")) for m in PRICE_RE.findall(text)]
    prices = [p for p in prices if MIN_HARGA[mode] <= p <= 30_000_000]
    if not prices:
        await page.screenshot(path=f"gagal_{mode}_{o}_{d}_{date}.png")
        return None
    return min(prices), len(prices)

def push(batch):
    """Kirim hasil scraping ke hosting (DomCloud) lewat POST JSON."""
    if not (INGEST_URL and batch): return
    req = urllib.request.Request(INGEST_URL, json.dumps({"key": INGEST_KEY, "rows": batch}).encode(),
                                 {"Content-Type": "application/json"})
    try: print("  push ke hosting:", urllib.request.urlopen(req, timeout=60).read()[:60])
    except Exception as e: print("  push gagal:", e)

async def run_once():
    db = init_db(); legs = needed_legs(); batch = []
    print(f"[{dt.datetime.now():%H:%M}] scraping {len(legs)} leg-tanggal...")
    async with async_playwright() as p:
        br = await p.chromium.launch(headless=HEADLESS)
        ctx = await br.new_context(locale="id-ID", timezone_id="Asia/Jakarta",
                                   user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                   "AppleWebKit/537.36 Chrome/124 Safari/537.36")
        page = await ctx.new_page()
        for mode, o, d, date in legs:
            try:
                r = await scrape_leg(page, mode, o, d, date)
            except Exception as e:
                print("  ERR", mode, o, d, date, str(e)[:60]); continue
            if r:
                db.execute("INSERT INTO prices VALUES(?,?,?,?,?,?,?)",
                           (dt.datetime.now().isoformat(), mode, o, d, date.isoformat(), r[0], r[1]))
                db.commit()
                batch.append(dict(mode=mode, o=o, d=d, date=date.isoformat(), price=r[0]))
                if len(batch) >= 25: push(batch); batch.clear()
                print(f"  {mode:6} {o}->{d} {date}  Rp{r[0]:,}")
            else:
                print(f"  {mode:6} {o}->{d} {date}  (tidak ada harga)")
            await asyncio.sleep(random.uniform(3, 8))  # jeda anti-blokir
        await br.close()
    push(batch)

def latest(db, mode, o, d, date):
    r = db.execute("SELECT price FROM prices WHERE mode=? AND o=? AND d=? AND date=? "
                   "ORDER BY ts DESC LIMIT 1", (mode, o, d, date.isoformat())).fetchone()
    return r[0] if r else None

def report():
    db = init_db(); rows = []
    for i in range((TGL_AKHIR - TGL_MULAI).days + 1):
        dep = TGL_MULAI + dt.timedelta(days=i)
        for nama, legs in SKENARIO.items():
            total, est, ok = 0, False, True
            for mode, o, d, off in legs:
                pr = latest(db, mode, o, d, dep + dt.timedelta(days=off))
                if pr is None:
                    pr = FALLBACK.get((mode, o, d))
                    if pr is None: ok = False; break
                    est = True
                total += pr
            if ok: rows.append((total, nama, dep, est))
    if not rows: print("Belum ada data. Jalankan: python tiketscout.py once"); return
    print("\n=== RINGKASAN PER SKENARIO ===")
    for nama in SKENARIO:
        r = [x for x in rows if x[1] == nama]
        if not r: print(f"{nama}\n   (data belum lengkap)"); continue
        best = min(r); avg = sum(x[0] for x in r) / len(r)
        print(f"{nama}\n   termurah Rp{best[0]:,} (berangkat {best[2]}{'*' if best[3] else ''})"
              f" | rata-rata Rp{avg:,.0f} | {len(r)} tanggal")
    print("\n=== 10 KOMBINASI TERMURAH ===")
    for t, n, dep, est in sorted(rows)[:10]:
        print(f"Rp{t:>10,}  {dep}  {n}{' *estimasi' if est else ''}")
    print("\n* = ada leg memakai harga estimasi (FALLBACK)")

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "report"
    if cmd == "once": asyncio.run(run_once()); report()
    elif cmd == "watch":
        while True:
            asyncio.run(run_once()); report()
            time.sleep(INTERVAL_JAM * 3600)
    else: report()
