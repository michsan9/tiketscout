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
# Idul Fitri 2027 diperkirakan 9-10 Maret. PERGI = mudik, BALIK = arus balik (pulang).
PERGI = (dt.date(2027, 2, 22), dt.date(2027, 3, 8))
BALIK = (dt.date(2027, 3, 12), dt.date(2027, 3, 28))
STEP_HARI = 2                          # ambil tiap N hari (hemat waktu scraping)
TGL_MULAI, TGL_AKHIR = PERGI          # dipakai report() sekali jalan
INTERVAL_JAM = 3
HEADLESS = True

# ---- Definisi skenario: tiap leg = (moda, asal, tujuan, offset_hari dari tgl berangkat)
# Offset ke leg berikutnya perlu disesuaikan dengan jam tiba (bus/kereta malam = +1).
SKENARIO = {
    "1. Bus>DPS + Pesawat DPS-PKU": [
        ("bus", "Singaraja", "Denpasar", 0), ("flight", "DPS", "PKU", 0)],
    "2. Bus>Ketapang + KA>Pasarsenen + Pesawat CGK-PKU": [
        ("bus", "Singaraja", "Ketapang", 0), ("train", "KTG", "PSE", 0), ("flight", "CGK", "PKU", 1)],
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
FALLBACK = {("bus", "Singaraja", "Denpasar"): 100_000,
            ("train", "KTG", "PSE"): 505_000}   # tarif KA Blambangan Ekspres ekonomi (KAI, 2024)

# ---- URL template. Situs bus/kereta sering berubah: cek manual di browser lalu sesuaikan.
def url_for(mode, o, d, date):
    s = date.isoformat() if date else "*"
    if mode == "flight":
        q = quote(f"Flights from {o} to {d} on {s} one way")
        return f"https://www.google.com/travel/flights?q={q}&curr=IDR&hl=id"
    if mode == "train":
        return (f"https://www.tiket.com/kereta-api/cari?d={o}&dt=STATION&a={d}"
                f"&at=STATION&date={s}&adult=1&infant=0")
    if mode == "bus":  # halaman rute redBus (memuat "Tiket Bus Termurah: RP xxx"), tidak per tanggal
        return f"https://www.redbus.id/tiket-bus/{SLUG.get(o, o).lower()}-ke-{SLUG.get(d, d).lower()}"

# Nama kota di URL redBus (Ketapang adalah titik naik di Banyuwangi)
SLUG = {"Denpasar": "denpasar-bali", "Ketapang": "banyuwangi"}
FROM_RE = re.compile(r"(?:mulai dari|harga mulai)\s*Rp[\s\xa0]*([\d]{1,3}(?:\.\d{3})+)", re.I)

def bus_urls(o, d):
    """Daftar sumber harga bus/travel (shuttle) berurutan; dicoba sampai ada harga."""
    pairs = []
    for a, b in [(SLUG.get(o, o), SLUG.get(d, d)), (o, d)]:
        if (a.lower(), b.lower()) not in pairs: pairs.append((a.lower(), b.lower()))
    urls = []
    for a, b in pairs:
        urls += [f"https://www.redbus.id/tiket-bus/{a}-ke-{b}",                      # bus + travel
                 f"https://www.busonlineticket.co.id/id-id/tiket-bus-{a}-ke-{b}",     # bus
                 f"https://www.traveloka.com/id-id/bus-and-shuttle/route/{a}.{b}"]    # bus + travel/shuttle
    return urls

MIN_HARGA = {"flight": 300_000, "train": 50_000, "bus": 30_000}
PRICE_RE = re.compile(r"(?:Rp|IDR)[\s\xa0]*([\d]{1,3}(?:[.,]\d{3})+)", re.I)   # "Rp 1.234.567" / "IDR 1,234,567"
BUS_RE = re.compile(r"Termurah\s*:?\s*Rp[\s\xa0]*([\d]{1,3}(?:\.\d{3})+)", re.I)

def init_db():
    c = sqlite3.connect(DB)
    c.execute("""CREATE TABLE IF NOT EXISTS prices(
        ts TEXT, mode TEXT, o TEXT, d TEXT, date TEXT, price INTEGER, n INTEGER)""")
    c.commit(); return c

def balik(legs):
    """Rute pulang: urutan dibalik, asal/tujuan ditukar, offset hari dicerminkan."""
    mx = max(l[3] for l in legs)
    return [(m, d, o, mx - off) for m, o, d, off in reversed(legs)]

def needed_legs():
    out = set()
    for (a, b), rev in ((PERGI, False), (BALIK, True)):
        for i in range(0, (b - a).days + 1, STEP_HARI):
            dep = a + dt.timedelta(days=i)
            for legs in SKENARIO.values():
                for mode, o, d, off in (balik(legs) if rev else legs):
                    # bus: harga rute (tidak per tanggal) -> cukup 1 halaman per rute
                    out.add((mode, o, d, None if mode == "bus" else dep + dt.timedelta(days=off)))
    return sorted(out, key=lambda x: (0 if x[0] == "flight" and {x[1], x[2]} == {"DPS", "PKU"} else 1, x[3] or dt.date.min, x[0]))

# ---- Durasi perjalanan: dibaca dari teks hasil OTA (bukan asumsi) ----
DUR_RE = re.compile(r"(\d{1,3})\s*(?:jam|j|h|hrs?|hours?)(?![a-z])(?:\s*(\d{1,2})\s*(?:menit|mnt|m|mins?|minutes?)(?![a-z]))?", re.I)

def to_min(m):
    """Match DUR_RE -> menit (None bila tidak masuk akal)."""
    v = int(m.group(1)) * 60 + (int(m.group(2)) if m.group(2) else 0)
    return v if 20 <= v <= 3600 else None

def opsi(text, mode):
    """Semua opsi di halaman: [(harga, durasi_menit|None)]. Durasi = yang terdekat SEBELUM harga (atau sesudahnya bila tak ada)."""
    out = []
    for m in PRICE_RE.finditer(text):
        v = int(re.sub(r"[.,]", "", m.group(1)))
        if not (MIN_HARGA[mode] <= v <= 30_000_000): continue
        before = [to_min(x) for x in DUR_RE.finditer(text[max(0, m.start() - 400): m.start()])]
        after = [to_min(x) for x in DUR_RE.finditer(text[m.end(): m.end() + 120])]
        before, after = [x for x in before if x], [x for x in after if x]
        out.append((v, before[-1] if before else (after[0] if after else None)))
    return out

def bus_dur(text, m):
    """Durasi rata-rata rute dari halaman rute bus (label 'Durasi/Duration' atau angka tepat sebelum harga termurah)."""
    lab = re.search(r"(?:durasi|duration)[^\d]{0,50}", text, re.I)
    if lab:
        x = DUR_RE.search(text[lab.end(): lab.end() + 40])
        if x and to_min(x): return to_min(x)
    for x in reversed(list(DUR_RE.finditer(text[max(0, m.start() - 250): m.start()]))):
        if to_min(x): return to_min(x)
    return None

# ---- Pesawat: dicari langsung di tiap OTA/maskapai (URL bisa berubah -> sesuaikan bila kosong) ----
FLIGHT_SITES = ["traveloka", "agoda", "trip", "airasia"]   # tambahkan "google" bila ingin Google Flights juga
# batikair.com: pencarian berupa form tanpa deep link -> harga Batik Air diisi manual di dashboard.

def flight_url(site, o, d, date):
    iso, dmy, dmy2 = date.isoformat(), date.strftime("%d-%m-%Y"), date.strftime("%d/%m/%Y")
    return {
        "traveloka": f"https://www.traveloka.com/id-id/flight/fullsearch?ap={o}.{d}&dt={dmy}.null&ps=1.0.0&sc=ECONOMY",
        "agoda": (f"https://www.agoda.com/id-id/flights/results?departureFrom={o}&departureFromType=1&arrivalTo={d}"
                  f"&arrivalToType=1&departDate={iso}&adults=1&children=0&infants=0&cabinType=Economy&tripType=OneWay"),
        "trip": f"https://id.trip.com/flights/showfarefirst?dcity={o.lower()}&acity={d.lower()}&ddate={iso}&triptype=ow&class=y&quantity=1",
        "airasia": (f"https://www.airasia.com/flights/search/?origin={o}&destination={d}&departDate={dmy2}"
                    "&tripType=O&adult=1&child=0&infant=0&locale=id-id&currency=IDR"),
        "google": url_for("flight", o, d, date),
    }[site]

TRAIN_SITES = ["traveloka", "tiket"]   # KAI Access adalah aplikasi HP (tidak bisa di-scrape); booking.kai.id berupa form

def train_url(site, o, d, date):
    dmy, iso = date.strftime("%d-%m-%Y"), date.isoformat()
    return {
        "traveloka": f"https://www.traveloka.com/id-id/kereta-api/search?st={o}.{d}&dt={dmy}.null&ps=1.0",
        "tiket": f"https://www.tiket.com/kereta-api/cari?d={o}&dt=STATION&a={d}&at=STATION&date={iso}&adult=1&infant=0",
    }[site]

SHOTS = [0]   # batasi jumlah screenshot gagal

async def scrape_leg(page, mode, o, d, date, site=None):
    if mode == "bus":                      # bus/travel: coba beberapa sumber sampai ada harga
        for u in bus_urls(o, d):
            try:
                await page.goto(u, wait_until="domcontentloaded", timeout=45000)
                await page.wait_for_timeout(4000)
                text = await page.inner_text("body")
            except Exception:
                continue
            m = BUS_RE.search(text) or FROM_RE.search(text)
            if m and int(m.group(1).replace(".", "")) >= MIN_HARGA["bus"]:
                dur = bus_dur(text, m)
                print("   sumber:", u.split("/")[2], "| durasi:", f"{dur} mnt" if dur else "-", flush=True)
                return int(m.group(1).replace(".", "")), 1, dur
        return None
    url = flight_url(site, o, d, date) if mode == "flight" else train_url(site, o, d, date)
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(10000 if mode == "flight" else 7000)  # tunggu hasil dimuat (JS)
    text = await page.inner_text("body")
    ops = opsi(text, mode)
    if not ops:
        blk = " (kemungkinan diblokir/CAPTCHA)" if re.search(r"captcha|robot|access denied|verify you", text, re.I) else ""
        print(f"     {site or mode}: kosong{blk}", flush=True)
        if SHOTS[0] < 20:
            SHOTS[0] += 1
            await page.screenshot(path=f"gagal_{site or mode}_{o}_{d}_{date}.png")
        return None
    best = min(ops, key=lambda t: t[0])
    return best[0], len(ops), best[1]      # (harga termurah, jumlah opsi, durasi opsi termurah dlm menit)

def push(batch):
    """Kirim hasil scraping ke hosting (DomCloud) lewat POST JSON."""
    if not (INGEST_URL and batch): return
    req = urllib.request.Request(INGEST_URL, json.dumps({"key": INGEST_KEY, "rows": batch}).encode(),
                                 {"Content-Type": "application/json"})
    try: print("  push ke hosting:", urllib.request.urlopen(req, timeout=60).read()[:60], flush=True)
    except Exception as e: print("  push gagal:", e, flush=True)

async def run_once():
    db = init_db(); legs = needed_legs()
    print(f"[{dt.datetime.now():%H:%M}] scraping {len(legs)} leg-tanggal...", flush=True)
    async with async_playwright() as p:
        br = await p.chromium.launch(headless=HEADLESS)
        ctx = await br.new_context(locale="id-ID", timezone_id="Asia/Jakarta",
                                   user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                   "AppleWebKit/537.36 Chrome/124 Safari/537.36")
        page = await ctx.new_page()
        for mode, o, d, date in legs:
            ds = date.isoformat() if date else "*"
            for site in (FLIGHT_SITES if mode == "flight" else TRAIN_SITES if mode == "train" else [None]):
                try:
                    r = await scrape_leg(page, mode, o, d, date, site)
                except Exception as e:
                    print("  ERR", site or mode, o, d, ds, str(e)[:60], flush=True); continue
                if r:
                    db.execute("INSERT INTO prices VALUES(?,?,?,?,?,?,?)",
                               (dt.datetime.now().isoformat(), mode, o, d, ds, r[0], r[1]))
                    db.commit()
                    push([dict(mode=mode, o=o, d=d, date=ds, price=r[0], dur=r[2], site=site or mode)])
                    print(f"  {mode:6} {(site or ''):9} {o}->{d} {ds}  Rp{r[0]:,}  " + (f"{r[2] // 60}j{r[2] % 60:02d}m" if r[2] else "durasi ?"), flush=True)
                elif mode == "bus":
                    print(f"  {mode:6} {o}->{d} {ds}  (tidak ada harga)", flush=True)
                await asyncio.sleep(random.uniform(2, 5))  # jeda anti-blokir
        await br.close()

async def cek(mode, o, d, ds):
    """Uji cepat satu rute di semua sumber:  python tiketscout.py cek flight DPS PKU 2027-03-01"""
    date = None if mode == "bus" else dt.date.fromisoformat(ds)
    sites = FLIGHT_SITES if mode == "flight" else TRAIN_SITES if mode == "train" else [None]
    async with async_playwright() as p:
        br = await p.chromium.launch(headless=HEADLESS)
        ctx = await br.new_context(locale="id-ID", timezone_id="Asia/Jakarta",
                                   user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36")
        page = await ctx.new_page()
        for site in sites:
            try: r = await scrape_leg(page, mode, o, d, date, site)
            except Exception as e: r = None; print("  ERR", site, str(e)[:80], flush=True)
            print(f"CEK {mode} {o}->{d} {ds} [{site or 'bus'}]: " + (f"Rp{r[0]:,} | " + (f"{r[2] // 60}j{r[2] % 60:02d}m" if r[2] else "durasi ?") if r else "KOSONG"), flush=True)
            try: await page.screenshot(path=f"cek_{mode}_{site or 'bus'}.png")
            except Exception: pass
            if r: push([dict(mode=mode, o=o, d=d, date=ds if date else "*", price=r[0], dur=r[2], site=site or mode)])
        await br.close()

def latest(db, mode, o, d, date):
    r = db.execute("SELECT price FROM prices WHERE mode=? AND o=? AND d=? AND date IN (?, '*') "
                   "ORDER BY (date='*') ASC, ts DESC LIMIT 1", (mode, o, d, date.isoformat())).fetchone()
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
    elif cmd == "cek": asyncio.run(cek(*sys.argv[2:6]))
    else: report()
