"""
TiketScout - scraping & perbandingan rute Singaraja -> Pekanbaru (mudik Idul Fitri 2027)

Install:
    pip install playwright && playwright install chromium

Pakai:
    python tiketscout.py once      # cari harga SEMUA tanggal (1-31 Maret 2027), pergi & pulang, semua skenario
    python tiketscout.py watch     # scrape terus-menerus tiap INTERVAL_JAM
    python tiketscout.py report    # bandingkan skenario dari data terakhir di DB
    python tiketscout.py cek flight DPS PKU 2027-03-01   # uji 1 rute di semua OTA

Catatan: 1 Syawal 1448 H diperkirakan ~9-10 Maret 2027 (cek sidang isbat).
"""
import asyncio, json, os, random, re, sqlite3, sys, time, urllib.request, datetime as dt
from urllib.parse import quote
from playwright.async_api import async_playwright

DB = "tiket.db"
INGEST_URL = os.environ.get("INGEST_URL")   # https://domain/index.php?a=ingest
INGEST_KEY = os.environ.get("INGEST_KEY")
# Pencarian per tanggal: 1-31 Maret 2027 (Idul Fitri diperkirakan 9-10 Maret). Pergi DAN pulang dicari di setiap tanggal.
TGL_MULAI = dt.date(2027, 3, 1)
TGL_AKHIR = dt.date(2027, 3, 31)
STEP_HARI = 1
WORKERS = int(os.environ.get("WORKERS", "4"))   # jumlah halaman browser paralel
K_VERIFIKASI = 3                                # per rute: K tanggal termurah dicek ulang di OTA lain
INTERVAL_JAM = 3
HEADLESS = True

# ---- Skenario: Anda tinggal di SINGARAJA (tanpa bandara komersial) -> darat dulu ke gerbang, lalu terbang.
#   Gerbang 1: DENPASAR (DPS) naik travel/bus ~3 jam.   Gerbang 2: SURABAYA (SUB) naik bus ~10 jam.
#   Tidak ada penerbangan langsung DPS-PKU: transit via Jakarta (CGK), Kuala Lumpur (KUL) atau Singapura (SIN) tidak masalah.
#   "Tiket tunggal" = OTA yang memilihkan transit-nya; "via X" = dua tiket terpisah (cek harga tiap sektor).
# Tiap leg = (moda, asal, tujuan, offset_hari dari tanggal berangkat). Pulang = urutan dibalik otomatis (balik()).
SKENARIO = {
    "A. Travel ke Denpasar + DPS-PKU (tiket tunggal, transit bebas)": [
        ("bus", "Singaraja", "Denpasar", 0), ("flight", "DPS", "PKU", 0)],
    "B. Travel ke Denpasar + via Jakarta (DPS-CGK + CGK-PKU)": [
        ("bus", "Singaraja", "Denpasar", 0), ("flight", "DPS", "CGK", 0), ("flight", "CGK", "PKU", 0)],
    "C. Travel ke Denpasar + via Kuala Lumpur (DPS-KUL + KUL-PKU)": [
        ("bus", "Singaraja", "Denpasar", 0), ("flight", "DPS", "KUL", 0), ("flight", "KUL", "PKU", 0)],
    "D. Travel ke Denpasar + via Singapura (DPS-SIN + SIN-PKU)": [
        ("bus", "Singaraja", "Denpasar", 0), ("flight", "DPS", "SIN", 0), ("flight", "SIN", "PKU", 0)],
    "E. Bus ke Surabaya + SUB-PKU (tiket tunggal, transit bebas)": [
        ("bus", "Singaraja", "Surabaya", 0), ("flight", "SUB", "PKU", 1)],
    "F. Bus ke Surabaya + via Jakarta (SUB-CGK + CGK-PKU)": [
        ("bus", "Singaraja", "Surabaya", 0), ("flight", "SUB", "CGK", 1), ("flight", "CGK", "PKU", 1)],
}

# Hanya tarif resmi yang sudah ada sumbernya. Leg lain TANPA harga = skenario ditandai belum lengkap (isi manual di dashboard).
FALLBACK = {("train", "KTG", "PSE"): 505_000}   # tarif KA Blambangan Ekspres ekonomi (KAI, 2024)

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
SLUG = {"Denpasar": "denpasar-bali"}
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
    for i in range(0, (TGL_AKHIR - TGL_MULAI).days + 1, STEP_HARI):
        dep = TGL_MULAI + dt.timedelta(days=i)
        for legs in SKENARIO.values():
            for rute in (legs, balik(legs)):                       # pergi & pulang
                for mode, o, d, off in rute:
                    # bus: harga rute (tidak per tanggal) -> cukup 1 halaman per rute
                    out.add((mode, o, d, None if mode == "bus" else dep + dt.timedelta(days=off)))
    return sorted(out, key=lambda x: (0 if x[0] == "flight" and {x[1], x[2]} == {"DPS", "PKU"} else 1, x[3] or dt.date.min, x[0]))

# ---- Durasi perjalanan: dibaca dari teks hasil OTA (bukan asumsi) ----
DUR_RE = re.compile(r"(\d{1,3})\s*(?:jam|j|h|hrs?|hours?)(?![a-z])(?:\s*(\d{1,2})\s*(?:menit|mnt|m|mins?|minutes?)(?![a-z]))?", re.I)

def to_min(m):
    """Match DUR_RE -> menit (None bila tidak masuk akal)."""
    v = int(m.group(1)) * 60 + (int(m.group(2)) if m.group(2) else 0)
    return v if 20 <= v <= 3600 else None

TIME_RE = re.compile(r"(?<![\d.,])(?:[01]?\d|2[0-3])[:.][0-5]\d(?!\d)")            # jam tayang 12:25 / 12.25
PROMO_RE = re.compile(r"cashback|hemat|potongan|voucher|kupon|coupon|bonus|poin|points", re.I)
AIRLINES = ["Batik Air Malaysia", "Batik Air", "Malaysia Airlines", "Lion Air", "Super Air Jet", "Citilink",
            "Garuda Indonesia", "Garuda", "Pelita Air", "AirAsia", "TransNusa", "Wings Air", "Scoot", "Singapore Airlines", "Sriwijaya"]

def maskapai(pre, post):
    best = None
    for name in AIRLINES:
        for x in re.finditer(re.escape(name), pre, re.I):
            if best is None or x.start() > best[0] or (x.start() == best[0] and len(name) > len(best[1])):
                best = (x.start(), name)
    if best: return best[1]
    for name in AIRLINES:
        if re.search(re.escape(name), post, re.I): return name
    return None

# Panel filter / bilah urut OTA (slider jam & durasi, 'Harga per orang', dst) BUKAN kartu hasil. Teks sebelum penanda ini dibuang.
FILTER_RE = re.compile(r"waktu (?:kedatangan|keberangkatan)|durasi (?:transit|perjalanan)|kota transit|harga per orang|hingga usd|rentang harga|disarankan"
                       r"|\d{1,2}[:.]\d{2}\s*[\u2013-]\s*\d{1,2}[:.]\d{2}|\d+j(?:\s*\d+m)?\s*[\u2013-]\s*\d+j", re.I)
BOT_RE = re.compile(r"human verification|confirm you are human|verifikasi keamanan|security check|captcha|are you a robot|just a moment|tunggu sebentar|access denied|unusual traffic|cloudflare", re.I)
USD_RE = re.compile(r"USD[\s\xa0]*(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?)")
KURS_USD = float(os.environ.get("KURS_USD_IDR") or 0)      # opsional, diisi SENDIRI (Agoda kadang menampilkan USD)

def konversi_usd(text):
    if not KURS_USD: return text
    return USD_RE.sub(lambda m: "Rp" + f"{int(float(m.group(1).replace(',', '')) * KURS_USD):,}".replace(",", ".") + "~kurs", text)

STOPS_RE = re.compile(r"langsung|non-?stop|direct|\d+\s*(?:transit|stops?)", re.I)
BANNER_RE = re.compile(r"turun|di ?bawah|notifikasi|mengabari|penawaran|alert|below|cashback|hemat|potongan|voucher|kupon|coupon|bonus|poin|points", re.I)

def opsi(text, mode):
    """Hasil pencarian OTA -> [(harga, durasi_menit|None, maskapai|None, bukti, transit|None)].
    Hanya KARTU HASIL: di depan harga (sejak harga sebelumnya) ada >=2 jam tayang. Banner promo/notifikasi diabaikan.
    Durasi total = durasi yang tepat diikuti 'Langsung / N transit / N stop' (BUKAN lama singgah/segmen);
    bila tidak ada, durasi terpanjang di kartu itu."""
    text = konversi_usd(text)
    out, prev_end, last_end = [], 0, -999
    for m in PRICE_RE.finditer(text):
        v = int(re.sub(r"[.,]", "", m.group(1)))
        pre = text[max(prev_end, m.start() - 450): m.start()]
        fm = list(FILTER_RE.finditer(pre))
        if fm: pre = pre[fm[-1].end():]                    # buang panel filter/slider di depan kartu
        post = text[m.end(): m.end() + 120]
        prev_end = m.end()
        if not (MIN_HARGA[mode] <= v <= 30_000_000): continue
        banner = BANNER_RE.search(text[max(0, m.start() - 90): m.start()])
        times = list(TIME_RE.finditer(pre))
        if len(times) < 2:
            # harga ke-2 pada kartu yang sama (harga coret/diskon): pakai yang terendah
            if out and not banner and m.start() - last_end <= 60 and v < out[-1][0]:
                out[-1] = (v,) + out[-1][1:]; last_end = m.end()
            continue
        if banner: continue
        durs = [(y.end(), to_min(y)) for y in DUR_RE.finditer(pre) if y.start() >= times[0].start() and to_min(y)]
        adj = [d for e, d in durs if STOPS_RE.match(pre[e:e + 16].lstrip())]
        dur = adj[-1] if adj else (max(d for e, d in durs) if durs else None)
        if dur is None and not STOPS_RE.search(pre): continue
        sm = list(STOPS_RE.finditer(pre))
        stop = sm[-1].group(0).strip().capitalize() if sm else None
        bukti = re.sub(r"\s+", " ", text[max(0, m.start() - 170): m.end() + 15]).strip()
        out.append((v, dur, maskapai(pre, post), bukti, stop)); last_end = m.end()
    if len(out) >= 5:                                   # buang harga yang jauh di bawah median halaman (salah baca)
        med = sorted(x[0] for x in out)[len(out) // 2]
        out = [x for x in out if x[0] >= 0.35 * med]
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
FLIGHT_SITES = ["trip", "traveloka", "agoda", "airasia"]   # urutan: Trip.com dulu; OTA berikutnya dipakai bila yang sebelumnya kosong
TRIP_ID = {"DPS": 723, "PKU": 5604}                        # id kota Trip.com yang sudah diketahui (dari URL contoh)

def trip_url(o, d, date):
    u = f"https://id.trip.com/flights/showfarefirst?dcity={o.lower()}&acity={d.lower()}&ddate={date.isoformat()}"
    if o in TRIP_ID: u += f"&dcityid={TRIP_ID[o]}"
    if d in TRIP_ID: u += f"&acityid={TRIP_ID[d]}"
    return u + "&triptype=ow&class=y&lowpricesource=searchform&quantity=1&searchboxarg=t&nonstoponly=off&locale=id-ID&curr=IDR"
# batikair.com: pencarian berupa form tanpa deep link -> harga Batik Air diisi manual di dashboard.

def flight_url(site, o, d, date):
    iso, dmy, dmy2 = date.isoformat(), date.strftime("%d-%m-%Y"), date.strftime("%d/%m/%Y")
    return {
        "traveloka": f"https://www.traveloka.com/id-id/flight/fullsearch?ap={o}.{d}&dt={dmy}.null&ps=1.0.0&sc=ECONOMY",
        "agoda": (f"https://www.agoda.com/id-id/flights/results?departureFrom={o}&departureFromType=1&arrivalTo={d}"
                  f"&arrivalToType=1&departDate={iso}&adults=1&children=0&infants=0&cabinType=Economy&tripType=OneWay&currencyCode=IDR&currency=IDR"),
        "trip": trip_url(o, d, date),
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

SHOTS, DUMPS = [0], [0]
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

def simpan_teks(nama, text, url=""):
    """Simpan teks halaman (untuk memperbaiki pembacaan bila OTA berubah). Diunggah sebagai artifact."""
    if DUMPS[0] < 12 or nama.startswith("cek_"):
        DUMPS[0] += 1
        with open(f"teks_{nama}.txt", "w", encoding="utf-8") as f: f.write(url + "\n\n" + text[:8000])

async def peluncur(p):
    args = []
    if os.environ.get("BROWSER_CHANNEL", "chrome") != "chromium":       # Chrome sudah terpasang di runner GitHub
        try:
            br = await p.chromium.launch(channel="chrome", headless=True, args=args)
            print("browser: Google Chrome", flush=True); return br
        except Exception as e:
            print("Chrome tidak tersedia, pakai Chromium:", str(e)[:70], flush=True)
    return await p.chromium.launch(headless=HEADLESS, args=args)

async def konteks(br):
    ctx = await br.new_context(locale="id-ID", timezone_id="Asia/Jakarta", user_agent=UA, viewport={"width": 1366, "height": 900})
    return ctx

DIAG_N, CEK_MODE, MAKS_TUNGGU = [0], [False], [28]

async def diagnosa(page, text, site):
    """Cetak ringkasan halaman yang kosong ke LOG (agar bisa langsung ditempel, tanpa unduh artifact)."""
    if DIAG_N[0] >= 8 and not CEK_MODE[0]: return
    DIAG_N[0] += 1
    try: judul = await page.title()
    except Exception: judul = "?"
    awal = re.sub(r"\s+", " ", text[:500])
    m = PRICE_RE.search(text)
    sekitar = re.sub(r"\s+", " ", text[max(0, m.start() - 250): m.end() + 60]) if m else "(tidak ada angka Rp/IDR)"
    print(f"     [diag {site}] url={page.url[:150]} | judul={judul[:80]!r} | teks={len(text)} huruf | harga={len(PRICE_RE.findall(text))} | jam={len(TIME_RE.findall(text))}", flush=True)
    print(f"     [diag {site}] awal teks: {awal}", flush=True)
    print(f"     [diag {site}] sekitar harga pertama: {sekitar}", flush=True)

async def tunggu_hasil(page, mode, maks=None):
    """Tunggu sampai kartu hasil (harga + jam tayang) muncul; scroll agar lazy-load jalan."""
    maks = maks or MAKS_TUNGGU[0]
    t0, text = time.time(), ""
    while time.time() - t0 < maks:
        await page.wait_for_timeout(3000)
        try: text = await page.inner_text("body")
        except Exception: continue
        if len(text) < 2500 and BOT_RE.search(text): return text      # halaman verifikasi anti-bot
        if opsi(text, mode):
            await page.wait_for_timeout(5000)          # hasil OTA datang bertahap -> beri waktu opsi lain muncul
            try: text = await page.inner_text("body")
            except Exception: pass
            return text
        try: await page.mouse.wheel(0, 800)
        except Exception: pass
    return text

async def scrape_leg(page, mode, o, d, date, site=None):
    """Return dict(price, n, dur, airline, bukti, url, batik) atau None."""
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
                bukti = re.sub(r"\s+", " ", text[max(0, m.start() - 60): m.end()]).strip()
                return dict(price=int(m.group(1).replace(".", "")), n=1, dur=dur, airline=None, bukti=bukti, url=u, batik=None, cepat=None, stops=None, ops=[])
        return None
    url = flight_url(site, o, d, date) if mode == "flight" else train_url(site, o, d, date)
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    text = await tunggu_hasil(page, mode)
    if len(text) < 2500 and BOT_RE.search(text):
        print(f"     {site or mode}: DIBLOKIR halaman verifikasi anti-bot -> situs dilewati 6 jam (tidak dicoba ditembus)", flush=True)
        JEDA[site] = time.time() + 6 * 3600
        return None
    ops = opsi(text, mode)
    if not ops:
        blk = " (kemungkinan diblokir/CAPTCHA)" if re.search(r"captcha|robot|access denied|verify you|unusual traffic", text, re.I) else ""
        print(f"     {site or mode}: kosong{blk}", flush=True)
        await diagnosa(page, text, site or mode)
        simpan_teks(f"{site or mode}_{o}_{d}_{date}", text, page.url)
        if SHOTS[0] < 12:
            SHOTS[0] += 1
            await page.screenshot(path=f"gagal_{site or mode}_{o}_{d}_{date}.png")
        return None
    best = min(ops, key=lambda t: t[0])
    batik = min([x for x in ops if x[2] and "batik" in x[2].lower()], key=lambda t: t[0], default=None)
    cepat = min([x for x in ops if x[1]], key=lambda t: (t[1], t[0]), default=None)   # opsi dengan durasi terpendek
    return dict(price=best[0], n=len(ops), dur=best[1], airline=best[2], bukti=best[3], url=page.url, batik=batik, cepat=cepat, stops=best[4], ops=ops)

def baris(mode, o, d, ds, site, r):
    """Baris ke dashboard: opsi TERMURAH + opsi TERCEPAT + (bila ada) opsi Batik Air termurah."""
    def b(x, suffix=""):
        return dict(mode=mode, o=o, d=d, date=ds, price=x[0], dur=x[1], airline=x[2], bukti=x[3], stops=x[4] if len(x) > 4 else None,
                    url=r["url"], site=(site or mode) + suffix)
    rows = [dict(mode=mode, o=o, d=d, date=ds, price=r["price"], dur=r["dur"], airline=r["airline"], bukti=r["bukti"],
                 stops=r.get("stops"), url=r["url"], site=site or mode)]
    if r.get("cepat"): rows.append(b(r["cepat"], ".cepat"))
    if r.get("batik"): rows.append(b(r["batik"], ".batik"))
    return rows

def push(batch):
    """Kirim hasil scraping ke hosting (DomCloud) lewat POST JSON."""
    if not (INGEST_URL and batch): return
    req = urllib.request.Request(INGEST_URL, json.dumps({"key": INGEST_KEY, "rows": batch}).encode(),
                                 {"Content-Type": "application/json"})
    try: print("  push ke hosting:", urllib.request.urlopen(req, timeout=60).read()[:60], flush=True)
    except Exception as e: print("  push gagal:", e, flush=True)

def fmt(r):
    return (f"Rp{r['price']:,}  " + (f"{r['dur'] // 60}j{r['dur'] % 60:02d}m" if r["dur"] else "durasi ?")
            + (f"  {r['airline']}" if r["airline"] else ""))

GAGAL, JEDA, HASIL = {}, {}, {}     # gagal berturut per situs, jeda situs, hasil {(moda,asal,tujuan,tgl): (harga, situs)}

async def proses(page, db, t):
    """t = (moda, asal, tujuan, tanggal, [situs...]): coba situs berurutan sampai ada hasil; catat & kirim ke dashboard."""
    mode, o, d, date, sites = t
    ds = date.isoformat() if date else "*"
    for site in sites:
        if site and time.time() < JEDA.get(site, 0): continue          # situs sedang dijeda
        try:
            r = await scrape_leg(page, mode, o, d, date, site)
        except Exception as e:
            print("  ERR", site or mode, o, d, ds, str(e)[:60], flush=True); r = None
        if r:
            GAGAL[site] = 0
            db.execute("INSERT INTO prices VALUES(?,?,?,?,?,?,?)", (dt.datetime.now().isoformat(), mode, o, d, ds, r["price"], r["n"]))
            db.commit()
            push(baris(mode, o, d, ds, site, r))
            print(f"  {mode:6} {(site or ''):9} {o}->{d} {ds}  {fmt(r)}", flush=True)
            HASIL[(mode, o, d, ds)] = min(HASIL.get((mode, o, d, ds), (10**12, "")), (r["price"], site or mode))
            return True
        GAGAL[site] = GAGAL.get(site, 0) + 1
        if site and GAGAL[site] >= 8:
            JEDA[site] = time.time() + 1800; GAGAL[site] = 0
            print(f"  !! {site} gagal 8x berturut-turut -> dijeda 30 menit", flush=True)
    if mode == "bus": print(f"  {mode:6} {o}->{d} {ds}  (tidak ada harga)", flush=True)
    return False

async def paralel(br, tugas, db):
    """Jalankan tugas dengan WORKERS halaman browser sekaligus."""
    q = asyncio.Queue()
    for t in tugas: q.put_nowait(t)
    async def kerja():
        ctx = await konteks(br); page = await ctx.new_page()
        while True:
            try: t = q.get_nowait()
            except asyncio.QueueEmpty: break
            await proses(page, db, t)
            await asyncio.sleep(random.uniform(1.5, 4))                # jeda anti-blokir
        await ctx.close()
    await asyncio.gather(*[kerja() for _ in range(max(1, WORKERS))])

async def run_once():
    db = init_db(); legs = needed_legs(); HASIL.clear()
    bus = [t for t in legs if t[0] == "bus"]; kereta = [t for t in legs if t[0] == "train"]; terbang = [t for t in legs if t[0] == "flight"]
    print(f"[{dt.datetime.now():%H:%M}] {TGL_MULAI} s/d {TGL_AKHIR}: {len(bus)} rute bus, {len(kereta)} kereta, {len(terbang)} pesawat (leg-tanggal)", flush=True)
    async with async_playwright() as p:
        br = await peluncur(p)
        print("== FASE 0: bus & kereta", flush=True)
        await paralel(br, [(m, o, d, dte, [None]) for m, o, d, dte in bus] + [(m, o, d, dte, TRAIN_SITES) for m, o, d, dte in kereta], db)
        # Probe: 2 tanggal per rute-arah. Rute tanpa hasil di semua OTA dilewati (hemat waktu; dicoba lagi di putaran berikutnya)
        tgl_rute = {}
        for m, o, d, dte in terbang: tgl_rute.setdefault((o, d), []).append(dte)
        probe = []
        for (o, d), lst in tgl_rute.items():
            lst.sort(); probe += [("flight", o, d, lst[len(lst) // 4], FLIGHT_SITES), ("flight", o, d, lst[3 * len(lst) // 4], FLIGHT_SITES)]
        print(f"== PROBE: {len(tgl_rute)} rute-arah pesawat", flush=True)
        await paralel(br, probe, db)
        hidup = {(o, d) for (m, o, d, ds) in HASIL if m == "flight"}
        for rd in sorted(set(tgl_rute) - hidup): print(f"  !! rute {rd[0]}->{rd[1]} tidak terbaca di semua OTA -> dilewati putaran ini", flush=True)
        print("== FASE 1: pesawat, setiap tanggal, OTA pertama yang berhasil (Trip.com dulu)", flush=True)
        await paralel(br, [(m, o, d, dte, FLIGHT_SITES) for m, o, d, dte in terbang
                           if (o, d) in hidup and ("flight", o, d, dte.isoformat()) not in HASIL], db)
        per_rute = {}
        for (m, o, d, ds), (harga, site) in HASIL.items():
            if m == "flight": per_rute.setdefault((o, d), []).append((harga, ds, site))
        tugas = [("flight", o, d, dt.date.fromisoformat(ds), [s2])
                 for (o, d), lst in per_rute.items() for harga, ds, site in sorted(lst)[:K_VERIFIKASI]
                 for s2 in FLIGHT_SITES if s2 != site]
        print(f"== FASE 2: verifikasi silang {len(tugas)} halaman ({K_VERIFIKASI} tanggal termurah per rute di OTA lain)", flush=True)
        await paralel(br, tugas, db)
        await br.close()
    print("== SELESAI. Tanggal pergi/pulang termurah per skenario: lihat dashboard.", flush=True)

async def cek(mode, o, d, ds):
    """Uji cepat satu rute di semua sumber:  python tiketscout.py cek flight DPS PKU 2027-03-01"""
    CEK_MODE[0] = True; MAKS_TUNGGU[0] = 45
    date = None if mode == "bus" else dt.date.fromisoformat(ds)
    sites = FLIGHT_SITES if mode == "flight" else TRAIN_SITES if mode == "train" else [None]
    async with async_playwright() as p:
        br = await peluncur(p); page = await (await konteks(br)).new_page()
        for site in sites:
            try: r = await scrape_leg(page, mode, o, d, date, site)
            except Exception as e: r = None; print("  ERR", site, str(e)[:80], flush=True)
            print(f"CEK {mode} {o}->{d} {ds} [{site or 'bus'}]: " + (fmt(r) if r else "KOSONG"), flush=True)
            if r:
                print(f"      termurah, teks kartu: {r['bukti']}", flush=True)
                for x in sorted(r["ops"])[:5]:
                    print(f"      opsi: Rp{x[0]:,} | {x[1] or '?'} mnt | {x[2] or '-'} | {x[4] or '-'}", flush=True)
                push(baris(mode, o, d, ds if date else "*", site, r))
            try:
                simpan_teks(f"cek_{mode}_{site or 'bus'}", await page.inner_text("body"), page.url)
                await page.screenshot(path=f"cek_{mode}_{site or 'bus'}.png")
            except Exception: pass
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
