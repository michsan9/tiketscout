"""
TiketScout v2 - Singaraja -> Pekanbaru (mudik Idul Fitri 2027)

ALUR
 1. Cari tiket pesawat ke kota transit:   via Denpasar : DPS->CGK, DPS->KUL (KLIA), DPS->SIN
                                          via Surabaya : SUB->PKU, SUB->KUL, SUB->SIN
 2. Dari kota transit, cari tiket lanjutan: CGK->PKU, KUL->PKU, SIN->PKU
 3. Setelah semua data terbaca, susun BEBERAPA skenario termurah. Penerbangan lanjutan harus berangkat
    minimal MIN_TRANSIT menit (2 jam) dan maksimal MAX_TRANSIT (12 jam) setelah penerbangan pertama mendarat (zona waktu bandara dihitung).
 4. Travel Singaraja->Denpasar Rp145.000, bus Singaraja->Surabaya Rp250.000 (ditetapkan pengguna).
 Pergi DAN pulang dicari di setiap tanggal (default 1-31 Maret 2027; ubah lewat variabel TGL_MULAI / TGL_AKHIR).

PAKAI
    python tiketscout.py once                         # cari semua tanggal, susun skenario, kirim ke dashboard
    python tiketscout.py cek DPS CGK 2027-03-05       # uji 1 rute di semua OTA (lihat kartu yang terbaca)
    python tiketscout.py cek url "https://..."         # uji URL pencarian OTA lain (salin dari browser Anda)
    OTA_TAMBAHAN='{"namasitus":"https://...{o}...{iso}"}'  # tambah OTA lain tanpa ubah kode (lihat isi_templat)
    python tiketscout.py login                        # PC sendiri: buka browser, selesaikan verifikasi 'saya bukan robot' SECARA MANUAL
"""
import asyncio, json, os, random, re, sys, time, urllib.request, datetime as dt
from urllib.parse import quote
from playwright.async_api import async_playwright

# ============================ KONFIGURASI ============================
INGEST_URL = os.environ.get("INGEST_URL")          # https://domain/index.php?a=ingest
INGEST_KEY = os.environ.get("INGEST_KEY")
TGL_MULAI = dt.date.fromisoformat(os.environ.get("TGL_MULAI") or "2027-03-01")     # bisa diatur, mis. 2027-02-27 (akhir pekan sebelum cuti)
TGL_AKHIR = dt.date.fromisoformat(os.environ.get("TGL_AKHIR") or "2027-03-31")
MIN_TRANSIT = 120                                  # menit antara mendarat dan penerbangan berikutnya (minimal 2 jam)
MAX_TRANSIT = int(os.environ.get("MAX_TRANSIT") or 720)   # singgah > 12 jam dianggap tidak praktis (ubah bila perlu)
BIAYA_DARAT = {"DPS": 145_000, "SUB": 250_000}     # Singaraja -> gerbang (ditetapkan pengguna)
DURASI_DARAT = {"DPS": 240, "SUB": 630}            # menit; PENGATURAN (bukan data OTA) - ubah sesuai pengalaman Anda
NAMA = {"DPS": "Denpasar", "SUB": "Surabaya", "CGK": "Jakarta", "KUL": "Kuala Lumpur", "SIN": "Singapura", "PKU": "Pekanbaru"}
TZ = {"DPS": 8, "SUB": 7, "CGK": 7, "KUL": 8, "SIN": 8, "PKU": 7}      # selisih jam terhadap UTC (WITA/WIB/MYT/SGT)
JALUR = [("DPS", ["DPS", "CGK", "PKU"]), ("DPS", ["DPS", "KUL", "PKU"]), ("DPS", ["DPS", "SIN", "PKU"]),
         ("SUB", ["SUB", "PKU"]),        ("SUB", ["SUB", "KUL", "PKU"]), ("SUB", ["SUB", "SIN", "PKU"])]
if os.environ.get("DPS_PKU") == "1":               # opsional: tiket tunggal DPS->PKU (OTA memilihkan transitnya)
    JALUR.append(("DPS", ["DPS", "PKU"]))
FLIGHT_SITES = ["trip", "agoda", "google", "traveloka", "airasia", "batikair", "scoot"]   # cascade OTOMATIS: sumber berikutnya dipakai bila yang sebelumnya kosong/diblokir
LANGSUNG = {"batikair", "scoot"}     # situs MASKAPAI: hanya menjual penerbangan sendiri -> pembanding (cek silang / OTA_SEMUA), bukan cadangan cascade
try:   # sumber tambahan tanpa ubah kode: {"namasitus": "https://...{o}...{iso}"}  (isi dari URL hasil pencarian di browser Anda)
    EXTRA = {k: v for k, v in json.loads(os.environ.get("OTA_TAMBAHAN") or "{}").items() if re.fullmatch(r"[a-z0-9]+", k) and str(v).startswith("https://")}
except Exception as e:
    print("OTA_TAMBAHAN bukan JSON valid, diabaikan:", e, flush=True); EXTRA = {}
FLIGHT_SITES += [k for k in EXTRA if k not in FLIGHT_SITES]
NOROUTE = {}     # (situs, asal, tujuan) -> berapa kali kosong; situs maskapai dilewati setelah 2x kosong (maskapai itu tidak terbang di rute tsb)
OTA_SEMUA = os.environ.get("OTA_SEMUA") == "1"             # 1 = baca SEMUA OTA untuk tiap rute-tanggal lalu gabungkan (lebih lama)
WORKERS = int(os.environ.get("WORKERS") or 4)
K_SILANG = int(os.environ.get("K_SILANG") or 3)            # cek silang: K tanggal termurah per rute dibaca juga di sumber LAIN (0 = matikan)              # halaman browser paralel
KURS = {"USD": float(os.environ.get("KURS_USD_IDR") or 0), "MYR": float(os.environ.get("KURS_MYR_IDR") or 0), "SGD": float(os.environ.get("KURS_SGD_IDR") or 0)}   # opsional, diisi SENDIRI bila situs menampilkan mata uang asing
PROFIL_DIR = os.environ.get("PROFIL_DIR")                  # opsional: profil browser tetap (untuk verifikasi manual di PC sendiri)
HEADLESS = os.environ.get("HEADED") != "1"
MIN_HARGA, MAX_HARGA = 300_000, 30_000_000
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# ============================ URL PENCARIAN ============================
TRIP_ID = {"DPS": 723, "PKU": 5604}                # id kota Trip.com yang diketahui (dari URL contoh)

BLN = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

def tgl_txt(date):                                   # 10 Oct 2026 -> 10%20Oct%202026
    return f"{date.day:02d}%20{BLN[date.month - 1]}%20{date.year}"

def isi_templat(t, o, d, date):
    """Placeholder: {o} {d} {o_lc} {d_lc} {iso}=2027-03-05 {dmy}=05-03-2027 {dmy2}=05/03/2027 {ymd}=20270305 {y} {m} {day}"""
    for k, v in {"{o}": o, "{d}": d, "{o_lc}": o.lower(), "{d_lc}": d.lower(), "{iso}": date.isoformat(), "{dmy}": date.strftime("%d-%m-%Y"),
                 "{dmy2}": date.strftime("%d/%m/%Y"), "{dmy2e}": date.strftime("%d%%2F%m%%2F%Y"), "{txt}": tgl_txt(date), "{txt_p7}": tgl_txt(date + dt.timedelta(days=7)), "{ymd}": date.strftime("%Y%m%d"), "{y}": str(date.year), "{m}": f"{date.month:02d}", "{day}": f"{date.day:02d}"}.items():
        t = t.replace(k, v)
    return t

def flight_url(site, o, d, date):
    if site in EXTRA: return isi_templat(EXTRA[site], o, d, date)
    iso, dmy, dmy2 = date.isoformat(), date.strftime("%d-%m-%Y"), date.strftime("%d/%m/%Y")
    if site == "batikair":       # flights.batikair.com: Jtype=1 (sekali jalan; contoh pengguna memakai Jtype=2 untuk pulang-pergi) - uji dengan 'cek'
        e = date.strftime("%d%%2F%m%%2F%Y")
        return (f"https://flights.batikair.com/default.aspx?aid=231&Jtype=1&depCity={o}&arrCity={d}&depDate={e}&arrDate={e}&currency=&adult1=1&child1=0&infant1=0"
                "&culture=en-GB&df=UK&afid=0&b2b=0&St=fa&DFlight=false&roomcount=1")
    if site == "scoot":          # booking.flyscoot.com: format 'return' dari contoh pengguna; tanggal pulang = berangkat + 7 hari (hanya daftar penerbangan pergi yang dibaca)
        return (f"https://booking.flyscoot.com/book/flight/return/{o}/{tgl_txt(date)}/{d}/{d}/{tgl_txt(date + dt.timedelta(days=7))}/{o}"
                "?adult=1&child=0&infant=0&cur=IDR&culture=en-sg")
    if site == "trip":
        u = f"https://id.trip.com/flights/showfarefirst?dcity={o.lower()}&acity={d.lower()}&ddate={iso}"
        if o in TRIP_ID: u += f"&dcityid={TRIP_ID[o]}"
        if d in TRIP_ID: u += f"&acityid={TRIP_ID[d]}"
        return u + "&triptype=ow&class=y&lowpricesource=searchform&quantity=1&searchboxarg=t&nonstoponly=off&locale=id-ID&curr=IDR"
    if site == "google":
        return "https://www.google.com/travel/flights?q=" + quote(f"Flights from {o} to {d} on {iso} one way") + "&curr=IDR&hl=id"
    return {
        "agoda": (f"https://www.agoda.com/id-id/flights/results?departureFrom={o}&departureFromType=1&arrivalTo={d}&arrivalToType=1"
                  f"&departDate={iso}&adults=1&children=0&infants=0&cabinType=Economy&tripType=OneWay&currencyCode=IDR&currency=IDR"),
        "traveloka": f"https://www.traveloka.com/id-id/flight/fullsearch?ap={o}.{d}&dt={dmy}.null&ps=1.0.0&sc=ECONOMY",
        "airasia": (f"https://www.airasia.com/flights/search/?origin={o}&destination={d}&departDate={dmy2}"
                    "&tripType=O&adult=1&child=0&infant=0&locale=id-id&currency=IDR"),
    }[site]

# ============================ MEMBACA KARTU HASIL ============================
PRICE_RE = re.compile(r"(?:Rp|IDR)[\s\xa0]*([\d]{1,3}(?:[.,]\d{3})+)", re.I)
CUR_RE = re.compile(r"\b(USD|MYR|SGD|RM)[\s\xa0]*\$?[\s\xa0]*(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?)")
TIME_RE = re.compile(r"(?<![\d.,])(?:[01]?\d|2[0-3])[:.][0-5]\d(?!\d)")                 # 12:25 / 12.25 / 5.20
DUR_RE = re.compile(r"(\d{1,3})\s*(?:jam|j|h|hrs?|hours?)(?![a-z])(?:\s*(\d{1,2})\s*(?:menit|mnt|m|mins?|minutes?)(?![a-z]))?", re.I)
STOPS_RE = re.compile(r"langsung|non-?stop|direct|\d+\s*(?:transit|stops?|pemberhentian)", re.I)
BANNER_RE = re.compile(r"turun|di ?bawah|notifikasi|mengabari|penawaran|alert|below|cashback|hemat|potongan|voucher|kupon|coupon|bonus|poin|points", re.I)
FILTER_RE = re.compile(r"waktu (?:kedatangan|keberangkatan)|durasi (?:transit|perjalanan)|kota transit|harga per orang|hingga usd|rentang harga|disarankan"
                       r"|00[:.]00\s*[\u2013-]\s*(?:24[:.]00|23[:.]59)|\d+j(?:\s*\d+m)?\s*[\u2013-]\s*\d+j", re.I)
BOT_RE = re.compile(r"human verification|confirm you are human|verifikasi keamanan|security check|captcha|are you a robot|just a moment|tunggu sebentar|access denied|unusual traffic|cloudflare", re.I)
AIRLINES = ["Batik Air Malaysia", "Batik Air", "Malaysia Airlines", "Lion Air", "Super Air Jet", "Citilink", "Garuda Indonesia", "Garuda",
            "Pelita Air", "AirAsia", "TransNusa", "Wings Air", "Scoot", "Singapore Airlines", "Sriwijaya", "Jetstar", "Malindo"]

def konversi_usd(t):
    """Harga bermata uang asing (USD/MYR/SGD/RM) -> Rp, HANYA bila kursnya Anda isi sendiri; ditandai '~kurs'."""
    if not any(KURS.values()): return t
    def ganti(m):
        k = KURS.get("MYR" if m.group(1) == "RM" else m.group(1), 0)
        return m.group(0) if not k else "Rp" + f"{int(float(m.group(2).replace(',', '')) * k):,}".replace(",", ".") + "~kurs"
    return CUR_RE.sub(ganti, t)

def to_min(m):
    v = int(m.group(1)) * 60 + (int(m.group(2)) if m.group(2) else 0)
    return v if 20 <= v <= 3600 else None

def jam(s):                                         # '5.20' -> '05:20'
    h, m = re.split(r"[:.]", s); return f"{int(h):02d}:{m}"

ALIAS = {"Lion": "Lion Air", "Super Air": "Super Air Jet", "Indonesia AirAsia": "AirAsia", "Air Asia": "AirAsia"}

def maskapai(t):
    best = None
    for n in AIRLINES + list(ALIAS):
        for x in re.finditer(re.escape(n), t, re.I):
            if best is None or x.start() < best[0] or (x.start() == best[0] and len(n) > len(best[1])): best = (x.start(), n)
    return ALIAS.get(best[1], best[1]) if best else None

def kartu(t):
    """Teks SATU kartu hasil -> dict(harga, dep, arr, dur, maskapai, transit, bukti) atau None.
    Kartu valid: >=2 jam tayang (berangkat, tiba) + harga. Panel filter/slider & harga di banner notifikasi ditolak."""
    t = konversi_usd(t)
    fm = list(FILTER_RE.finditer(t))
    if fm: t = t[fm[-1].end():]                       # buang panel filter/slider yang ikut terambil
    times = list(TIME_RE.finditer(t))
    if len(times) < 2: return None
    harga = []
    for m in PRICE_RE.finditer(t):
        if BANNER_RE.search(t[max(0, m.start() - 90): m.start()]): continue          # 'harga turun di bawah Rp...' dll
        v = int(re.sub(r"[.,]", "", m.group(1)))
        if MIN_HARGA <= v <= MAX_HARGA: harga.append(v)
    if not harga: return None
    durs = [(y.end(), to_min(y)) for y in DUR_RE.finditer(t) if y.start() >= times[0].start() and to_min(y)]
    adj = [d for e, d in durs if STOPS_RE.match(t[e:e + 16].lstrip())]                # durasi TOTAL = yang tepat diikuti 'Langsung/N transit'
    dur = adj[-1] if adj else (max(d for e, d in durs) if durs else None)
    sm = list(STOPS_RE.finditer(t))
    return dict(harga=min(harga), dep=jam(times[0].group(0)), arr=jam(times[1].group(0)), dur=dur, maskapai=maskapai(t),
                transit=sm[-1].group(0).strip().capitalize() if sm else None, bukti=re.sub(r"\s+", " ", t).strip()[:170])

def cocokkan(c, o, d):
    """Verifikasi silang kartu: berangkat + durasi harus = jam tiba (selisih zona waktu o->d dihitung).
    Cocok (+-10 mnt) -> durasi dipakai dari jam tayang (tepat). Tidak cocok -> kartu DIBUANG (salah baca). Tanpa durasi -> dihitung dari jam."""
    base = dt.date(2027, 1, 1); dep = utc(o, base, c["dep"]); best = None
    for k in range(3):
        m = int((utc(d, base + dt.timedelta(days=k), c["arr"]) - dep).total_seconds() // 60)
        if m <= 0: continue
        if c["dur"] is None: best = (m, k); break
        if best is None or abs(m - c["dur"]) < abs(best[0] - c["dur"]): best = (m, k)
    if best is None or (c["dur"] is not None and abs(best[0] - c["dur"]) > 10): return None
    return dict(c, dur=best[0])

def kartu_dari_teks(text):
    """Cadangan bila DOM tidak memberi kartu: potong teks halaman per harga."""
    text = konversi_usd(text); out, prev = [], 0
    for m in PRICE_RE.finditer(text):
        pre = text[max(prev, m.start() - 450): m.start()]; prev = m.end()
        fm = list(FILTER_RE.finditer(pre))
        if fm: pre = pre[fm[-1].end():]
        c = kartu(pre + " " + m.group(0))
        if c: out.append(c)
    return out

# JavaScript di dalam halaman: ambil elemen KARTU (elemen terbesar yang masih memuat 1-2 harga + >=2 jam tayang; bila 2+ kartu menyatu, dipecah)
JS_KARTU = r"""() => {
  const pr = /(?:Rp|IDR|USD)[\s\u00a0]*\d[\d.,]*/g, pr1 = /(?:Rp|IDR|USD)[\s\u00a0]*\d/;
  const tm = /(?<![\d.,])(?:[01]?\d|2[0-3])[:.][0-5]\d(?!\d)/g;
  const txt = el => (el.innerText !== undefined ? el.innerText : el.textContent) || '';
  const memo = new Map();
  const cand = el => {
    if (memo.has(el)) return memo.get(el);
    let ok = false;
    if (el.textContent.length <= 6000) {
      const t = txt(el);
      if (t.length >= 40 && t.length <= 1400) {
        ok = pr1.test(t) && (t.match(tm) || []).length >= 2;
      }
    }
    memo.set(el, ok); return ok;
  };
  const maks = el => {                       // kandidat terbesar di bawah el
    const out = [];
    for (const c of el.children) {
      if (!pr1.test(c.textContent)) continue;
      if (cand(c)) out.push(c); else out.push(...maks(c));
    }
    return out;
  };
  const kartu = el => { const k = maks(el); return k.length >= 2 ? k.flatMap(kartu) : [el]; };
  return maks(document.body).flatMap(kartu).map(txt);
}"""

async def ambil_kartu(page, text):
    try: tk = await page.evaluate(JS_KARTU)
    except Exception: tk = []
    cards = [c for c in (kartu(x) for x in tk) if c]
    return cards if cards else kartu_dari_teks(text)

# ============================ BROWSER ============================
class _Bersama:                                     # konteks profil tetap: close() tidak menutup profil
    def __init__(self, ctx): self.ctx = ctx
    async def new_page(self): return await self.ctx.new_page()
    async def close(self): pass

class _Profil:
    def __init__(self, ctx): self.ctx = ctx
    async def new_context(self, **k): return _Bersama(self.ctx)
    async def close(self): await self.ctx.close()

async def peluncur(p):
    opsi = dict(locale="id-ID", timezone_id="Asia/Jakarta", user_agent=UA, viewport={"width": 1366, "height": 900})
    if PROFIL_DIR:                                  # profil tetap: cookie hasil verifikasi manual tersimpan di folder ini
        for ch in (["chrome"] if os.environ.get("BROWSER_CHANNEL", "chrome") != "chromium" else []) + [None]:
            try:
                ctx = await p.chromium.launch_persistent_context(PROFIL_DIR, headless=HEADLESS, channel=ch, **opsi)
                print(f"browser: profil tetap {PROFIL_DIR} ({ch or 'chromium'})", flush=True); return _Profil(ctx)
            except Exception as e: print("profil gagal:", str(e)[:70], flush=True)
    if os.environ.get("BROWSER_CHANNEL", "chrome") != "chromium":
        try:
            br = await p.chromium.launch(channel="chrome", headless=True); print("browser: Google Chrome", flush=True); return br
        except Exception as e: print("Chrome tidak tersedia, pakai Chromium:", str(e)[:70], flush=True)
    return await p.chromium.launch(headless=HEADLESS)

async def konteks(br):
    return await br.new_context(locale="id-ID", timezone_id="Asia/Jakarta", user_agent=UA, viewport={"width": 1366, "height": 900})

SHOTS, DUMPS, DIAG_N, CEK_MODE, MAKS_TUNGGU = [0], [0], [0], [False], [30]

def simpan_teks(nama, text, url=""):
    if DUMPS[0] < 12 or CEK_MODE[0]:
        DUMPS[0] += 1
        with open(f"teks_{nama}.txt", "w", encoding="utf-8") as f: f.write(url + "\n\n" + text[:8000])

async def diagnosa(page, text, site):
    if DIAG_N[0] >= 8 and not CEK_MODE[0]: return
    DIAG_N[0] += 1
    try: judul = await page.title()
    except Exception: judul = "?"
    m = PRICE_RE.search(text)
    sekitar = re.sub(r"\s+", " ", text[max(0, m.start() - 250): m.end() + 60]) if m else "(tidak ada angka Rp/IDR)"
    print(f"     [diag {site}] url={page.url[:150]} | judul={judul[:80]!r} | teks={len(text)} huruf | harga={len(PRICE_RE.findall(text))} | jam={len(TIME_RE.findall(text))}", flush=True)
    print(f"     [diag {site}] awal teks: {re.sub(chr(10), ' ', text[:400])}", flush=True)
    print(f"     [diag {site}] sekitar harga pertama: {sekitar}", flush=True)

async def tunggu_hasil(page, maks=None):
    """Tunggu kartu hasil muncul (scroll agar lazy-load jalan). Halaman verifikasi anti-bot -> berhenti cepat."""
    maks = maks or MAKS_TUNGGU[0]; t0, text = time.time(), ""
    while time.time() - t0 < maks:
        await page.wait_for_timeout(3000)
        try: text = await page.inner_text("body")
        except Exception: continue
        if len(text) < 2500 and BOT_RE.search(text): return text, []
        cards = await ambil_kartu(page, text)
        if cards:
            await page.wait_for_timeout(5000)           # hasil OTA datang bertahap -> tunggu opsi lain
            try: text = await page.inner_text("body")
            except Exception: pass
            return text, await ambil_kartu(page, text) or cards
        try: await page.mouse.wheel(0, 800)
        except Exception: pass
    return text, []

async def muat_lebih(page, cards):
    """Hasil OTA sering dimuat bertahap: scroll ke bawah dan klik 'Lihat lebih banyak' (seperti pengguna) sampai kartu tidak bertambah."""
    for _ in range(3):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.get_by_role("button", name=re.compile(r"lihat lebih banyak|tampilkan lebih banyak|penerbangan lainnya|lihat semua|muat lebih|more flights|show more|load more", re.I)).first.click(timeout=1200)
        except Exception: pass
        await page.wait_for_timeout(2000)
        try: baru = await ambil_kartu(page, await page.inner_text("body"))
        except Exception: break
        if len(baru) <= len(cards): break
        cards = baru
    return cards

GAGAL, JEDA = {}, {}

async def tutup_consent(page):
    """Halaman persetujuan cookie Google (bukan verifikasi bot): klik 'Tolak semua' seperti pengguna biasa."""
    try:
        await page.wait_for_timeout(1500)
        if re.search(r"sebelum anda melanjutkan|before you continue", await page.inner_text("body"), re.I):
            await page.get_by_role("button", name=re.compile(r"tolak semua|reject all", re.I)).first.click(timeout=4000)
            await page.wait_for_timeout(2500)
    except Exception: pass

async def baca(page, site, o, d, date):
    """Satu halaman pencarian OTA -> dict(cards, url, site) atau None."""
    await page.goto(flight_url(site, o, d, date), wait_until="domcontentloaded", timeout=60000)
    if site == "google": await tutup_consent(page)
    text, cards = await tunggu_hasil(page)
    if cards: cards = await muat_lebih(page, cards)
    if len(text) < 2500 and BOT_RE.search(text):
        print(f"     {site}: isi halaman: {re.sub(chr(10), ' ', text[:160])!r}", flush=True)
        print(f"     {site}: halaman VERIFIKASI ANTI-BOT -> situs dilewati 6 jam (tidak ditembus; lihat 'login' untuk verifikasi manual di PC sendiri)", flush=True)
        JEDA[site] = time.time() + 6 * 3600
        return None
    if not cards:
        print(f"     {site}: tidak ada kartu hasil", flush=True)
        await diagnosa(page, text, site); simpan_teks(f"{site}_{o}_{d}_{date}", text, page.url)
        if SHOTS[0] < 12:
            SHOTS[0] += 1
            try: await page.screenshot(path=f"gagal_{site}_{o}_{d}_{date}.png")
            except Exception: pass
        return None
    sebelum = len(cards); cards = [x for x in (cocokkan(c, o, d) for c in cards) if x]
    if len(cards) < sebelum: print(f"     {site}: {sebelum - len(cards)} kartu dibuang (jam & durasi tidak cocok = salah baca)", flush=True)
    if not cards:
        print(f"     {site}: semua kartu tidak konsisten", flush=True); return None
    return dict(cards=cards, url=flight_url(site, o, d, date), site=site)

# ============================ PENYIMPANAN KARTU & PENGIRIMAN ============================
CARDS = {}                                          # (asal, tujuan, tanggal) -> {kunci: kartu}
FEED = []

def post(url, payload):
    if not (url and INGEST_KEY): return None
    req = urllib.request.Request(url, json.dumps(payload).encode(), {"Content-Type": "application/json"})
    try: return urllib.request.urlopen(req, timeout=90).read()[:80]
    except Exception as e: print("  kirim gagal:", e, flush=True)

def simpan_kartu(o, d, ds, r):
    kunci = CARDS.setdefault((o, d, ds), {})
    for c in r["cards"]:
        k = (c["dep"], c["dur"], c["maskapai"]); c = dict(c, sumber=r["site"], url=r["url"])
        if k not in kunci or c["harga"] < kunci[k]["harga"]: kunci[k] = c
    if len(kunci) > 30:
        for k, _ in sorted(kunci.items(), key=lambda kv: kv[1]["harga"])[30:]: del kunci[k]

def kartu_untuk(o, d, tgl):
    return sorted(CARDS.get((o, d, tgl.isoformat()), {}).values(), key=lambda c: c["harga"])

# ============================ MENYUSUN SKENARIO ============================
def utc(ap, tgl, hhmm):
    h, m = map(int, hhmm.split(":"))
    return dt.datetime.combine(tgl, dt.time(h % 24, m)) - dt.timedelta(hours=TZ[ap])

def tiba_utc(o, d, tgl, c):
    dep = utc(o, tgl, c["dep"])
    if c["dur"]: return dep + dt.timedelta(minutes=c["dur"])
    a = utc(d, tgl, c["arr"])
    while a <= dep: a += dt.timedelta(days=1)
    return a

def rakit(arah, gateway, ap, penerbangan):
    """penerbangan = [(asal, tujuan, tanggal, kartu), ...] berurutan."""
    legs, total = [], BIAYA_DARAT[gateway]
    darat = dict(t="darat", dari="Singaraja" if arah == "pergi" else NAMA[gateway], ke=NAMA[gateway] if arah == "pergi" else "Singaraja",
                 harga=BIAYA_DARAT[gateway], menit=DURASI_DARAT[gateway])
    if arah == "pergi": legs.append(darat)
    prev_tiba, lay_min = None, None
    for o, d, tgl, c in penerbangan:
        dep, tiba = utc(o, tgl, c["dep"]), tiba_utc(o, d, tgl, c)
        if prev_tiba is not None:
            lay = int((dep - prev_tiba).total_seconds() // 60); lay_min = lay if lay_min is None else min(lay_min, lay)
            legs.append(dict(t="singgah", di=o, menit=lay))
        tiba_lokal = tiba + dt.timedelta(hours=TZ[d])
        legs.append(dict(t="terbang", dari=o, ke=d, tgl=tgl.isoformat(), dep=c["dep"], arr=tiba_lokal.strftime("%H:%M"),
                         hari=(tiba_lokal.date() - tgl).days, menit=c["dur"], harga=c["harga"], maskapai=c["maskapai"], transit=c["transit"],
                         sumber=c["sumber"], url=c["url"]))
        total += c["harga"]; prev_tiba = tiba
    if arah == "pulang": legs.append(darat)
    t0, t1 = utc(penerbangan[0][0], penerbangan[0][2], penerbangan[0][3]["dep"]), prev_tiba
    waktu = DURASI_DARAT[gateway] + int((t1 - t0).total_seconds() // 60)
    seq = ap if arah == "pergi" else ap[::-1]
    return dict(arah=arah, jalur="-".join(seq), tgl=penerbangan[0][2].isoformat(), total=total, waktu=waktu, layover=lay_min, data=dict(legs=legs))

def semua_skenario(arah):
    """Untuk tiap jalur & tanggal: gabungkan kartu; penerbangan lanjutan wajib berangkat >= MIN_TRANSIT menit setelah mendarat.
    Simpan 2 termurah per (jalur, tanggal)."""
    hasil = []
    for gateway, ap in JALUR:
        seq = ap if arah == "pergi" else ap[::-1]; pasang = list(zip(seq, seq[1:]))
        for i in range((TGL_AKHIR - TGL_MULAI).days + 1):
            d1 = TGL_MULAI + dt.timedelta(days=i); cand = []
            o1, h1 = pasang[0]
            for c1 in kartu_untuk(o1, h1, d1):
                if len(pasang) == 1: cand.append(rakit(arah, gateway, ap, [(o1, h1, d1, c1)])); continue
                if not (c1["dur"] or c1["arr"]): continue
                tiba1, (o2, h2) = tiba_utc(o1, h1, d1, c1), pasang[1]
                for off in (0, 1):
                    d2 = d1 + dt.timedelta(days=off)
                    for c2 in kartu_untuk(o2, h2, d2):
                        if MIN_TRANSIT <= int((utc(o2, d2, c2["dep"]) - tiba1).total_seconds() // 60) <= MAX_TRANSIT:
                            cand.append(rakit(arah, gateway, ap, [(o1, h1, d1, c1), (o2, h2, d2, c2)]))
            cand.sort(key=lambda r: (r["total"], r["waktu"])); hasil += cand[:2]
    return hasil

def hitung_dan_kirim(tampil=False):
    ringkas = {}
    for arah in ("pergi", "pulang"):
        rows = semua_skenario(arah); ringkas[arah] = rows
        post(INGEST_URL.replace("a=ingest", "a=skenario") if INGEST_URL else None, dict(key=INGEST_KEY, arah=arah, rows=rows))
    if tampil:
        for arah, rows in ringkas.items():
            print(f"\n== {len(rows)} skenario {arah}; 5 termurah:", flush=True)
            for r in sorted(rows, key=lambda r: r["total"])[:5]:
                print(f"   Rp{r['total']:>10,}  {r['tgl']}  {r['jalur']:<12} {r['waktu'] // 60}j{r['waktu'] % 60:02d}m  singgah>={r['layover'] or '-'} mnt", flush=True)
    return ringkas

# ============================ ALUR PENCARIAN ============================
def status(s, info=""):
    """Detak ke dashboard: mulai / progres / selesai / gagal."""
    if INGEST_URL: post(INGEST_URL.replace("a=ingest", "a=status"), dict(key=INGEST_KEY, status=s, info=info))

def umpan(o, d, ds, c, site, url):
    post(INGEST_URL, dict(key=INGEST_KEY, rows=[dict(mode="flight", o=o, d=d, date=ds, price=c["harga"], dur=c["dur"], airline=c["maskapai"],
                                                  bukti=c["bukti"], url=url, stops=c["transit"], site=site)]))

async def proses_silang(page, t):
    """Baca satu sumber PEMBANDING untuk rute-tanggal tertentu; hasilnya hanya untuk tabel cek silang (tidak mengubah skenario)."""
    o, d, date, site = t; ds = date.isoformat()
    if time.time() < JEDA.get(site, 0) or NOROUTE.get((site, o, d), 0) >= 2: return False
    try: r = await baca(page, site, o, d, date)
    except Exception as e: print("  ERR", site, o, d, ds, str(e)[:60], flush=True); r = None
    if not r and site in LANGSUNG and time.time() >= JEDA.get(site, 0):
        NOROUTE[(site, o, d)] = NOROUTE.get((site, o, d), 0) + 1; return False
    if not r:
        GAGAL[site] = GAGAL.get(site, 0) + 1
        if GAGAL[site] >= 8: JEDA[site] = time.time() + 1800; GAGAL[site] = 0
        return False
    GAGAL[site] = 0; c = min(r["cards"], key=lambda x: x["harga"]); umpan(o, d, ds, c, site, r["url"])
    print(f"  silang {site:9} {o}->{d} {ds}  termurah Rp{c['harga']:,}", flush=True); return True

def daftar_silang():
    """Untuk K tanggal termurah tiap rute: kirim ulang baris sumber utama (agar berpasangan) + antrekan sumber lain."""
    tugas = []; per = {}
    for (o, d, ds), kartu in CARDS.items():
        if kartu: per.setdefault((o, d), []).append((min(c["harga"] for c in kartu.values()), ds))
    for (o, d), lst in per.items():
        for _, ds in sorted(lst)[:K_SILANG]:
            kartu = CARDS[(o, d, ds)]; c = min(kartu.values(), key=lambda x: x["harga"])
            umpan(o, d, ds, c, c["sumber"], c["url"])
            tugas += [(o, d, dt.date.fromisoformat(ds), site) for site in FLIGHT_SITES if site not in {x["sumber"] for x in kartu.values()}]
    return tugas

def rute_semua():
    s = set()
    for _, ap in JALUR:
        for seq in (ap, ap[::-1]): s.update(zip(seq, seq[1:]))
    return sorted(s)

def tugas_semua():
    n = (TGL_AKHIR - TGL_MULAI).days + 1                          # leg lanjutan boleh +1 hari
    return [(o, d, TGL_MULAI + dt.timedelta(days=i)) for i in range(n + 1) for o, d in rute_semua()]

STAT = dict(ok=0, kosong=0)

async def proses(page, t):
    o, d, date = t; ds = date.isoformat(); berhasil = False
    for site in (FLIGHT_SITES if OTA_SEMUA else [x for x in FLIGHT_SITES if x not in LANGSUNG]):
        if time.time() < JEDA.get(site, 0) or NOROUTE.get((site, o, d), 0) >= 2: continue
        try: r = await baca(page, site, o, d, date)
        except Exception as e: print("  ERR", site, o, d, ds, str(e)[:60], flush=True); r = None
        if r:
            GAGAL[site] = 0; simpan_kartu(o, d, ds, r); berhasil = True; STAT["ok"] += 1
            c = min(r["cards"], key=lambda x: x["harga"])
            post(INGEST_URL, dict(key=INGEST_KEY, rows=[dict(mode="flight", o=o, d=d, date=ds, price=c["harga"], dur=c["dur"], airline=c["maskapai"],
                                                          bukti=c["bukti"], url=r["url"], stops=c["transit"], site=site)]))
            print(f"  {site:9} {o}->{d} {ds}  {len(r['cards']):>2} kartu | termurah Rp{c['harga']:,} {c['dep']} " +
                  (f"{c['dur'] // 60}j{c['dur'] % 60:02d}m" if c["dur"] else "durasi ?") + f" {c['maskapai'] or ''} {c['transit'] or ''}", flush=True)
            if STAT["ok"] % 40 == 0: status("progres", f"{STAT['ok']} halaman terbaca")
            if STAT["ok"] % 80 == 0: hitung_dan_kirim()               # dashboard ikut terisi selama proses berjalan
            if not OTA_SEMUA: return True
            continue
        if site in LANGSUNG and time.time() >= JEDA.get(site, 0): NOROUTE[(site, o, d)] = NOROUTE.get((site, o, d), 0) + 1; continue      # kosong biasa (maskapai tak terbang di rute ini)
        GAGAL[site] = GAGAL.get(site, 0) + 1
        if GAGAL[site] >= 8:
            JEDA[site] = time.time() + 1800; GAGAL[site] = 0
            print(f"  !! {site} gagal 8x berturut-turut -> dijeda 30 menit", flush=True)
    if not berhasil: STAT["kosong"] += 1
    return berhasil

async def paralel(br, tugas, fn=None):
    q = asyncio.Queue()
    for t in tugas: q.put_nowait(t)
    async def kerja():
        ctx = await konteks(br); page = await ctx.new_page()
        while True:
            try: t = q.get_nowait()
            except asyncio.QueueEmpty: break
            await (fn or proses)(page, t); await asyncio.sleep(random.uniform(1.5, 4))
        await ctx.close()
    await asyncio.gather(*[kerja() for _ in range(max(1, WORKERS))])

async def run_once():
    status("mulai", f"{TGL_MULAI} s/d {TGL_AKHIR}")
    try: await _run_once()
    except BaseException as e:
        status("gagal", repr(e)[:150]); raise
    status("selesai", f"{STAT['ok']} halaman terbaca, {STAT['kosong']} kosong")

async def _run_once():
    CARDS.clear(); tugas = tugas_semua(); per_rute = {}
    for o, d, date in tugas: per_rute.setdefault((o, d), []).append(date)
    print(f"[{dt.datetime.now():%H:%M}] {TGL_MULAI} s/d {TGL_AKHIR}: {len(per_rute)} rute x {len(tugas) // len(per_rute)} tanggal = {len(tugas)} pencarian", flush=True)
    async with async_playwright() as p:
        br = await peluncur(p)
        probe = [(o, d, lst[len(lst) // 4]) for (o, d), lst in per_rute.items()] + [(o, d, lst[3 * len(lst) // 4]) for (o, d), lst in per_rute.items()]
        print(f"== PROBE: {len(per_rute)} rute (2 tanggal per rute)", flush=True)
        await paralel(br, probe)
        hidup = {(o, d) for (o, d, ds) in CARDS}
        for o, d in sorted(set(per_rute) - hidup): print(f"  !! rute {o}->{d} tidak terbaca di semua OTA -> dilewati putaran ini", flush=True)
        print("== PENCARIAN: setiap tanggal untuk tiap rute", flush=True)
        await paralel(br, [t for t in tugas if (t[0], t[1]) in hidup and (t[0], t[1], t[2].isoformat()) not in CARDS])
        hitung_dan_kirim()                                         # skenario sudah bisa dilihat sebelum cek silang selesai
        if K_SILANG:
            ts = daftar_silang(); print(f"== CEK SILANG: {len(ts)} halaman (sumber lain untuk {K_SILANG} tanggal termurah per rute)", flush=True)
            await paralel(br, ts, proses_silang)
        await br.close()
    hitung_dan_kirim(tampil=True)
    print("== SELESAI.", STAT, flush=True)

async def cek(o, d, ds):
    """Uji 1 rute di SEMUA OTA:  python tiketscout.py cek DPS CGK 2027-03-05"""
    CEK_MODE[0] = True; MAKS_TUNGGU[0] = 45; date = dt.date.fromisoformat(ds)
    async with async_playwright() as p:
        br = await peluncur(p); page = await (await konteks(br)).new_page()
        for site in FLIGHT_SITES:
            try: r = await baca(page, site, o, d, date)
            except Exception as e: r = None; print("  ERR", site, str(e)[:80], flush=True)
            print(f"CEK {o}->{d} {ds} [{site}]: " + (f"{len(r['cards'])} kartu" if r else "KOSONG"), flush=True)
            if r:
                for c in sorted(r["cards"], key=lambda x: x["harga"])[:8]:
                    print(f"      Rp{c['harga']:>10,} | {c['dep']}->{c['arr']} | " + (f"{c['dur'] // 60}j{c['dur'] % 60:02d}m" if c["dur"] else "durasi ?") +
                          f" | {c['maskapai'] or '-'} | {c['transit'] or '-'}", flush=True)
                print(f"      teks kartu termurah: {min(r['cards'], key=lambda x: x['harga'])['bukti']}", flush=True)
                simpan_kartu(o, d, ds, r)
                c = min(r["cards"], key=lambda x: x["harga"])
                post(INGEST_URL, dict(key=INGEST_KEY, rows=[dict(mode="flight", o=o, d=d, date=ds, price=c["harga"], dur=c["dur"], airline=c["maskapai"],
                                                              bukti=c["bukti"], url=r["url"], stops=c["transit"], site=site)]))
            try:
                simpan_teks(f"cek_{o}_{d}_{site}", await page.inner_text("body"), page.url); await page.screenshot(path=f"cek_{o}_{d}_{site}.png")
            except Exception: pass
        await br.close()

async def cek_url(url):
    """Uji URL pencarian OTA APA SAJA (salin dari browser Anda): apakah kartunya bisa dibaca dari server?"""
    CEK_MODE[0] = True; MAKS_TUNGGU[0] = 45
    async with async_playwright() as p:
        br = await peluncur(p); page = await (await konteks(br)).new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        text, cards = await tunggu_hasil(page)
        if cards: cards = await muat_lebih(page, cards)
        if len(text) < 2500 and BOT_RE.search(text):
            print("CEK-URL: halaman VERIFIKASI ANTI-BOT (situs ini tidak bisa dibaca otomatis dari server)", flush=True)
            print("   isi halaman:", re.sub(r"\s+", " ", text[:200]), flush=True)
        print(f"CEK-URL {url[:110]}: {len(cards)} kartu", flush=True)
        for c in sorted(cards, key=lambda x: x["harga"])[:10]:
            print(f"      Rp{c['harga']:>10,} | {c['dep']}->{c['arr']} | " + (f"{c['dur'] // 60}j{c['dur'] % 60:02d}m" if c["dur"] else "durasi ?") + f" | {c['maskapai'] or '-'} | {c['transit'] or '-'}", flush=True)
        if not cards: await diagnosa(page, text, "url")
        simpan_teks("cek_url", text, page.url)
        try: await page.screenshot(path="cek_url.png")
        except Exception: pass
        await br.close()

async def login():
    """PC sendiri (bukan GitHub): buka browser berprofil tetap. Anda menyelesaikan verifikasi 'saya bukan robot' SECARA MANUAL;
    cookie tersimpan di PROFIL_DIR dan dipakai saat 'once' dijalankan dengan PROFIL_DIR yang sama."""
    global PROFIL_DIR, HEADLESS
    PROFIL_DIR = PROFIL_DIR or os.path.abspath("profil_browser"); HEADLESS = False
    async with async_playwright() as p:
        br = await peluncur(p); ctx = await konteks(br)
        for site in ("traveloka", "airasia"):
            pg = await ctx.new_page(); await pg.goto(flight_url(site, "DPS", "CGK", TGL_MULAI), wait_until="domcontentloaded")
        print(f"\nSelesaikan verifikasi manusia di tiap tab (klik 'Begin'/centang) lalu tekan Enter di sini. Profil: {PROFIL_DIR}", flush=True)
        await asyncio.get_event_loop().run_in_executor(None, input, "Tekan Enter bila selesai... ")
        await br.close()

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "once"
    if cmd == "once": asyncio.run(run_once())
    elif cmd == "cek":
        a = [x for x in sys.argv[2:] if x.lower() != "flight"]
        asyncio.run(cek_url(a[1]) if a and a[0] == "url" else cek(*a[:3]))
    elif cmd == "login": asyncio.run(login())
    else: print(__doc__)
