import os
import sys
import requests
import json
import time
import threading
import traceback
import csv
import io
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

# ── ReportLab PDF ─────────────────────────────────────────────────────────────
try:
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                     Paragraph, Spacer, HRFlowable)
    from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
    REPORTLAB_OK = True
except ImportError:
    REPORTLAB_OK = False

# --- CONFIGURATION ---
BASE_URL = "https://ncod153.n-able.com"
JWT = os.environ.get("NABLE_TOKEN")
CACHE_FILE = "customers_cache.json"
CACHE_TTL = 300
ASSET_FETCH_DELAY = 0.15

cache_lock = threading.Lock()

# ── All available columns, with display labels and grouping ──────────────────
ALL_COLUMNS = [
    ("device_name",        "Device Name",         "Identity"),
    ("device_class",       "Device Class",        "Identity"),
    ("customer",           "Customer",            "Identity"),
    ("site",               "Site",                "Identity"),
    ("device_id",          "Device ID",           "Identity"),
    ("ip_address",         "IP Address",          "Network"),
    ("mac_address",        "MAC Address",         "Network"),
    ("os",                 "OS",                  "Operating System"),
    ("os_version",         "OS Version",          "Operating System"),
    ("os_architecture",    "OS Architecture",     "Operating System"),
    ("last_boot",          "Last Boot",           "Operating System"),
    ("cpu",                "CPU",                 "Hardware"),
    ("cpu_cores",          "CPU Cores",           "Hardware"),
    ("cpu_speed",          "CPU Speed",           "Hardware"),
    ("ram_total",          "RAM Total",           "Hardware"),
    ("ram_detail",         "RAM Detail",          "Hardware"),
    ("disk_total",         "Disk Total",          "Hardware"),
    ("disk_model",         "Disk Model",          "Hardware"),
    ("system_model",       "System Model",        "Hardware"),
    ("serial_number",      "Serial Number",       "Hardware"),
    ("motherboard",        "Motherboard",         "Hardware"),
    ("bios_version",       "BIOS Version",        "Hardware"),
    ("last_seen",          "Last Seen",           "Activity"),
    ("last_user",          "Last Logged In User", "Activity"),
]

COLUMN_KEYS   = [c[0] for c in ALL_COLUMNS]
COLUMN_LABELS = {c[0]: c[1] for c in ALL_COLUMNS}

DEFAULT_COLUMNS = [
    "device_name", "device_class", "ip_address",
    "os", "os_version", "os_architecture",
    "cpu", "cpu_cores", "cpu_speed",
    "ram_total", "disk_total",
    "last_seen", "last_user",
]

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

# ── Auth ──────────────────────────────────────────────────────────────────────
def get_access_token():
    if not JWT:
        raise Exception("NABLE_TOKEN environment variable is missing!")
    headers = {"Authorization": f"Bearer {JWT}", "Accept": "application/json"}
    res = requests.post(f"{BASE_URL}/api/auth/authenticate", headers=headers, timeout=30)
    if res.status_code != 200:
        raise Exception(f"AUTH FAILED: {res.status_code} {res.text[:300]}")
    return res.json()['tokens']['access']['token']

# ── Device fetching ───────────────────────────────────────────────────────────
def fetch_all_devices(api_headers):
    devices = []
    next_uri = f"{BASE_URL}/api/devices?pageSize=1000"
    while next_uri:
        res = requests.get(next_uri, headers=api_headers, timeout=60)
        res.raise_for_status()
        data = res.json()
        devices.extend(data.get('data', []))
        next_page = data.get('_links', {}).get('nextPage')
        next_uri = f"{BASE_URL}{next_page}" if next_page else None
    return devices

def fetch_device_detail(device_id, api_headers):
    try:
        res = requests.get(f"{BASE_URL}/api/devices/{device_id}", headers=api_headers, timeout=15)
        if res.status_code == 200: return res.json().get('data', res.json())
    except Exception as e: log(f"  detail error {device_id}: {e}")
    return {}

def fetch_device_assets(device_id, api_headers):
    try:
        res = requests.get(f"{BASE_URL}/api/devices/{device_id}/assets", headers=api_headers, timeout=20)
        if res.status_code == 200: return res.json().get('data', res.json())
    except Exception as e: log(f"  assets error {device_id}: {e}")
    return {}

# ── Field extractors ──────────────────────────────────────────────────────────
def safe(val, fallback='N/A'):
    if val is None or val == '': return fallback
    s = str(val).strip()
    return s if s else fallback

def bytes_to_gb(val):
    try:
        v = float(val)
        return f"{v / (1024**3):.1f} GB" if v > 0 else 'N/A'
    except Exception: return safe(val)

def mhz_to_ghz(val):
    try:
        mhz = float(val)
        return f"{mhz / 1000:.2f} GHz" if mhz > 0 else 'N/A'
    except Exception: return safe(val)

def extract_all_fields(dev, detail, assets):
    base = {**dev, **detail}
    extra = assets.get('_extra', {})

    na_list = assets.get('networkadapter', {}).get('list', []) if isinstance(assets.get('networkadapter'), dict) else []
    ip  = safe(na_list[0].get('ipaddress')) if na_list else 'N/A'
    mac = safe(na_list[0].get('macaddress')) if na_list else 'N/A'

    os_block = assets.get('os', {})
    os_name  = safe(os_block.get('reportedos') or base.get('supportedOs'))
    os_ver   = safe(os_block.get('version'))
    os_arch  = safe(os_block.get('osarchitecture'))

    os_extra  = extra.get('os', {})
    raw_boot  = os_extra.get('lastbootuptime', '')
    last_boot = 'N/A'
    if raw_boot:
        try: last_boot = datetime.strptime(raw_boot[:19], "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M")
        except Exception: last_boot = safe(raw_boot)

    proc       = assets.get('processor', {})
    cpu_name   = safe(proc.get('name'))
    cpu_cores  = safe(proc.get('numberofcores'))
    extra_proc = extra.get('processor', {})
    cpu_speed  = mhz_to_ghz(extra_proc.get('maxclockspeed')) if extra_proc.get('maxclockspeed') else 'N/A'
    if cpu_name == 'N/A': cpu_name = safe(extra_proc.get('description') or extra_proc.get('name'))

    cs        = assets.get('computersystem', {})
    ram_total = bytes_to_gb(cs.get('totalphysicalmemory')) if cs.get('totalphysicalmemory') else 'N/A'
    if ram_total == 'N/A':
        mem_list = extra.get('memory', {}).get('list', [])
        if mem_list:
            try:
                total = sum(float(m.get('capacity', 0)) for m in mem_list if m.get('capacity'))
                ram_total = bytes_to_gb(total) if total > 0 else 'N/A'
            except Exception: pass

    mem_list   = extra.get('memory', {}).get('list', [])
    ram_detail = 'N/A'
    if mem_list:
        sticks = []
        for m in mem_list:
            cap  = bytes_to_gb(m.get('capacity', 0))
            mfr  = safe(m.get('manufacturer', ''), '')
            typ  = safe(m.get('type', ''), '')
            parts = [p for p in [mfr, cap, typ] if p]
            sticks.append(' '.join(parts))
        ram_detail = f"{len(mem_list)}x — {', '.join(sticks)}"

    logical     = extra.get('logicaldevice', {}).get('list', [])
    disk_total  = 'N/A'
    total_bytes = 0
    for vol in logical:
        try: total_bytes += float(vol.get('maxcapacity', 0))
        except Exception: pass
    if total_bytes > 0:
        disk_total = bytes_to_gb(total_bytes)
    else:
        phys = extra.get('physicaldrive', {}).get('list', [])
        phys_total = 0
        for d in phys:
            try: phys_total += float(d.get('capacity', 0))
            except Exception: pass
        if phys_total > 0: disk_total = bytes_to_gb(phys_total)

    phys_drives = extra.get('physicaldrive', {}).get('list', [])
    disk_model  = safe(phys_drives[0].get('modelnumber')) if phys_drives else 'N/A'

    mfr         = safe(cs.get('manufacturer', ''), '')
    mdl         = safe(cs.get('model', ''), '')
    sys_parts   = [p for p in [mfr, mdl] if p and p.lower() not in ('n/a', '')]
    system_model = ' '.join(sys_parts) if sys_parts else 'N/A'
    serial      = safe(cs.get('serialnumber', ''), 'N/A')
    if not serial or serial.lower() in ('n/a', 'to be filled by o.e.m.', ''): serial = 'N/A'

    mb      = extra.get('motherboard', {})
    mobo    = safe(mb.get('product'))
    bios    = f"BIOS {mb.get('biosversion')}" if mb.get('biosversion') else 'N/A'

    raw_ts    = base.get('lastApplianceCheckinTime') or base.get('lastCheckin')
    last_seen = 'N/A'
    if raw_ts:
        try: last_seen = datetime.strptime(raw_ts[:19], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y %H:%M")
        except Exception: last_seen = safe(raw_ts)

    dev_extra = extra.get('device', {})
    last_user = safe(dev_extra.get('lastloggedinuser') or base.get('lastLoggedInUser'))

    return {
        "device_name":     safe(base.get('longName') or base.get('name')),
        "device_class":    safe(base.get('deviceClass')),
        "customer":        safe(base.get('customerName')),
        "site":            safe(base.get('siteName')),
        "device_id":       safe(str(base.get('deviceId', ''))),
        "ip_address":      ip,
        "mac_address":     mac,
        "os":              os_name,
        "os_version":      os_ver,
        "os_architecture": os_arch,
        "last_boot":       last_boot,
        "cpu":             cpu_name,
        "cpu_cores":       cpu_cores,
        "cpu_speed":       cpu_speed,
        "ram_total":       ram_total,
        "ram_detail":      ram_detail,
        "disk_total":      disk_total,
        "disk_model":      disk_model,
        "system_model":    system_model,
        "serial_number":   serial,
        "motherboard":     mobo,
        "bios_version":    bios,
        "last_seen":       last_seen,
        "last_user":       last_user,
    }

# ── Customer hierarchy cache ──────────────────────────────────────────────────
def get_customers_list():
    with cache_lock:
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r') as f:
                    cached = json.load(f)
                if time.time() - cached.get('fetched_at', 0) < CACHE_TTL:
                    return cached['customers']
            except Exception: pass

    log("Fetching customer list from N-able...")
    token = get_access_token()
    h = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    devices = fetch_all_devices(h)
    
    cust_map = {}
    for d in devices:
        c = d.get('customerName')
        if not c: continue
        s = d.get('siteName')
        if c not in cust_map:
            cust_map[c] = set()
        if s:
            cust_map[c].add(s)
            
    customers = []
    for c in sorted(cust_map.keys()):
        customers.append({
            "customer": c,
            "sites": sorted(list(cust_map[c]))
        })

    with cache_lock:
        with open(CACHE_FILE, 'w') as f:
            json.dump({'fetched_at': time.time(), 'customers': customers}, f)

    log(f"Found {len(customers)} customers (with sub-sites).")
    return customers

# ── Data fetch ────────────────────────────────────────────────────────────────
def fetch_customer_rows(customer_name, site_name=None):
    """Fetch + extract all fields for a customer (and optionally filter by site)."""
    prefix = f"[{customer_name} - {site_name}]" if site_name else f"[{customer_name}]"
    log(f"{prefix} Authenticating...")
    token = get_access_token()
    h = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    log(f"{prefix} Fetching device list...")
    all_devices = fetch_all_devices(h)
    customer_devices = [d for d in all_devices if d.get('customerName') == customer_name]
    
    if site_name:
        customer_devices = [d for d in customer_devices if d.get('siteName') == site_name]

    total = len(customer_devices)
    log(f"{prefix} {total} device(s) found — starting detail fetch")

    rows = []
    skipped = 0
    t_start = time.time()

    for i, dev in enumerate(customer_devices, start=1):
        dev_id   = dev.get('deviceId')
        dev_name = dev.get('longName') or dev.get('name') or str(dev_id)
        if not dev_id:
            log(f"{prefix}   [{i}/{total}] SKIP — no deviceId ({dev_name})")
            skipped += 1
            continue

        if i > 1: time.sleep(ASSET_FETCH_DELAY)

        elapsed  = time.time() - t_start
        avg_each = elapsed / (i - 1) if i > 1 else 0
        eta_secs = int(avg_each * (total - i + 1)) if avg_each else 0
        eta_str  = f"ETA ~{eta_secs}s" if eta_secs else "ETA unknown"

        log(f"{prefix}   [{i}/{total}] {dev_name}  ({eta_str})")

        detail = fetch_device_detail(dev_id, h)
        assets = fetch_device_assets(dev_id, h)
        rows.append(extract_all_fields(dev, detail, assets))

    elapsed_total = time.time() - t_start
    log(f"{prefix} Done — {len(rows)} rows built, {skipped} skipped, {elapsed_total:.1f}s elapsed")
    return rows

def fetch_all_customer_rows():
    log("[ALL] Authenticating...")
    token = get_access_token()
    h = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    log("[ALL] Fetching full device list...")
    all_devices = fetch_all_devices(h)
    total = len(all_devices)
    log(f"[ALL] {total} device(s) across all customers — starting detail fetch")

    rows = []
    skipped = 0
    t_start = time.time()

    for i, dev in enumerate(all_devices, start=1):
        dev_id    = dev.get('deviceId')
        dev_name  = dev.get('longName') or dev.get('name') or str(dev_id)
        cust_name = dev.get('customerName', 'Unknown')
        if not dev_id:
            log(f"[ALL]   [{i}/{total}] SKIP — no deviceId ({dev_name} / {cust_name})")
            skipped += 1
            continue

        if i > 1: time.sleep(ASSET_FETCH_DELAY)

        elapsed  = time.time() - t_start
        avg_each = elapsed / (i - 1) if i > 1 else 0
        eta_secs = int(avg_each * (total - i + 1)) if avg_each else 0
        eta_str  = f"ETA ~{eta_secs}s" if eta_secs > 0 else "calculating..."

        pct = int((i / total) * 100)
        log(f"[ALL]   [{i}/{total}] {pct}%  {dev_name} ({cust_name})  {eta_str}")

        detail = fetch_device_detail(dev_id, h)
        assets = fetch_device_assets(dev_id, h)
        rows.append(extract_all_fields(dev, detail, assets))

    elapsed_total = time.time() - t_start
    log(f"[ALL] Done — {len(rows)} rows built, {skipped} skipped, {elapsed_total:.1f}s total")
    return rows

# ── CSV export ────────────────────────────────────────────────────────────────
def generate_csv(rows, selected_columns):
    headers = [COLUMN_LABELS[k] for k in selected_columns if k in COLUMN_LABELS]
    output  = io.StringIO()
    writer  = csv.writer(output)
    writer.writerow(headers)
    for row in rows:
        writer.writerow([row.get(k, 'N/A') for k in selected_columns])
    return output.getvalue().encode('utf-8-sig')

# ── PDF export ────────────────────────────────────────────────────────────────
PDF_WHITE  = colors.white
PDF_BLACK  = colors.HexColor('#111111')
PDF_ACCENT = colors.HexColor('#1a5fa8')   
PDF_HDR_BG = colors.HexColor('#1a5fa8')   
PDF_ROW_A  = colors.white                 
PDF_ROW_B  = colors.HexColor('#f4f7fb')   
PDF_BORDER = colors.HexColor('#c8d8e8')   
PDF_MUTED  = colors.HexColor('#5a7080')   
PDF_MID    = PDF_HDR_BG                   

LANDSCAPE_THRESHOLD = 7

def generate_pdf(rows, selected_columns, customer_name):
    if not REPORTLAB_OK:
        raise Exception("ReportLab not installed — PDF generation unavailable.")

    buf        = io.BytesIO()
    use_land   = len(selected_columns) > LANDSCAPE_THRESHOLD
    page_size  = landscape(A4) if use_land else A4
    W, H       = page_size

    margin = 18 * mm
    doc = SimpleDocTemplate(
        buf,
        pagesize=page_size,
        leftMargin=margin, rightMargin=margin,
        topMargin=22 * mm, bottomMargin=18 * mm,
    )

    styles = getSampleStyleSheet()

    title_style = ParagraphStyle('Title', fontName='Helvetica-Bold', fontSize=18, textColor=PDF_BLACK, spaceAfter=10)
    cell_style = ParagraphStyle('Cell', fontName='Helvetica', fontSize=7, textColor=PDF_BLACK, leading=9, wordWrap='LTR')
    cell_bold = ParagraphStyle('CellBold', fontName='Helvetica-Bold', fontSize=7, textColor=PDF_BLACK, leading=9)
    na_style = ParagraphStyle('NA', fontName='Helvetica', fontSize=7, textColor=PDF_MUTED, leading=9)

    story = []

    story.append(Paragraph("EMERALD", ParagraphStyle('Brand', fontName='Helvetica-Bold', fontSize=8, textColor=PDF_ACCENT, letterSpacing=4, spaceAfter=4)))
    story.append(Paragraph(f"Device Inventory Report", title_style))
    story.append(Paragraph(
        f"Customer/Site: <b>{customer_name}</b> &nbsp;·&nbsp; "
        f"Generated: {datetime.now().strftime('%d %B %Y at %H:%M')} &nbsp;·&nbsp; "
        f"{len(rows)} device{'s' if len(rows) != 1 else ''} &nbsp;·&nbsp; "
        f"{len(selected_columns)} column{'s' if len(selected_columns) != 1 else ''}",
        ParagraphStyle('Meta', fontName='Helvetica', fontSize=8, textColor=PDF_MUTED, spaceAfter=6)
    ))
    story.append(HRFlowable(width="100%", thickness=1, color=PDF_ACCENT, spaceAfter=10, spaceBefore=0))

    if not rows:
        story.append(Paragraph("No devices found for this target.", cell_style))
        doc.build(story, onFirstPage=_pdf_bg, onLaterPages=_pdf_bg)
        return buf.getvalue()

    usable_w = W - 2 * margin
    weight_map = {
        "device_name": 2.2, "cpu": 3.0, "ram_detail": 2.8, "os": 2.0, "system_model": 2.0, "motherboard": 2.2,
        "disk_model": 2.2, "last_user": 1.8, "device_class": 1.8, "mac_address": 1.6, "serial_number": 1.6, "bios_version": 1.4,
    }
    weights   = [weight_map.get(k, 1.2) for k in selected_columns]
    col_widths = [usable_w * (w / sum(weights)) for w in weights]

    header_row = [Paragraph(COLUMN_LABELS[k].upper(), ParagraphStyle('Hdr', fontName='Helvetica-Bold', fontSize=6.5, textColor=PDF_WHITE, leading=8, letterSpacing=0.5)) for k in selected_columns]
    table_data = [header_row]

    for row in rows:
        table_row = []
        for k in selected_columns:
            val = row.get(k, 'N/A')
            if val == 'N/A' or val == '' or val is None:
                p = Paragraph('—', na_style)
            elif k == 'device_name':
                p = Paragraph(str(val), cell_bold)
            else:
                p = Paragraph(str(val), cell_style)
            table_row.append(p)
        table_data.append(table_row)

    n_rows = len(table_data)
    style_cmds = [
        ('BACKGROUND',   (0, 0), (-1, 0),  PDF_MID),
        ('LINEBELOW',    (0, 0), (-1, 0),  1.5, PDF_ACCENT),
        *[('BACKGROUND', (0, i), (-1, i), PDF_ROW_A if i % 2 == 1 else PDF_ROW_B) for i in range(1, n_rows)],
        ('LINEBELOW',    (0, 1), (-1, -1), 0.3, PDF_BORDER),
        ('LINEBEFORE',   (0, 0), (0, -1),  0,   PDF_ACCENT),
        ('TOPPADDING',   (0, 0), (-1, 0),  5),
        ('BOTTOMPADDING',(0, 0), (-1, 0),  5),
        ('TOPPADDING',   (0, 1), (-1, -1), 4),
        ('BOTTOMPADDING',(0, 1), (-1, -1), 4),
        ('LEFTPADDING',  (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('VALIGN',       (0, 0), (-1, -1), 'TOP'),
        ('LINEAFTER',    (0, 0), (0, -1),  0.5, PDF_BORDER),
    ]

    tbl = Table(table_data, colWidths=col_widths, repeatRows=1, hAlign='LEFT', splitByRow=True)
    tbl.setStyle(TableStyle(style_cmds))
    story.append(tbl)

    story.append(Spacer(1, 6 * mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#c0cdd8'), spaceAfter=4))
    story.append(Paragraph(
        f"Emerald IT Managed Solutions · Confidential · {len(rows)} devices · Data sourced from N-able RMM",
        ParagraphStyle('Footer', fontName='Helvetica', fontSize=6.5, textColor=PDF_MUTED, alignment=TA_CENTER)
    ))

    doc.build(story, onFirstPage=_pdf_bg, onLaterPages=_pdf_bg)
    return buf.getvalue()

def _pdf_bg(canvas, doc):
    canvas.saveState()
    W, H = doc.pagesize
    canvas.setFillColor(colors.white)
    canvas.rect(0, 0, W, H, fill=1, stroke=0)
    canvas.setFillColor(PDF_ACCENT)
    canvas.rect(0, H - 3, W, 3, fill=1, stroke=0)
    canvas.setFont('Helvetica', 7)
    canvas.setFillColor(PDF_MUTED)
    canvas.drawRightString(W - 18 * mm, 10 * mm, f"Page {doc.page}")
    canvas.restoreState()

# ── HTTP server ───────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (BrokenPipeError, ConnectionResetError)): return
        super().handle_error(request, client_address)

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-store')

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path
        qs     = parse_qs(parsed.query)

        if path in ('/', '/index.html'):
            self._file('index.html', 'text/html')

        elif path == '/api/columns':
            self._json(200, {
                'columns': [{'key': k, 'label': l, 'group': g} for k, l, g in ALL_COLUMNS],
                'defaults': DEFAULT_COLUMNS,
            })

        elif path == '/api/customers':
            try:
                self._json(200, {'customers': get_customers_list()})
            except Exception as e:
                self._json(500, {'error': str(e)})

        elif path == '/api/export':
            customer = qs.get('customer', [None])[0]
            site     = qs.get('site', [None])[0]
            fmt      = qs.get('format', ['csv'])[0].lower()
            cols_raw = qs.get('columns', [','.join(DEFAULT_COLUMNS)])[0]
            selected = [c.strip() for c in cols_raw.split(',') if c.strip() in COLUMN_KEYS]
            if not selected: selected = DEFAULT_COLUMNS

            if not customer:
                self._json(400, {'error': 'Missing customer parameter'})
                return

            try:
                target_label = f"{customer} - {site}" if site else customer
                log(f"Generating {fmt.upper()} export for: {target_label} ({len(selected)} columns)")
                rows      = fetch_customer_rows(customer, site)
                safe_name = target_label.replace(' ', '_').replace('/', '-')
                ts        = datetime.now().strftime('%Y%m%d_%H%M')
                self._deliver(fmt, rows, selected, safe_name, ts, target_label)
            except Exception as e:
                log(f"Export error: {e}\n{traceback.format_exc()}")
                self._json(500, {'error': str(e)})

        elif path == '/api/export-all':
            fmt      = qs.get('format', ['csv'])[0].lower()
            cols_raw = qs.get('columns', [','.join(DEFAULT_COLUMNS)])[0]
            selected = [c.strip() for c in cols_raw.split(',') if c.strip() in COLUMN_KEYS]
            if not selected: selected = DEFAULT_COLUMNS

            try:
                log(f"Generating {fmt.upper()} export for ALL customers ({len(selected)} columns)")
                rows = fetch_all_customer_rows()
                ts   = datetime.now().strftime('%Y%m%d_%H%M')
                self._deliver(fmt, rows, selected, "all_customers", ts, "All Customers")
            except Exception as e:
                log(f"Export-all error: {e}\n{traceback.format_exc()}")
                self._json(500, {'error': str(e)})

        elif path == '/api/debug':
            device_id = qs.get('id', [None])[0]
            if not device_id:
                self._json(400, {'error': 'Missing id parameter'})
                return
            try:
                token = get_access_token()
                h = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                self._json(200, {
                    'detail': fetch_device_detail(device_id, h),
                    'assets': fetch_device_assets(device_id, h),
                })
            except Exception as e:
                self._json(500, {'error': str(e)})

        else:
            self.send_response(404)
            self.end_headers()

    def _deliver(self, fmt, rows, selected, safe_name, ts, display_name):
        if fmt == 'pdf':
            pdf_bytes = generate_pdf(rows, selected, display_name)
            filename  = f"{safe_name}_devices_{ts}.pdf"
            self.send_response(200)
            self.send_header('Content-Type', 'application/pdf')
            self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
            self._cors()
            self.end_headers()
            try: self.wfile.write(pdf_bytes)
            except (BrokenPipeError, ConnectionResetError): pass
            log(f"PDF delivered: {display_name} ({len(pdf_bytes)} bytes)")
        else:
            csv_bytes = generate_csv(rows, selected)
            filename  = f"{safe_name}_devices_{ts}.csv"
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv; charset=utf-8')
            self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
            self._cors()
            self.end_headers()
            try: self.wfile.write(csv_bytes)
            except (BrokenPipeError, ConnectionResetError): pass
            log(f"CSV delivered: {display_name} ({len(csv_bytes)} bytes)")

    def _file(self, filename, content_type):
        try:
            with open(filename, 'rb') as f: content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self._cors()
            self.end_headers()
            try: self.wfile.write(content)
            except (BrokenPipeError, ConnectionResetError): pass
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()

    def _json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self._cors()
        self.end_headers()
        try: self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError): pass

if __name__ == "__main__":
    log("N-able Device Export Tool starting on port 8080...")
    if not JWT:
        log("!! WARNING: NABLE_TOKEN not set. API calls will fail.")
    if not REPORTLAB_OK:
        log("!! WARNING: ReportLab not installed — PDF export disabled.")
    HTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
