import os
import sys
import requests
import json
import time
import threading
import traceback
import csv
import io
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
    # (key, label, group)
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

# Default column set
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
        res = requests.get(f"{BASE_URL}/api/devices/{device_id}",
                           headers=api_headers, timeout=15)
        if res.status_code == 200:
            body = res.json()
            return body.get('data', body)
    except Exception as e:
        log(f"  detail error {device_id}: {e}")
    return {}


def fetch_device_assets(device_id, api_headers):
    try:
        res = requests.get(f"{BASE_URL}/api/devices/{device_id}/assets",
                           headers=api_headers, timeout=20)
        if res.status_code == 200:
            body = res.json()
            return body.get('data', body)
    except Exception as e:
        log(f"  assets error {device_id}: {e}")
    return {}


# ── Field extractors ──────────────────────────────────────────────────────────

def safe(val, fallback='N/A'):
    if val is None or val == '':
        return fallback
    s = str(val).strip()
    return s if s else fallback


def bytes_to_gb(val):
    try:
        v = float(val)
        return f"{v / (1024**3):.1f} GB" if v > 0 else 'N/A'
    except Exception:
        return safe(val)


def mhz_to_ghz(val):
    try:
        mhz = float(val)
        return f"{mhz / 1000:.2f} GHz" if mhz > 0 else 'N/A'
    except Exception:
        return safe(val)


def extract_all_fields(dev, detail, assets):
    """Extract every possible field and return as a dict keyed by column key."""
    base = {**dev, **detail}
    extra = assets.get('_extra', {})

    # ── Network
    na_list = assets.get('networkadapter', {}).get('list', []) if isinstance(assets.get('networkadapter'), dict) else []
    ip  = safe(na_list[0].get('ipaddress')) if na_list else 'N/A'
    mac = safe(na_list[0].get('macaddress')) if na_list else 'N/A'

    # ── OS
    os_block = assets.get('os', {})
    os_name  = safe(os_block.get('reportedos') or base.get('supportedOs'))
    os_ver   = safe(os_block.get('version'))
    os_arch  = safe(os_block.get('osarchitecture'))

    # ── Last boot
    os_extra  = extra.get('os', {})
    raw_boot  = os_extra.get('lastbootuptime', '')
    last_boot = 'N/A'
    if raw_boot:
        try:
            last_boot = datetime.strptime(raw_boot[:19], "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M")
        except Exception:
            last_boot = safe(raw_boot)

    # ── CPU
    proc       = assets.get('processor', {})
    cpu_name   = safe(proc.get('name'))
    cpu_cores  = safe(proc.get('numberofcores'))
    extra_proc = extra.get('processor', {})
    cpu_speed  = mhz_to_ghz(extra_proc.get('maxclockspeed')) if extra_proc.get('maxclockspeed') else 'N/A'
    if cpu_name == 'N/A':
        cpu_name = safe(extra_proc.get('description') or extra_proc.get('name'))

    # ── RAM
    cs        = assets.get('computersystem', {})
    ram_total = bytes_to_gb(cs.get('totalphysicalmemory')) if cs.get('totalphysicalmemory') else 'N/A'
    if ram_total == 'N/A':
        mem_list = extra.get('memory', {}).get('list', [])
        if mem_list:
            try:
                total = sum(float(m.get('capacity', 0)) for m in mem_list if m.get('capacity'))
                ram_total = bytes_to_gb(total) if total > 0 else 'N/A'
            except Exception:
                pass

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

    # ── Disk
    logical     = extra.get('logicaldevice', {}).get('list', [])
    disk_total  = 'N/A'
    total_bytes = 0
    for vol in logical:
        try:
            total_bytes += float(vol.get('maxcapacity', 0))
        except Exception:
            pass
    if total_bytes > 0:
        disk_total = bytes_to_gb(total_bytes)
    else:
        phys = extra.get('physicaldrive', {}).get('list', [])
        phys_total = 0
        for d in phys:
            try:
                phys_total += float(d.get('capacity', 0))
            except Exception:
                pass
        if phys_total > 0:
            disk_total = bytes_to_gb(phys_total)

    phys_drives = extra.get('physicaldrive', {}).get('list', [])
    disk_model  = safe(phys_drives[0].get('modelnumber')) if phys_drives else 'N/A'

    # ── System
    mfr         = safe(cs.get('manufacturer', ''), '')
    mdl         = safe(cs.get('model', ''), '')
    sys_parts   = [p for p in [mfr, mdl] if p and p.lower() not in ('n/a', '')]
    system_model = ' '.join(sys_parts) if sys_parts else 'N/A'
    serial      = safe(cs.get('serialnumber', ''), 'N/A')
    if not serial or serial.lower() in ('n/a', 'to be filled by o.e.m.', ''):
        serial = 'N/A'

    mb      = extra.get('motherboard', {})
    mobo    = safe(mb.get('product'))
    bios    = f"BIOS {mb.get('biosversion')}" if mb.get('biosversion') else 'N/A'

    # ── Activity
    raw_ts    = base.get('lastApplianceCheckinTime') or base.get('lastCheckin')
    last_seen = 'N/A'
    if raw_ts:
        try:
            last_seen = datetime.strptime(raw_ts[:19], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y %H:%M")
        except Exception:
            last_seen = safe(raw_ts)

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


# ── Customer cache ────────────────────────────────────────────────────────────

def get_customers_list():
    with cache_lock:
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r') as f:
                    cached = json.load(f)
                if time.time() - cached.get('fetched_at', 0) < CACHE_TTL:
                    return cached['customers']
            except Exception:
                pass

    log("Fetching customer list from N-able...")
    token = get_access_token()
    h = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    devices = fetch_all_devices(h)
    customers = sorted(set(d.get('customerName', 'Unknown')
                           for d in devices if d.get('customerName')))

    with cache_lock:
        with open(CACHE_FILE, 'w') as f:
            json.dump({'fetched_at': time.time(), 'customers': customers}, f)

    log(f"Found {len(customers)} customers.")
    return customers


# ── Data fetch ────────────────────────────────────────────────────────────────

def fetch_customer_rows(customer_name):
    """Fetch + extract all fields for every device of a customer."""
    token = get_access_token()
    h = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    all_devices = fetch_all_devices(h)
    customer_devices = [d for d in all_devices if d.get('customerName') == customer_name]
    log(f"  {len(customer_devices)} devices found for {customer_name}")

    rows = []
    for i, dev in enumerate(customer_devices):
        dev_id = dev.get('deviceId')
        if not dev_id:
            continue
        if i > 0:
            time.sleep(ASSET_FETCH_DELAY)
        detail = fetch_device_detail(dev_id, h)
        assets = fetch_device_assets(dev_id, h)
        rows.append(extract_all_fields(dev, detail, assets))

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

# Brand colours
PDF_DARK   = colors.HexColor('#0a0e14')
PDF_ACCENT = colors.HexColor('#00c8f0')
PDF_MID    = colors.HexColor('#1a2535')
PDF_LIGHT  = colors.HexColor('#e8f4f8')
PDF_MUTED  = colors.HexColor('#7a9ab8')
PDF_WHITE  = colors.white
PDF_ROW_A  = colors.HexColor('#0f1822')
PDF_ROW_B  = colors.HexColor('#141f2e')
PDF_BORDER = colors.HexColor('#1e3040')

# Columns that are short enough to fit nicely in a table
SHORT_COLS = {
    "device_class", "ip_address", "mac_address",
    "os_version", "os_architecture", "cpu_cores", "cpu_speed",
    "ram_total", "disk_total", "last_seen", "last_boot",
    "last_user", "site", "device_id", "serial_number",
    "bios_version", "system_model",
}

# For very wide tables, rotate to landscape
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

    # Custom paragraph styles
    title_style = ParagraphStyle(
        'Title', fontName='Helvetica-Bold', fontSize=18,
        textColor=PDF_WHITE, spaceAfter=2,
    )
    sub_style = ParagraphStyle(
        'Sub', fontName='Helvetica', fontSize=9,
        textColor=PDF_MUTED, spaceAfter=0,
    )
    cell_style = ParagraphStyle(
        'Cell', fontName='Helvetica', fontSize=7,
        textColor=PDF_LIGHT, leading=9, wordWrap='LTR',
    )
    cell_bold = ParagraphStyle(
        'CellBold', fontName='Helvetica-Bold', fontSize=7,
        textColor=PDF_WHITE, leading=9,
    )
    na_style = ParagraphStyle(
        'NA', fontName='Helvetica', fontSize=7,
        textColor=PDF_MUTED, leading=9,
    )

    story = []

    # ── Header block ──────────────────────────────────────────────────────────
    story.append(Paragraph("EMERALD", ParagraphStyle(
        'Brand', fontName='Helvetica-Bold', fontSize=8,
        textColor=PDF_ACCENT, letterSpacing=4, spaceAfter=4,
    )))
    story.append(Paragraph(f"Device Inventory Report", title_style))
    story.append(Paragraph(
        f"Customer: <b>{customer_name}</b> &nbsp;·&nbsp; "
        f"Generated: {datetime.now().strftime('%d %B %Y at %H:%M')} &nbsp;·&nbsp; "
        f"{len(rows)} device{'s' if len(rows) != 1 else ''} &nbsp;·&nbsp; "
        f"{len(selected_columns)} column{'s' if len(selected_columns) != 1 else ''}",
        ParagraphStyle('Meta', fontName='Helvetica', fontSize=8,
                       textColor=PDF_MUTED, spaceAfter=6)
    ))
    story.append(HRFlowable(width="100%", thickness=1, color=PDF_ACCENT,
                             spaceAfter=10, spaceBefore=0))

    if not rows:
        story.append(Paragraph("No devices found for this customer.", cell_style))
        doc.build(story, onFirstPage=_pdf_bg, onLaterPages=_pdf_bg)
        return buf.getvalue()

    # ── Calculate column widths ───────────────────────────────────────────────
    usable_w = W - 2 * margin

    # Base weights per column (wider for text-heavy columns)
    weight_map = {
        "device_name":     2.2, "cpu":          3.0, "ram_detail":    2.8,
        "os":              2.0, "system_model": 2.0, "motherboard":   2.2,
        "disk_model":      2.2, "last_user":    1.8, "device_class":  1.8,
        "mac_address":     1.6, "serial_number":1.6, "bios_version":  1.4,
    }
    weights   = [weight_map.get(k, 1.2) for k in selected_columns]
    total_w   = sum(weights)
    col_widths = [usable_w * (w / total_w) for w in weights]

    # ── Build table data ──────────────────────────────────────────────────────
    header_row = [
        Paragraph(COLUMN_LABELS[k].upper(), ParagraphStyle(
            'Hdr', fontName='Helvetica-Bold', fontSize=6.5,
            textColor=PDF_ACCENT, leading=8, letterSpacing=0.5,
        ))
        for k in selected_columns
    ]
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

    # ── Table styling ─────────────────────────────────────────────────────────
    n_rows = len(table_data)
    style_cmds = [
        # Header
        ('BACKGROUND',   (0, 0), (-1, 0),  PDF_MID),
        ('LINEBELOW',    (0, 0), (-1, 0),  1.5, PDF_ACCENT),
        # Alternating rows
        *[('BACKGROUND', (0, i), (-1, i), PDF_ROW_A if i % 2 == 1 else PDF_ROW_B)
          for i in range(1, n_rows)],
        # Grid
        ('LINEBELOW',    (0, 1), (-1, -1), 0.3, PDF_BORDER),
        ('LINEBEFORE',   (0, 0), (0, -1),  0,   PDF_ACCENT),
        # Padding
        ('TOPPADDING',   (0, 0), (-1, 0),  5),
        ('BOTTOMPADDING',(0, 0), (-1, 0),  5),
        ('TOPPADDING',   (0, 1), (-1, -1), 4),
        ('BOTTOMPADDING',(0, 1), (-1, -1), 4),
        ('LEFTPADDING',  (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('VALIGN',       (0, 0), (-1, -1), 'TOP'),
        # Left accent bar on device name column
        ('LINEAFTER',    (0, 0), (0, -1),  0.5, PDF_BORDER),
    ]

    tbl = Table(table_data, colWidths=col_widths, repeatRows=1,
                hAlign='LEFT', splitByRow=True)
    tbl.setStyle(TableStyle(style_cmds))
    story.append(tbl)

    # ── Footer note ───────────────────────────────────────────────────────────
    story.append(Spacer(1, 6 * mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=PDF_BORDER,
                             spaceAfter=4))
    story.append(Paragraph(
        f"Emerald IT Managed Solutions · Confidential · {len(rows)} devices · "
        f"Data sourced from N-able RMM",
        ParagraphStyle('Footer', fontName='Helvetica', fontSize=6.5,
                       textColor=PDF_MUTED, alignment=TA_CENTER)
    ))

    doc.build(story, onFirstPage=_pdf_bg, onLaterPages=_pdf_bg)
    return buf.getvalue()


def _pdf_bg(canvas, doc):
    """Draw dark background on every page."""
    canvas.saveState()
    W, H = doc.pagesize
    canvas.setFillColor(PDF_DARK)
    canvas.rect(0, 0, W, H, fill=1, stroke=0)
    # Subtle accent line at very top
    canvas.setFillColor(PDF_ACCENT)
    canvas.rect(0, H - 2, W, 2, fill=1, stroke=0)
    # Page number
    canvas.setFont('Helvetica', 7)
    canvas.setFillColor(PDF_MUTED)
    canvas.drawRightString(W - 18 * mm, 12 * mm,
                           f"Page {doc.page}")
    canvas.restoreState()


# ── HTTP server ───────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (BrokenPipeError, ConnectionResetError)):
            return
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
            # Return all available columns with labels and groups
            self._json(200, {
                'columns': [
                    {'key': k, 'label': l, 'group': g}
                    for k, l, g in ALL_COLUMNS
                ],
                'defaults': DEFAULT_COLUMNS,
            })

        elif path == '/api/customers':
            try:
                self._json(200, {'customers': get_customers_list()})
            except Exception as e:
                self._json(500, {'error': str(e)})

        elif path == '/api/export':
            customer = qs.get('customer', [None])[0]
            fmt      = qs.get('format', ['csv'])[0].lower()
            cols_raw = qs.get('columns', [','.join(DEFAULT_COLUMNS)])[0]
            selected = [c.strip() for c in cols_raw.split(',') if c.strip() in COLUMN_KEYS]
            if not selected:
                selected = DEFAULT_COLUMNS

            if not customer:
                self._json(400, {'error': 'Missing customer parameter'})
                return

            try:
                log(f"Generating {fmt.upper()} export for: {customer} ({len(selected)} columns)")
                rows     = fetch_customer_rows(customer)
                safe_name = customer.replace(' ', '_').replace('/', '-')
                ts        = datetime.now().strftime('%Y%m%d_%H%M')

                if fmt == 'pdf':
                    pdf_bytes = generate_pdf(rows, selected, customer)
                    filename  = f"{safe_name}_devices_{ts}.pdf"
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/pdf')
                    self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
                    self._cors()
                    self.end_headers()
                    try:
                        self.wfile.write(pdf_bytes)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    log(f"PDF delivered: {customer} ({len(pdf_bytes)} bytes)")
                else:
                    csv_bytes = generate_csv(rows, selected)
                    filename  = f"{safe_name}_devices_{ts}.csv"
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/csv; charset=utf-8')
                    self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
                    self._cors()
                    self.end_headers()
                    try:
                        self.wfile.write(csv_bytes)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    log(f"CSV delivered: {customer} ({len(csv_bytes)} bytes)")

            except Exception as e:
                log(f"Export error: {e}\n{traceback.format_exc()}")
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

    def _file(self, filename, content_type):
        try:
            with open(filename, 'rb') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self._cors()
            self.end_headers()
            try:
                self.wfile.write(content)
            except (BrokenPipeError, ConnectionResetError):
                pass
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()

    def _json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self._cors()
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass


if __name__ == "__main__":
    log("N-able Device Export Tool starting on port 8080...")
    if not JWT:
        log("!! WARNING: NABLE_TOKEN not set. API calls will fail.")
    if not REPORTLAB_OK:
        log("!! WARNING: ReportLab not installed — PDF export disabled.")
    HTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
