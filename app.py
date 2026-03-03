import os
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

# --- CONFIGURATION ---
BASE_URL = "https://ncod153.n-able.com"
JWT = os.environ.get("NABLE_TOKEN")
CACHE_FILE = "customers_cache.json"
CACHE_TTL = 300  # 5 minutes
ASSET_FETCH_DELAY = 0.15  # seconds between per-device calls

cache_lock = threading.Lock()


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


# ── Device list ───────────────────────────────────────────────────────────────

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


# ── Per-device endpoints ──────────────────────────────────────────────────────

def fetch_device_detail(device_id, api_headers):
    """GET /api/devices/{id} — returns basic device info."""
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
    """
    GET /api/devices/{id}/assets

    Real response shape (confirmed from debug output):
    {
      "_extra": {
        "processor":    { name, numberofcores, maxclockspeed }
        "memory":       { list: [{ capacity, type, speed, manufacturer, location }] }
        "os":           { supportedos, licensetype, installdate, lastbootuptime }
        "physicaldrive":{ list: [{ capacity, modelnumber, serialnumber }] }
        "logicaldevice":{ list: [{ volumename, maxcapacity }] }
        "computersystem":{ totalphysicalmemory, model, manufacturer, serialnumber }
        "motherboard":  { product, manufacturer, biosversion }
        "networkadapter": skipped — see top-level networkadapter
        ...
      },
      "os": {
        "reportedos", "osarchitecture", "version"
      },
      "computersystem": {
        "totalphysicalmemory", "model", "manufacturer", "serialnumber", "netbiosname"
      },
      "networkadapter": {
        "list": [{ ipaddress, macaddress, description, gateway, dnsserver }]
      },
      "processor": {
        "name", "numberofcores", "numberofcpus"
      }
    }
    """
    try:
        res = requests.get(f"{BASE_URL}/api/devices/{device_id}/assets",
                           headers=api_headers, timeout=20)
        if res.status_code == 200:
            body = res.json()
            return body.get('data', body)
    except Exception as e:
        log(f"  assets error {device_id}: {e}")
    return {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe(val, fallback='N/A'):
    if val is None or val == '':
        return fallback
    s = str(val).strip()
    return s if s else fallback


def bytes_to_gb(val):
    try:
        v = float(val)
        if v <= 0:
            return 'N/A'
        return f"{v / (1024 ** 3):.1f} GB"
    except Exception:
        return safe(val)


def mhz_to_ghz(val):
    try:
        mhz = float(val)
        if mhz <= 0:
            return 'N/A'
        return f"{mhz / 1000:.2f} GHz"
    except Exception:
        return safe(val)


# ── Field extractors — mapped to the confirmed API structure ──────────────────

def get_ip(assets):
    """
    assets.networkadapter.list[0].ipaddress
    """
    na = assets.get('networkadapter', {})
    lst = na.get('list', []) if isinstance(na, dict) else []
    for adapter in lst:
        ip = adapter.get('ipaddress', '')
        if ip:
            return ip
    return 'N/A'


def get_os(assets, detail):
    """
    assets.os.reportedos / osarchitecture / version
    Fallback: detail.supportedOs
    """
    os_block = assets.get('os', {})
    name    = safe(os_block.get('reportedos') or detail.get('supportedOs'))
    version = safe(os_block.get('version'))
    arch    = safe(os_block.get('osarchitecture'))
    return name, version, arch


def get_cpu(assets):
    """
    assets.processor.name / numberofcores / numberofcpus
    Fallback: assets._extra.processor.name / maxclockspeed
    """
    proc = assets.get('processor', {})
    name   = safe(proc.get('name'))
    cores  = safe(proc.get('numberofcores'))
    # Top-level processor block doesn't have speed — check _extra
    extra_proc = assets.get('_extra', {}).get('processor', {})
    speed_raw  = extra_proc.get('maxclockspeed')
    speed      = mhz_to_ghz(speed_raw) if speed_raw else 'N/A'
    # If we got nothing from top-level, try _extra for name too
    if name == 'N/A':
        name = safe(extra_proc.get('description') or extra_proc.get('name'))
    return name, cores, speed


def get_ram(assets):
    """
    assets.computersystem.totalphysicalmemory (bytes)
    Fallback: sum assets._extra.memory.list[].capacity (bytes)
    """
    cs = assets.get('computersystem', {})
    raw = cs.get('totalphysicalmemory')
    if raw:
        return bytes_to_gb(raw)

    # _extra.memory.list fallback
    mem_list = assets.get('_extra', {}).get('memory', {}).get('list', [])
    if mem_list:
        try:
            total = sum(float(m.get('capacity', 0)) for m in mem_list if m.get('capacity'))
            if total > 0:
                return bytes_to_gb(total)
        except Exception:
            pass
    return 'N/A'


def get_ram_details(assets):
    """
    Returns a human-readable string of RAM sticks, e.g. '2x DDR3 (Kingston 4.0 GB, 859B 2.0 GB)'
    """
    mem_list = assets.get('_extra', {}).get('memory', {}).get('list', [])
    if not mem_list:
        return 'N/A'
    sticks = []
    for m in mem_list:
        cap = bytes_to_gb(m.get('capacity', 0))
        mfr = safe(m.get('manufacturer', ''), '')
        typ = safe(m.get('type', ''), '')
        parts = [p for p in [mfr, cap, typ] if p]
        sticks.append(' '.join(parts))
    count = len(mem_list)
    return f"{count}x — {', '.join(sticks)}"


def get_disk(assets):
    """
    Disk TOTAL from assets._extra.logicaldevice.list[].maxcapacity (bytes)
    Sum all non-zero volumes to get total capacity.
    Disk Free is not available from N-central assets API.
    Physical drive info from _extra.physicaldrive.list[].
    """
    # Logical volumes — gives per-volume capacity
    logical = assets.get('_extra', {}).get('logicaldevice', {}).get('list', [])
    total_bytes = 0
    for vol in logical:
        cap = vol.get('maxcapacity', 0)
        try:
            total_bytes += float(cap)
        except Exception:
            pass
    if total_bytes > 0:
        return bytes_to_gb(total_bytes), 'N/A', 'N/A'

    # Physical drives fallback
    phys = assets.get('_extra', {}).get('physicaldrive', {}).get('list', [])
    phys_total = 0
    for d in phys:
        cap = d.get('capacity', 0)
        try:
            phys_total += float(cap)
        except Exception:
            pass
    if phys_total > 0:
        return bytes_to_gb(phys_total), 'N/A', 'N/A'

    return 'N/A', 'N/A', 'N/A'


def get_disk_model(assets):
    """Returns physical drive model, e.g. 'CT240BX500SSD1 ATA Device'"""
    drives = assets.get('_extra', {}).get('physicaldrive', {}).get('list', [])
    if drives:
        return safe(drives[0].get('modelnumber'))
    return 'N/A'


def get_motherboard(assets):
    """assets._extra.motherboard.product"""
    mb = assets.get('_extra', {}).get('motherboard', {})
    product = safe(mb.get('product'))
    mfr     = safe(mb.get('manufacturer', ''), '')
    bios    = safe(mb.get('biosversion', ''), '')
    if product == 'N/A':
        return 'N/A', 'N/A'
    desc = product
    return desc, f"BIOS {bios}" if bios else 'N/A'


def get_system_model(assets):
    """assets.computersystem.manufacturer + model"""
    cs  = assets.get('computersystem', {})
    mfr = safe(cs.get('manufacturer', ''), '')
    mdl = safe(cs.get('model', ''), '')
    serial = safe(cs.get('serialnumber', ''), '')
    parts = [p for p in [mfr, mdl] if p and p.lower() not in ('n/a', '')]
    return ' '.join(parts) if parts else 'N/A', serial


def get_last_boot(assets):
    """assets._extra.os.lastbootuptime"""
    os_extra = assets.get('_extra', {}).get('os', {})
    raw = os_extra.get('lastbootuptime', '')
    if raw:
        try:
            dt = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%d/%m/%Y %H:%M")
        except Exception:
            return safe(raw)
    return 'N/A'


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


# ── CSV ───────────────────────────────────────────────────────────────────────

COLUMNS = [
    'Device Name',
    'Device Class',
    'Customer',
    'Site',
    'IP Address',
    'MAC Address',
    'OS',
    'OS Version',
    'OS Architecture',
    'Last Boot',
    'CPU',
    'CPU Cores',
    'CPU Speed',
    'RAM Total',
    'RAM Detail',
    'Disk Total',
    'Disk Model',
    'System Model',
    'Serial Number',
    'Motherboard',
    'BIOS Version',
    'Last Seen',
    'Last Logged In User',
    'Device ID',
]


def generate_csv_for_customer(customer_name):
    log(f"Generating export for: {customer_name}")
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

        # Merge list item with detail (detail wins)
        base = {**dev, **detail}

        # ── Extract all fields ──
        ip                           = get_ip(assets)
        os_name, os_ver, os_arch     = get_os(assets, base)
        cpu_name, cpu_cores, cpu_spd = get_cpu(assets)
        ram_total                    = get_ram(assets)
        ram_detail                   = get_ram_details(assets)
        disk_total, _, _             = get_disk(assets)
        disk_model                   = get_disk_model(assets)
        system_model, serial         = get_system_model(assets)
        motherboard, bios            = get_motherboard(assets)
        last_boot                    = get_last_boot(assets)
        last_seen                    = 'N/A'
        last_user                    = 'N/A'

        # MAC address
        na_list = assets.get('networkadapter', {}).get('list', [])
        mac = safe(na_list[0].get('macaddress')) if na_list else 'N/A'

        # Last seen timestamp
        raw_ts = base.get('lastApplianceCheckinTime') or base.get('lastCheckin')
        if raw_ts:
            try:
                last_seen = datetime.strptime(raw_ts[:19], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y %H:%M")
            except Exception:
                last_seen = safe(raw_ts)

        # Last logged-in user
        device_extra = assets.get('_extra', {}).get('device', {})
        last_user = safe(device_extra.get('lastloggedinuser')
                         or base.get('lastLoggedInUser'))

        rows.append({
            'Device Name':         safe(base.get('longName') or base.get('name')),
            'Device Class':        safe(base.get('deviceClass')),
            'Customer':            safe(base.get('customerName')),
            'Site':                safe(base.get('siteName')),
            'IP Address':          ip,
            'MAC Address':         mac,
            'OS':                  os_name,
            'OS Version':          os_ver,
            'OS Architecture':     os_arch,
            'Last Boot':           last_boot,
            'CPU':                 cpu_name,
            'CPU Cores':           cpu_cores,
            'CPU Speed':           cpu_spd,
            'RAM Total':           ram_total,
            'RAM Detail':          ram_detail,
            'Disk Total':          disk_total,
            'Disk Model':          disk_model,
            'System Model':        system_model,
            'Serial Number':       serial,
            'Motherboard':         motherboard,
            'BIOS Version':        bios,
            'Last Seen':           last_seen,
            'Last Logged In User': last_user,
            'Device ID':           safe(str(dev_id)),
        })

    if not rows:
        rows.append({k: 'No devices found' for k in COLUMNS})

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode('utf-8-sig')  # BOM → opens cleanly in Excel


# ── HTTP server ───────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-store')

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path
        qs     = parse_qs(parsed.query)

        if path in ('/', '/index.html'):
            self._file('index.html', 'text/html')

        elif path == '/api/customers':
            try:
                self._json(200, {'customers': get_customers_list()})
            except Exception as e:
                self._json(500, {'error': str(e)})

        elif path == '/api/export':
            customer = qs.get('customer', [None])[0]
            if not customer:
                self._json(400, {'error': 'Missing customer parameter'})
                return
            try:
                csv_bytes = generate_csv_for_customer(customer)
                safe_name = customer.replace(' ', '_').replace('/', '-')
                filename  = f"{safe_name}_devices_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                self.send_response(200)
                self.send_header('Content-Type', 'text/csv; charset=utf-8')
                self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
                self._cors()
                self.end_headers()
                self.wfile.write(csv_bytes)
                log(f"Delivered: {customer} ({len(csv_bytes)} bytes)")
            except Exception as e:
                log(f"Export error: {e}\n{traceback.format_exc()}")
                self._json(500, {'error': str(e)})

        # Debug: GET /api/debug?id=<deviceId>
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
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()

    def _json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self._cors()
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    log("N-able Device Export Tool starting on port 8080...")
    if not JWT:
        log("!! WARNING: NABLE_TOKEN not set. API calls will fail.")
    HTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
