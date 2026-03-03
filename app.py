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

# N-central rate limit: max ~10 concurrent calls per endpoint.
# We serialise asset fetches with a small delay to stay well clear.
ASSET_FETCH_DELAY = 0.15  # seconds between per-device asset calls

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
    """Page through /api/devices and return every device object."""
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
    """
    GET /api/devices/{id}
    Returns richer fields: uri (IP), siteName, supportedOs, isProbe, etc.
    """
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
    Returns hardware inventory: processors, memory, logicalDisks, OS block, etc.
    Only available for devices with Asset Tracking enabled in N-central.
    Returns an empty dict with a 404 for probes or devices without asset tracking.
    """
    try:
        res = requests.get(f"{BASE_URL}/api/devices/{device_id}/assets",
                           headers=api_headers, timeout=20)
        if res.status_code == 200:
            body = res.json()
            # Some N-central versions wrap in 'data', some return the object directly
            return body.get('data', body)
        # 404 = no asset data for this device — normal for probes
    except Exception as e:
        log(f"  assets error {device_id}: {e}")
    return {}


# ── Field extraction helpers ──────────────────────────────────────────────────

def safe(val, fallback='N/A'):
    if val is None or val == '':
        return fallback
    s = str(val).strip()
    return s if s else fallback


def smart_gb(val):
    """Convert a raw number (bytes, MB, or GB) to a '12.3 GB' string."""
    try:
        v = float(val)
        if v <= 0:
            return 'N/A'
        if v > 10_000_000:          # Looks like bytes (>10 MB expressed as bytes)
            return f"{v / (1024**3):.1f} GB"
        elif v > 500:               # Looks like MB
            return f"{v / 1024:.1f} GB"
        else:                       # Already in GB or very small device
            return f"{v:.1f} GB"
    except Exception:
        return safe(val)


def _format_mhz(speed):
    if speed is None:
        return 'N/A'
    try:
        mhz = float(speed)
        if mhz > 100:
            return f"{mhz/1000:.2f} GHz"
        return f"{mhz:.2f} GHz"
    except Exception:
        return safe(speed)


def extract_cpu(assets):
    """
    Try the known shapes of the /assets response for processor data.
    Returns (name, cores, speed_str).
    """
    # Shape A: assets.processors = [{ name, numberOfCores, maxClockSpeed }]
    procs = (assets.get('processors') or assets.get('processor') or [])
    if isinstance(procs, list) and procs:
        p = procs[0]
        name  = safe(p.get('name') or p.get('description') or p.get('caption'))
        cores = safe(p.get('numberOfCores') or p.get('coreCount'))
        speed = p.get('maxClockSpeed') or p.get('currentClockSpeed') or p.get('speed')
        return name, cores, _format_mhz(speed)

    # Shape B: flat keys inside assets dict
    name  = safe(assets.get('processorType') or assets.get('cpuName'))
    cores = safe(assets.get('numberOfCores') or assets.get('processorCores'))
    speed = assets.get('processorSpeed') or assets.get('cpuSpeed') or assets.get('maxClockSpeed')
    return name, cores, _format_mhz(speed)


def extract_ram(assets):
    """Return total RAM as a human-readable GB string."""
    # Shape A: assets.memory = { totalPhysicalMemory: <bytes> }
    mem = assets.get('memory') or {}
    if isinstance(mem, dict):
        raw = (mem.get('totalPhysicalMemory') or mem.get('totalMemory')
               or mem.get('capacity') or mem.get('totalVisibleMemorySize'))
        if raw:
            return smart_gb(raw)

    # Shape B: assets.memoryModules = [{ capacity: <bytes> }] — sum them
    modules = assets.get('memoryModules') or assets.get('memoryModule') or []
    if isinstance(modules, list) and modules:
        try:
            total = sum(float(m.get('capacity', 0)) for m in modules if m.get('capacity'))
            if total > 0:
                return smart_gb(total)
        except Exception:
            pass

    # Shape C: flat top-level keys
    raw = (assets.get('totalPhysicalMemory') or assets.get('totalMemory')
           or assets.get('physicalMemory') or assets.get('ramTotal')
           or assets.get('totalVisibleMemorySize'))
    return smart_gb(raw) if raw else 'N/A'


def extract_disks(assets):
    """Return (total, used, free) as GB strings, summed across all volumes."""
    # Shape A: assets.logicalDisks = [{ size, freeSpace }]
    logical = assets.get('logicalDisks') or assets.get('logicalDisk') or []
    if isinstance(logical, list) and logical:
        try:
            total = sum(float(d.get('size', 0)) for d in logical if d.get('size'))
            free  = sum(float(d.get('freeSpace', 0) or d.get('freeSize', 0)) for d in logical)
            used  = total - free
            if total > 0:
                return smart_gb(total), smart_gb(used), smart_gb(free)
        except Exception:
            pass

    # Shape B: assets.diskDrives = [{ size }] (physical drives — no free space info)
    drives = assets.get('diskDrives') or assets.get('diskDrive') or []
    if isinstance(drives, list) and drives:
        try:
            total = sum(float(d.get('size', 0)) for d in drives if d.get('size'))
            if total > 0:
                return smart_gb(total), 'N/A', 'N/A'
        except Exception:
            pass

    # Shape C: flat fields
    total_raw = assets.get('totalDiskSpace') or assets.get('diskTotal') or assets.get('hddTotal')
    free_raw  = assets.get('freeDiskSpace')  or assets.get('diskFree')  or assets.get('hddFree')
    if total_raw:
        total_s = smart_gb(total_raw)
        free_s  = smart_gb(free_raw) if free_raw else 'N/A'
        used_s  = 'N/A'
        if free_raw:
            try:
                used_s = smart_gb(float(total_raw) - float(free_raw))
            except Exception:
                pass
        return total_s, used_s, free_s

    return 'N/A', 'N/A', 'N/A'


def extract_os(detail, assets):
    """Pull OS name, version, architecture — prefer assets over detail."""
    os_block = assets.get('operatingSystem') or assets.get('os') or {}
    if isinstance(os_block, dict) and os_block:
        os_name    = safe(os_block.get('name') or os_block.get('caption'))
        os_version = safe(os_block.get('version') or os_block.get('buildNumber')
                          or os_block.get('osVersion'))
        os_arch    = safe(os_block.get('osArchitecture') or os_block.get('architecture'))
    else:
        os_name    = safe(detail.get('supportedOs') or detail.get('operatingSystem')
                          or assets.get('osName'))
        os_version = safe(detail.get('osVersion') or assets.get('osVersion'))
        os_arch    = safe(detail.get('osArchitecture') or assets.get('osArchitecture'))

    return os_name, os_version, os_arch


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


# ── CSV generation ────────────────────────────────────────────────────────────

COLUMNS = [
    'Device Name', 'Device Class', 'Customer', 'Site', 'IP Address',
    'OS', 'OS Version', 'OS Architecture',
    'CPU', 'CPU Cores', 'CPU Speed',
    'RAM Total',
    'Disk Total', 'Disk Used', 'Disk Free',
    'Last Seen', 'Device ID',
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

        # Rate-limit courtesy pause
        if i > 0:
            time.sleep(ASSET_FETCH_DELAY)

        detail = fetch_device_detail(dev_id, h)
        assets = fetch_device_assets(dev_id, h)

        # Merge: list item → detail (detail wins on key conflicts)
        base = {**dev, **detail}

        ip                             = safe(base.get('uri') or base.get('ipAddress'))
        os_name, os_ver, os_arch       = extract_os(base, assets)
        cpu_name, cpu_cores, cpu_speed = extract_cpu(assets)
        if cpu_name == 'N/A':          # fallback if assets returned nothing
            cpu_name = safe(base.get('processorType') or base.get('cpuName'))
        ram                            = extract_ram(assets)
        disk_total, disk_used, disk_free = extract_disks(assets)

        raw_ts = base.get('lastApplianceCheckinTime') or base.get('lastCheckin')
        last_seen = 'N/A'
        if raw_ts:
            try:
                last_seen = datetime.strptime(raw_ts[:19], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y %H:%M")
            except Exception:
                last_seen = safe(raw_ts)

        rows.append({
            'Device Name':     safe(base.get('longName') or base.get('name')),
            'Device Class':    safe(base.get('deviceClass')),
            'Customer':        safe(base.get('customerName')),
            'Site':            safe(base.get('siteName')),
            'IP Address':      ip,
            'OS':              os_name,
            'OS Version':      os_ver,
            'OS Architecture': os_arch,
            'CPU':             cpu_name,
            'CPU Cores':       cpu_cores,
            'CPU Speed':       cpu_speed,
            'RAM Total':       ram,
            'Disk Total':      disk_total,
            'Disk Used':       disk_used,
            'Disk Free':       disk_free,
            'Last Seen':       last_seen,
            'Device ID':       safe(str(dev_id)),
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
        pass  # Suppress default request logs

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

        # Debug endpoint: GET /api/debug?id=<deviceId>
        # Returns the raw detail + assets JSON so you can see exactly what N-able sends.
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
