import os
import json
import time
import threading
import traceback
import csv
import io
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import requests

# --- CONFIGURATION ---
BASE_URL = "https://ncod153.n-able.com"
JWT = os.environ.get("NABLE_TOKEN")
CACHE_FILE = "customers_cache.json"
CACHE_TTL = 300  # 5 minutes

cache_lock = threading.Lock()

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def get_access_token():
    if not JWT:
        raise Exception("NABLE_TOKEN environment variable is missing!")
    headers = {"Authorization": f"Bearer {JWT}", "Accept": "application/json"}
    res = requests.post(f"{BASE_URL}/api/auth/authenticate", headers=headers, timeout=30)
    if res.status_code != 200:
        raise Exception(f"AUTH FAILED: {res.status_code} {res.text}")
    return res.json()['tokens']['access']['token']

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

def fetch_device_details(device_id, api_headers):
    """Fetch hardware/OS details for a single device."""
    details = {}
    try:
        res = requests.get(f"{BASE_URL}/api/devices/{device_id}", headers=api_headers, timeout=15)
        if res.status_code == 200:
            details = res.json().get('data', {})
    except Exception:
        pass
    return details

def get_customers_list():
    """Return cached customer list or fetch fresh."""
    with cache_lock:
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r') as f:
                    cached = json.load(f)
                age = time.time() - cached.get('fetched_at', 0)
                if age < CACHE_TTL:
                    return cached['customers']
            except Exception:
                pass

    log("Fetching customer list from N-able...")
    token = get_access_token()
    api_headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    devices = fetch_all_devices(api_headers)

    customers = sorted(set(
        d.get('customerName', 'Unknown')
        for d in devices
        if d.get('customerName')
    ))

    with cache_lock:
        with open(CACHE_FILE, 'w') as f:
            json.dump({'fetched_at': time.time(), 'customers': customers}, f)

    log(f"Found {len(customers)} customers.")
    return customers

def safe_val(val, fallback='N/A'):
    if val is None or val == '' or val == 0:
        return fallback
    return str(val)

def bytes_to_gb(val):
    try:
        return f"{float(val) / (1024**3):.1f} GB"
    except Exception:
        return 'N/A'

def mb_to_gb(val):
    try:
        return f"{float(val) / 1024:.1f} GB"
    except Exception:
        return 'N/A'

def generate_csv_for_customer(customer_name):
    """Fetch all devices for a customer and build CSV bytes."""
    log(f"Generating export for: {customer_name}")
    token = get_access_token()
    api_headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    devices = fetch_all_devices(api_headers)

    customer_devices = [d for d in devices if d.get('customerName') == customer_name]
    log(f"Found {len(customer_devices)} devices for {customer_name}")

    rows = []
    for dev in customer_devices:
        dev_id = dev.get('deviceId')
        details = fetch_device_details(dev_id, api_headers) if dev_id else {}

        # Merge top-level device fields with detail fields
        merged = {**dev, **details}

        # --- CPU ---
        cpu_name = safe_val(merged.get('processorType') or merged.get('cpuDescription') or merged.get('processor'))
        cpu_cores = safe_val(merged.get('processorCores') or merged.get('cpuCores') or merged.get('numberOfCores'))
        cpu_speed = safe_val(merged.get('processorSpeed') or merged.get('cpuSpeed'))

        # --- RAM ---
        ram_raw = merged.get('totalMemory') or merged.get('ramTotal') or merged.get('physicalMemory')
        ram_display = 'N/A'
        if ram_raw:
            try:
                val = float(ram_raw)
                # N-able returns RAM in MB or bytes depending on endpoint
                if val > 1_000_000:
                    ram_display = bytes_to_gb(val)
                else:
                    ram_display = mb_to_gb(val)
            except Exception:
                ram_display = safe_val(ram_raw)

        # --- Disk ---
        disk_size_raw = merged.get('totalDiskSpace') or merged.get('diskTotal') or merged.get('hddTotal')
        disk_free_raw = merged.get('freeDiskSpace') or merged.get('diskFree') or merged.get('hddFree')
        disk_size = 'N/A'
        disk_free = 'N/A'
        disk_used = 'N/A'
        if disk_size_raw:
            try:
                val = float(disk_size_raw)
                disk_size = bytes_to_gb(val) if val > 1_000_000 else mb_to_gb(val)
            except Exception:
                disk_size = safe_val(disk_size_raw)
        if disk_free_raw:
            try:
                val = float(disk_free_raw)
                disk_free = bytes_to_gb(val) if val > 1_000_000 else mb_to_gb(val)
            except Exception:
                disk_free = safe_val(disk_free_raw)
        if disk_size_raw and disk_free_raw:
            try:
                used = float(disk_size_raw) - float(disk_free_raw)
                disk_used = bytes_to_gb(used) if used > 1_000_000 else mb_to_gb(used)
            except Exception:
                pass

        # --- OS ---
        os_name = safe_val(merged.get('operatingSystem') or merged.get('osName') or merged.get('os'))
        os_version = safe_val(merged.get('osVersion') or merged.get('operatingSystemVersion'))
        os_arch = safe_val(merged.get('osArchitecture') or merged.get('architecture'))

        # --- Last Seen ---
        last_seen_raw = merged.get('lastApplianceCheckinTime') or merged.get('lastCheckin')
        last_seen = 'N/A'
        if last_seen_raw:
            try:
                dt = datetime.strptime(last_seen_raw[:19], "%Y-%m-%dT%H:%M:%S")
                last_seen = dt.strftime("%d/%m/%Y %H:%M")
            except Exception:
                last_seen = safe_val(last_seen_raw)

        rows.append({
            'Device Name': safe_val(merged.get('longName') or merged.get('name')),
            'Device Class': safe_val(merged.get('deviceClass')),
            'Customer': safe_val(merged.get('customerName')),
            'Site': safe_val(merged.get('siteName')),
            'IP Address': safe_val(merged.get('ipAddress') or merged.get('lastKnownIPAddress')),
            'OS': os_name,
            'OS Version': os_version,
            'OS Architecture': os_arch,
            'CPU': cpu_name,
            'CPU Cores': cpu_cores,
            'CPU Speed': cpu_speed,
            'RAM Total': ram_display,
            'Disk Total': disk_size,
            'Disk Used': disk_used,
            'Disk Free': disk_free,
            'Last Seen': last_seen,
            'Device ID': safe_val(dev_id),
        })

    if not rows:
        rows.append({k: 'No devices found' for k in [
            'Device Name','Device Class','Customer','Site','IP Address',
            'OS','OS Version','OS Architecture','CPU','CPU Cores','CPU Speed',
            'RAM Total','Disk Total','Disk Used','Disk Free','Last Seen','Device ID'
        ]})

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode('utf-8-sig')  # BOM for Excel compatibility


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Suppress default logs

    def send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-store')

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == '/' or path == '/index.html':
            self.serve_file('index.html', 'text/html')

        elif path == '/api/customers':
            try:
                customers = get_customers_list()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({'customers': customers}).encode())
            except Exception as e:
                self.send_error_json(500, str(e))

        elif path == '/api/export':
            customer = qs.get('customer', [None])[0]
            if not customer:
                self.send_error_json(400, 'Missing customer parameter')
                return
            try:
                csv_bytes = generate_csv_for_customer(customer)
                safe_name = customer.replace(' ', '_').replace('/', '-')
                filename = f"{safe_name}_devices_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                self.send_response(200)
                self.send_header('Content-Type', 'text/csv; charset=utf-8')
                self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
                self.send_cors_headers()
                self.end_headers()
                self.wfile.write(csv_bytes)
                log(f"Export delivered for: {customer} ({len(csv_bytes)} bytes)")
            except Exception as e:
                log(f"Export error: {e}\n{traceback.format_exc()}")
                self.send_error_json(500, str(e))

        else:
            self.send_response(404)
            self.end_headers()

    def serve_file(self, filename, content_type):
        try:
            with open(filename, 'rb') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_cors_headers()
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()

    def send_error_json(self, code, message):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps({'error': message}).encode())


if __name__ == "__main__":
    log("N-able Device Export Tool starting on port 8080...")
    if not JWT:
        log("!! WARNING: NABLE_TOKEN not set. API calls will fail.")
    HTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
