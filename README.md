# 📋 N-able Device Export Tool

A lightweight, self-hosted web tool that connects to your **N-able RMM** instance, lets you select any customer, and downloads a **CSV spreadsheet** of all their managed devices — complete with hardware specs, OS details, disk usage, RAM, and more.

---

## What's Exported

Each row in the CSV represents one device:

| Column | Description |
|---|---|
| Device Name | Full device hostname |
| Device Class | Server / Workstation / etc. |
| Customer | Customer name |
| Site | Site name within the customer |
| IP Address | Last known IP address |
| OS | Operating system name |
| OS Version | Full OS version string |
| OS Architecture | x64 / x86 / ARM |
| CPU | Processor model name |
| CPU Cores | Number of cores |
| CPU Speed | Clock speed |
| RAM Total | Total physical memory (GB) |
| Disk Total | Total disk capacity (GB) |
| Disk Used | Used disk space (GB) |
| Disk Free | Free disk space (GB) |
| Last Seen | Last N-able check-in timestamp |
| Device ID | Internal N-able device ID |

> The CSV is UTF-8 with BOM so it opens cleanly in Microsoft Excel on Windows.

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- An N-able RMM account with API access
- Your N-able **JWT API token**

---

## Quick Start

### 1. Download docker-compose.yml

Save `docker-compose.yml` to a folder. No other files are needed.

```bash
docker pull samuelstreets/nable-device-export:latest
```

### 2. Set your API token

Open `docker-compose.yml` and replace the placeholder:

```yaml
environment:
  - NABLE_TOKEN=your_jwt_token_here
```

> ⚠️ Never commit your JWT token to source control.

### 3. Run

```bash
docker compose up -d
```

### 4. Open the tool

Navigate to [http://localhost:8081](http://localhost:8081)

- The customer list loads automatically on page open
- Search/filter customers by name
- Click a customer to select it
- Click **Download CSV** — the file downloads instantly

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `NABLE_TOKEN` | *(required)* | Your N-able JWT API token |
| `PYTHONUNBUFFERED` | `1` | Real-time container logs |

Port is mapped to `8081` by default to avoid conflict with the Emerald Monitor on `8080`. Change in `docker-compose.yml` if needed.

---

## How It Works

1. On page load, the backend authenticates with the N-able API and fetches the list of all unique customer names (cached for 5 minutes)
2. When you click **Download CSV**, the backend fetches all devices for that customer, enriches each device with detailed hardware data via a per-device API call, and streams the result as a CSV file
3. The CSV opens directly in Excel

---

## Running Both Tools Together

You can run this alongside the [Emerald Server Status Monitor](../emerald-monitor) using a combined compose file, or just run both separately — they use different ports.

---

## Troubleshooting

**Customer list won't load**
Check container logs: `docker logs nable-export`

**AUTH FAILED in logs**
Your JWT has expired. Regenerate it in N-able, update `docker-compose.yml`, then:
```bash
docker compose down && docker compose up -d
```

**CSV downloads but columns show N/A**
Some hardware fields depend on the N-able agent reporting them. Devices that have never sent hardware inventory will show N/A for CPU/RAM/disk fields — this is expected.

---

## Project Structure

| File | Purpose |
|---|---|
| `app.py` | API proxy + CSV generator + HTTP server |
| `index.html` | Frontend UI (customer search + download) |
| `Dockerfile` | Container image (Python 3.9 slim) |
| `docker-compose.yml` | Service definition |
| `.github/workflows/docker-build.yml` | CI/CD — builds `linux/amd64` + `linux/arm64` and pushes to Docker Hub |
