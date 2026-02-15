import io
import os
import smtplib
import threading
import time
from email.message import EmailMessage

import pandas as pd
import requests
from flask import Flask, jsonify

# =====================================
# CONFIG
# =====================================

CHOTU_URL = "https://api.chotu.com/api/biz/latlong"

CHOTU_API_KEY = os.getenv("CHOTU_API_KEY")

HEADERS = {
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "chotu_api_key": CHOTU_API_KEY,
}

CATEGORY = "stationery"
RADIUS = 20
GRID_STEP = 0.3

STATES = [
    "Tamil Nadu",
    "Karnataka",
]

# Email config
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_APP_PASSWORD")
EMAIL_TO = os.getenv("EMAIL_TO")

# =====================================
# VALIDATION
# =====================================

if not CHOTU_API_KEY:
    raise Exception("CHOTU_API_KEY not set in environment variables")

if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO:
    raise Exception("Email environment variables missing")

# =====================================
# GET STATE BOUNDING BOX
# =====================================


def get_state_bbox(state_name):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{state_name}, India",
        "format": "json",
        "limit": 1,
    }

    response = requests.get(
        url,
        params=params,
        headers={"User-Agent": "geo-scraper"},
        timeout=30,
    )

    data = response.json()
    if not data:
        return None

    bbox = data[0]["boundingbox"]

    return {
        "min_lat": float(bbox[0]),
        "max_lat": float(bbox[1]),
        "min_lon": float(bbox[2]),
        "max_lon": float(bbox[3]),
    }


# =====================================
# GENERATE GRID
# =====================================


def generate_grid(bbox, step):
    points = []
    lat = bbox["min_lat"]

    while lat <= bbox["max_lat"]:
        lon = bbox["min_lon"]
        while lon <= bbox["max_lon"]:
            points.append((round(lat, 4), round(lon, 4)))
            lon += step
        lat += step

    return points


# =====================================
# CALL CHOTU API
# =====================================


def fetch_businesses(lat, lon):
    params = {
        "lat": lat,
        "long": lon,
        "cat": CATEGORY,
        "radius": RADIUS,
    }
    response = requests.get(CHOTU_URL, params=params, headers=HEADERS, timeout=30)
    return response.json()


# =====================================
# SEND EMAIL WITH CSV
# =====================================


def send_email(state_name, df):
    msg = EmailMessage()
    msg["Subject"] = f"{state_name} - Chotu Business Data"
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO
    msg.set_content(f"Attached is the scraped Chotu data for {state_name}.")

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    msg.add_attachment(
        csv_buffer.getvalue(),
        subtype="csv",
        filename=f"{state_name.replace(' ', '_')}.csv",
    )

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_USER, EMAIL_PASS)
        smtp.send_message(msg)


# =====================================
# MAIN WORKER
# =====================================


def run_scrape():
    for state_name in STATES:

        print(f"Processing {state_name}")

        bbox = get_state_bbox(state_name)
        if not bbox:
            print("Could not fetch bounding box")
            continue

        grid_points = generate_grid(bbox, GRID_STEP)
        print(f"Grid points: {len(grid_points)}")

        rows = []

        for i, (lat, lon) in enumerate(grid_points):
            print(f"{state_name} -> {i+1}/{len(grid_points)} : {lat}, {lon}")

            try:
                data = fetch_businesses(lat, lon)

                for biz in data.get("data", []):
                    rows.append({
                        "state": state_name,
                        "scan_lat": lat,
                        "scan_lon": lon,
                        "business_name": biz.get("name"),
                        "category": biz.get("category"),
                        "address": biz.get("address"),
                        "phone": biz.get("phone"),
                        "business_lat": biz.get("lat"),
                        "business_lon": biz.get("long"),
                    })

            except Exception as e:
                print("Error:", e)

            time.sleep(0.4)

        if rows:
            df = pd.DataFrame(rows)
            send_email(state_name, df)
            print(f"Email sent for {state_name}")
        else:
            print(f"No data found for {state_name}")

    print("ALL STATES COMPLETED & EMAILED")


# =====================================
# WEB SERVER
# =====================================

app = Flask(__name__)


@app.get("/")
def index():
    return jsonify({"status": "ok"})


@app.post("/run")
def run_job():
    thread = threading.Thread(target=run_scrape, daemon=True)
    thread.start()
    return jsonify({"status": "started"})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
