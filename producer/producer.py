"""
Egypt Traffic — Kafka Producer
================================
Polls TomTom Traffic API every 60 seconds and publishes to:
  - traffic-flow      (flow segment readings per location)
  - traffic-incidents (active incidents per bounding box)
  - traffic-dlq       (anything that fails schema validation or serialization)

Avro schemas are registered with Confluent Schema Registry on first run.

Install:
  pip install confluent-kafka[avro] requests fastavro python-dotenv

Run:
  export TOMTOM_API_KEY="..."
  python producer/producer.py
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

TOMTOM_API_KEY      = os.environ["TOMTOM_API_KEY"]
KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP",     "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
POLL_INTERVAL_S     = int(os.getenv("POLL_INTERVAL_S", "900"))

TOPIC_FLOW      = "traffic-flow"
TOPIC_INCIDENTS = "traffic-incidents"
TOPIC_DLQ       = "traffic-dlq"

TOMTOM_BASE = "https://api.tomtom.com"


LOCATIONS = {
    # ── Cairo Ring Road (4 points covering N/E/S/W quadrants) ─────────────────
    "ring_road_north":       (30.1550,  31.3200),  # northern arc near Heliopolis
    "ring_road_east":        (30.0700,  31.5200),  # eastern arc near New Cairo
    "ring_road_south":       (29.9500,  31.2800),  # southern arc near Katameya
    "ring_road_west":        (30.0300,  31.0500),  # western arc near 6 October
 
    # ── Downtown / central Cairo arteries ─────────────────────────────────────
    "tahrir_square":         (30.0444,  31.2357),  # Tahrir — Qasr El Aini junction
    "ramses_square":         (30.0626,  31.2497),  # Ramses — Galaa St / Port Said St
    "salah_salem_north":     (30.0700,  31.2900),  # Salah Salem near Abbasiya
    "salah_salem_south":     (30.0100,  31.3000),  # Salah Salem near Autostrad
    "corniche_downtown":     (30.0550,  31.2280),  # Nile Corniche near Garden City
 
    # ── Airport corridor ──────────────────────────────────────────────────────
    "cairo_airport":         (30.1219,  31.4056),  # airport terminal approach
 
    # ── October 6th / Mehwar corridor ─────────────────────────────────────────
    "6th_october_bridge":    (30.0544,  31.2230),  # 6 October bridge over Nile
    "mehwar_north":          (30.0800,  31.1800),  # Al Mehwar near Imbaba
 
    # ── Giza ──────────────────────────────────────────────────────────────────
    "giza_square":           (30.0082,  31.2114),  # Giza Square / Pyramids Rd start
 
    # ── East Cairo ────────────────────────────────────────────────────────────
    "new_admin_capital":     (30.0131,  31.7392),  # NAC main boulevard
    "new_cairo_90th":        (30.0050,  31.4700),  # 90th Street, New Cairo
 
    # ── Alexandria — Corniche ─────────────────────────────────────────────────
    "alex_corniche_east":    (31.2156,  29.9553),  # Eastern Corniche / Sidi Bishr
    "alex_corniche_west":    (31.2050,  29.8800),  # Western Corniche near Stanley
 
    # ── Alexandria — inland arteries ──────────────────────────────────────────
    "alex_victoria_square":  (31.1980,  29.9100),  # Victoria Square / Port Said St
    "alex_port_area":        (31.2000,  29.8650),  # Western Harbour / port entrance
 
    # ── Alexandria — eastern suburbs ──────────────────────────────────────────
    "alex_montaza":          (31.2800,  30.0100),  # Montaza / Al Mamurah
    "alex_abu_qir_road":     (31.2600,  30.0600),  # Abu Qir Road / coastal highway
}
 
BBOXES = {
    # Tightened to actual urban Cairo — avoids picking up desert incidents
    "cairo_downtown":    (30.00, 31.18, 30.12, 31.35),
    "cairo_east":        (30.00, 31.35, 30.15, 31.55),
    "cairo_ring_road":   (29.92, 31.05, 30.18, 31.60),
    "alexandria":        (31.15, 29.82, 31.30, 30.10),
}

INCIDENT_CATEGORIES = {
    0: "Unknown", 1: "Accident", 2: "Fog", 3: "Dangerous conditions",
    4: "Rain", 5: "Ice", 6: "Jam", 7: "Lane closed", 8: "Road closed",
    9: "Road works", 10: "Wind", 11: "Flooding", 14: "Broken down vehicle",
}

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Schema loading & registration
# ──────────────────────────────────────────────────────────────────────────────

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")


def load_schema(filename: str) -> str:
    path = os.path.join(SCHEMA_DIR, filename)
    with open(path) as f:
        return f.read()


def build_serializer(schema_str: str, sr_client: SchemaRegistryClient) -> AvroSerializer:
    return AvroSerializer(sr_client, schema_str)


# ──────────────────────────────────────────────────────────────────────────────
# Kafka producer setup
# ──────────────────────────────────────────────────────────────────────────────

def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        # Backpressure: block when queue is full rather than dropping messages
        "queue.buffering.max.messages": 10_000,
        "queue.buffering.max.kbytes":   32_768,
        # Reliability: wait for all in-sync replicas
        "acks": "all",
        # Retry transient broker errors
        "retries": 5,
        "retry.backoff.ms": 500,
        # Batching — small latency tradeoff for throughput
        "linger.ms": 100,
        "compression.type": "snappy",
    })


def delivery_callback(err, msg):
    if err:
        log.error("Delivery failed for topic=%s key=%s: %s",
                  msg.topic(), msg.key(), err)
    else:
        log.debug("Delivered to %s [%d] @ offset %d",
                  msg.topic(), msg.partition(), msg.offset())


# ──────────────────────────────────────────────────────────────────────────────
# TomTom API calls  (same logic as explorer script)
# ──────────────────────────────────────────────────────────────────────────────

def _get(url: str, params: dict) -> dict | None:
    params["key"] = TOMTOM_API_KEY
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        log.warning("TomTom request failed: %s", e)
        return None


def _segment_midpoint(coordinates: list[dict]) -> tuple[float, float] | tuple[None, None]:
    """Return the midpoint lat/lon of a flow segment coordinate list."""
    if not coordinates:
        return None, None
    mid = coordinates[len(coordinates) // 2]
    return mid["latitude"], mid["longitude"]


def fetch_flow(location_name: str, lat: float, lon: float) -> dict | None:
    data = _get(
        f"{TOMTOM_BASE}/traffic/services/4/flowSegmentData/absolute/10/json",
        {"point": f"{lat},{lon}", "unit": "KMPH"},
    )
    if not data:
        return None
    seg   = data.get("flowSegmentData", {})
    ff    = seg.get("freeFlowSpeed", 0)
    cs    = seg.get("currentSpeed",  0)

    # Road segment geometry — list of {latitude, longitude} dicts
    coords     = seg.get("coordinates", {}).get("coordinate", [])
    seg_lat, seg_lon = _segment_midpoint(coords)

    return {
        "ingested_at":           int(datetime.now(timezone.utc).timestamp() * 1000),
        "location_name":         location_name,
        "query_lat":             lat,
        "query_lon":             lon,
        "segment_lat":           seg_lat,   # midpoint of actual road segment
        "segment_lon":           seg_lon,
        "frc":                   seg.get("frc", ""),
        "current_speed_kmh":     cs,
        "free_flow_speed_kmh":   ff,
        "current_travel_time":   seg.get("currentTravelTime", 0),
        "free_flow_travel_time": seg.get("freeFlowTravelTime", 0),
        "confidence":            float(seg.get("confidence", 0.0)),
        "road_closed":           bool(seg.get("roadClosure", False)),
        "congestion_ratio":      round(cs / ff, 3) if ff else None,
    }


def _linestring_midpoint(coordinates: list) -> tuple[float, float] | tuple[None, None]:
    """Return midpoint [lat, lon] of a GeoJSON LineString coordinate array."""
    if not coordinates:
        return None, None
    mid = coordinates[len(coordinates) // 2]  # [lon, lat] in GeoJSON
    return mid[1], mid[0]


def fetch_incidents(bbox_name: str, bbox: tuple) -> list[dict]:
    min_lat, min_lon, max_lat, max_lon = bbox
    data = _get(
        f"{TOMTOM_BASE}/traffic/services/5/incidentDetails",
        {
            "bbox":     f"{min_lon},{min_lat},{max_lon},{max_lat}",
            "fields":   "{incidents{type,geometry{type,coordinates},properties{iconCategory,magnitudeOfDelay,from,to,delay,length,roadNumbers,startTime,endTime,timeValidity,probabilityOfOccurrence,numberOfReports,lastReportTime}}}",
            "language": "en-GB",
        },
    )
    if not data:
        return []

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    rows = []
    for inc in data.get("incidents", []):
        props  = inc.get("properties", {})
        geo    = inc.get("geometry", {})
        coords = geo.get("coordinates", [])

        # All real incidents come back as LineString — take midpoint
        geo_type = geo.get("type")
        if geo_type == "LineString":
            inc_lat, inc_lon = _linestring_midpoint(coords)
        elif geo_type == "Point" and len(coords) >= 2:
            inc_lon, inc_lat = coords[0], coords[1]
        else:
            inc_lat = inc_lon = None

        rows.append({
            "ingested_at":             now_ms,
            "bbox_name":               bbox_name,
            "category":                INCIDENT_CATEGORIES.get(props.get("iconCategory", 0), "Unknown"),
            "icon_category_id":        props.get("iconCategory", 0),
            "magnitude":               props.get("magnitudeOfDelay", 0),
            "from_location":           props.get("from"),
            "to_location":             props.get("to"),
            "delay_seconds":           props.get("delay") or 0,
            "length_meters":           props.get("length") or 0,
            "road_numbers":            ", ".join(props.get("roadNumbers") or []) or None,
            "start_time":              props.get("startTime"),
            "end_time":                props.get("endTime"),
            "time_validity":           props.get("timeValidity"),
            "probability_of_occurrence": props.get("probabilityOfOccurrence"),
            "number_of_reports":       props.get("numberOfReports"),
            "last_report_time":        props.get("lastReportTime"),
            "lat":                     inc_lat,
            "lon":                     inc_lon,
        })
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# DLQ helper
# ──────────────────────────────────────────────────────────────────────────────

def send_to_dlq(producer: Producer, key: str, record: dict, reason: str) -> None:
    payload = json.dumps({
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "reason":    reason,
        "record":    record,
    }).encode()
    producer.produce(TOPIC_DLQ, key=key.encode(), value=payload, callback=delivery_callback)
    log.warning("Sent to DLQ — key=%s reason=%s", key, reason)


# ──────────────────────────────────────────────────────────────────────────────
# Publish helpers
# ──────────────────────────────────────────────────────────────────────────────

def publish_flow(
    producer: Producer,
    flow_serializer: AvroSerializer,
    key_serializer: StringSerializer,
) -> None:
    for name, (lat, lon) in LOCATIONS.items():
        record = fetch_flow(name, lat, lon)
        if not record:
            log.warning("No flow data for %s, skipping", name)
            continue
        try:
            key   = key_serializer(name)
            value = flow_serializer(
                record,
                SerializationContext(TOPIC_FLOW, MessageField.VALUE),
            )
            producer.produce(TOPIC_FLOW, key=key, value=value, callback=delivery_callback)
            log.info("flow → %s  speed=%d/%d  ratio=%s",
                     name, record["current_speed_kmh"],
                     record["free_flow_speed_kmh"], record["congestion_ratio"])
        except Exception as e:
            send_to_dlq(producer, name, record, str(e))

    producer.poll(0)  # trigger delivery callbacks without blocking


def publish_incidents(
    producer: Producer,
    incident_serializer: AvroSerializer,
    key_serializer: StringSerializer,
) -> None:
    for bbox_name, bbox in BBOXES.items():
        records = fetch_incidents(bbox_name, bbox)
        log.info("incidents → %s: %d records", bbox_name, len(records))
        for i, record in enumerate(records):
            try:
                key   = key_serializer(f"{bbox_name}_{i}")
                value = incident_serializer(
                    record,
                    SerializationContext(TOPIC_INCIDENTS, MessageField.VALUE),
                )
                producer.produce(TOPIC_INCIDENTS, key=key, value=value, callback=delivery_callback)
            except Exception as e:
                send_to_dlq(producer, f"{bbox_name}_{i}", record, str(e))

    producer.poll(0)


# ──────────────────────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("Starting Egypt Traffic Producer")
    log.info("  Bootstrap : %s", KAFKA_BOOTSTRAP)
    log.info("  Schema Reg: %s", SCHEMA_REGISTRY_URL)
    log.info("  Poll every: %ds", POLL_INTERVAL_S)

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    flow_schema     = load_schema("traffic_flow.avsc")
    incident_schema = load_schema("traffic_incident.avsc")

    flow_serializer     = build_serializer(flow_schema,     sr_client)
    incident_serializer = build_serializer(incident_schema, sr_client)
    key_serializer      = StringSerializer("utf_8")

    producer = build_producer()

    try:
        while True:
            cycle_start = time.monotonic()
            log.info("── Poll cycle %s ──", datetime.now().strftime("%H:%M:%S"))

            publish_flow(producer, flow_serializer, key_serializer)
            publish_incidents(producer, incident_serializer, key_serializer)

            # Flush at end of each cycle — ensures everything is delivered
            # before we sleep. Returns number of messages still in queue.
            remaining = producer.flush(timeout=30)
            if remaining:
                log.warning("%d messages not delivered after flush", remaining)

            elapsed = time.monotonic() - cycle_start
            sleep_s = max(0, POLL_INTERVAL_S - elapsed)
            log.info("Cycle done in %.1fs, sleeping %.1fs", elapsed, sleep_s)
            time.sleep(sleep_s)

    except KeyboardInterrupt:
        log.info("Shutting down — flushing remaining messages...")
        producer.flush(timeout=30)
        log.info("Done.")


if __name__ == "__main__":
    main()
