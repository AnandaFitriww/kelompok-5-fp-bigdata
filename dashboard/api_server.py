"""
Flask API Server — Serve Gold Layer predictions ke dashboard.

Membaca JSON exports dari Gold Spark job dan menyajikan via REST API.
"""

from flask import Flask, jsonify
from flask_cors import CORS
import json
import os

app = Flask(__name__)
CORS(app)  # Enable CORS untuk akses dari Next.js dashboard

# Path ke gold JSON exports (relatif dari file ini)
GOLD_EXPORTS = os.path.join(os.path.dirname(__file__), "..", "gold_exports")

BUS_CAPACITY = 60


def read_export(filename):
    """Baca JSON export file. Return empty dict jika belum ada."""
    filepath = os.path.join(GOLD_EXPORTS, filename)
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"data": [], "count": 0, "updated_at": None, "bus_capacity": BUS_CAPACITY}
    except json.JSONDecodeError:
        return {"data": [], "count": 0, "updated_at": None, "bus_capacity": BUS_CAPACITY,
                "error": "Invalid JSON file"}


# ============================================================
# API ENDPOINTS
# ============================================================

@app.route("/api/health", methods=["GET"])
def health():
    """Health check."""
    return jsonify({"status": "ok", "bus_capacity": BUS_CAPACITY})


@app.route("/api/bus-occupancy", methods=["GET"])
def bus_occupancy():
    """
    Current occupancy per bus.
    Response: { data: [{bus_id, occupancy, occupancy_pct, route_id, ...}], ... }
    """
    return jsonify(read_export("bus_occupancy_current.json"))


@app.route("/api/stop-congestion", methods=["GET"])
def stop_congestion():
    """
    Shelter Congestion Index per halte.
    Response: { data: [{stop_name, congestion_index, congestion_level, demand_count, ...}], ... }
    """
    return jsonify(read_export("shelter_congestion.json"))


@app.route("/api/predictions/overcapacity", methods=["GET"])
def predictions_overcapacity():
    """
    Stop overcapacity risk predictions.
    Response: { data: [{stop_name, predicted_demand_15min, risk_score, risk_level, ...}], ... }
    """
    result = read_export("stop_overcapacity.json")
    # Sort by risk score descending
    if result["data"]:
        result["data"] = sorted(
            result["data"],
            key=lambda x: x.get("overcapacity_risk_score", 0) or 0,
            reverse=True
        )
    return jsonify(result)


@app.route("/api/predictions/bus-arrival", methods=["GET"])
def predictions_bus_arrival():
    """
    Bus arrival occupancy predictions.
    Response: { data: [{bus_id, next_stop, predicted_occupancy, predicted_occupancy_pct, ...}], ... }
    """
    result = read_export("bus_arrival_occupancy.json")
    # Sort by predicted occupancy pct descending (most full first)
    if result["data"]:
        result["data"] = sorted(
            result["data"],
            key=lambda x: x.get("predicted_occupancy_pct", 0) or 0,
            reverse=True
        )
    return jsonify(result)


@app.route("/api/summary", methods=["GET"])
def summary():
    """
    Ringkasan semua predictions untuk panel dashboard.
    """
    congestion = read_export("shelter_congestion.json")
    overcapacity = read_export("stop_overcapacity.json")
    bus_arrival = read_export("bus_arrival_occupancy.json")
    bus_occ = read_export("bus_occupancy_current.json")

    # Count alerts
    critical_stops = sum(
        1 for s in congestion.get("data", [])
        if s.get("congestion_level") == "CRITICAL"
    )
    high_risk_stops = sum(
        1 for s in overcapacity.get("data", [])
        if s.get("risk_level") in ("CRITICAL", "HIGH")
    )
    full_buses = sum(
        1 for b in bus_occ.get("data", [])
        if (b.get("occupancy_pct") or 0) >= 80
    )

    return jsonify({
        "bus_capacity": BUS_CAPACITY,
        "total_buses": len(bus_occ.get("data", [])),
        "total_stops": len(congestion.get("data", [])),
        "alerts": {
            "critical_congestion_stops": critical_stops,
            "high_risk_overcapacity_stops": high_risk_stops,
            "near_full_buses": full_buses,
        },
        "updated_at": bus_occ.get("updated_at"),
    })


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    print("🌐 ShelterEye API Server")
    print(f"   Gold exports dir: {os.path.abspath(GOLD_EXPORTS)}")
    print(f"   BUS_CAPACITY: {BUS_CAPACITY}")
    print(f"   Server: http://localhost:5000")

    app.run(host="0.0.0.0", port=5000, debug=True)
