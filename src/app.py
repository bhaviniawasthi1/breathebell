import json
import os
import time
import uuid
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
schools = dynamodb.Table(os.environ["SCHOOLS_TABLE"])
history = dynamodb.Table(os.environ["HISTORY_TABLE"])
TOPIC_ARN = os.environ["TOPIC_ARN"]

AIR_URL = "https://air-quality-api.open-meteo.com/v1/air-quality?"
GEO_URL = "https://geocoding-api.open-meteo.com/v1/search?"

# School hours: 08:00 - 16:00 (local time of the school)
SCHOOL_START = 8
SCHOOL_END = 16

# PM2.5 (ug/m3) limits per group: (YES up to, LIMITED up to, above that = INDOOR)
# Adapted from CPCB / WHO bands; younger and sensitive children get stricter limits.
THRESH = {
    "primary": (30, 60),
    "middle": (45, 75),
    "senior": (60, 90),
    "sensitive": (15, 30),
}
GROUP_NAMES = {
    "primary": "Primary (Class 1-5)",
    "middle": "Middle (Class 6-8)",
    "senior": "Senior (Class 9-12)",
    "sensitive": "Sensitive (asthma/allergy)",
}
REGULAR = ("primary", "middle", "senior")
RANK = {"YES": 0, "LIMITED": 1, "INDOOR": 2}
# Assumption for the impact estimate: LIMITED hours cut outdoor exposure by half
IMPACT_WEIGHT = {"YES": 0.0, "LIMITED": 0.5, "INDOOR": 1.0}
DEC_TEXT = {"YES": "Allowed", "LIMITED": "Limited", "INDOOR": "Indoor only"}

DECISIONS = {
    "YES": {
        "color": "#2e9e5b",
        "headline": "Children can play outside today",
        "advice": ["PE, sports and assembly can be held outdoors.",
                   "Keep an eye on children with asthma or allergies."],
    },
    "LIMITED": {
        "color": "#e8830c",
        "headline": "Keep outdoor play light and short",
        "advice": ["Keep outdoor activity light and under 30 minutes.",
                   "Move heavy sports such as running and football indoors.",
                   "Children with asthma or allergies should stay inside."],
    },
    "INDOOR": {
        "color": "#d62f2f",
        "headline": "Keep all activities indoors today",
        "advice": ["Hold sports, PE and assembly indoors.",
                   "Keep windows closed and take breaks inside the classroom.",
                   "Children who go outside should wear a mask."],
    },
}

AQI_BANDS = [
    (50, "GOOD", "#2e9e5b"),
    (100, "MODERATE", "#d4b106"),
    (150, "SENSITIVE", "#e8830c"),
    (200, "UNHEALTHY", "#d62f2f"),
    (300, "VERY_UNHEALTHY", "#8b3fb8"),
    (10**9, "HAZARDOUS", "#7a1f1f"),
]


# ---------------------------------------------------------------- helpers
def to_json(o):
    if isinstance(o, Decimal):
        return int(o) if o == o.to_integral_value() else float(o)
    raise TypeError(str(type(o)))


def resp(code, body):
    return {
        "statusCode": code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=to_json),
    }


def dec(x):
    return Decimal(str(x if x is not None else 0))


def get_json(url):
    last = None
    for _ in range(2):
        try:
            with urllib.request.urlopen(url, timeout=8) as r:
                return json.loads(r.read().decode())
        except Exception as e:  # retry once
            last = e
    raise last


def scan_all(table):
    items, kwargs = [], {}
    while True:
        r = table.scan(**kwargs)
        items.extend(r.get("Items", []))
        if "LastEvaluatedKey" not in r:
            return items
        kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def count_of(s, key):
    try:
        return int(s.get(key, 0) or 0)
    except (TypeError, ValueError):
        return 0


def total_students(s):
    return sum(count_of(s, g) for g in REGULAR)


def geocode(city):
    """Look in India first (this tool is built for Indian schools), then worldwide."""
    results = None
    for country in ("IN", None):
        params = {"name": city, "count": 1}
        if country:
            params["countryCode"] = country
        results = get_json(GEO_URL + urllib.parse.urlencode(params)).get("results")
        if results:
            break
    if not results:
        return None
    r = results[0]
    place = ", ".join(x for x in [r.get("name"), r.get("admin1")] if x)
    district = r.get("admin2") or r.get("admin1") or r.get("name") or "Unknown"
    return {"lat": r["latitude"], "lon": r["longitude"],
            "place": place, "district": district}


# ---------------------------------------------------------------- air data
def build_forecast(d, current_time, hours=48):
    h = d.get("hourly") or {}
    times = h.get("time") or []
    pm = h.get("pm2_5") or []
    aq = h.get("us_aqi") or []
    start = 0
    if current_time:
        cur = current_time[:13]
        for i, t in enumerate(times):
            if t[:13] == cur:
                start = i
                break
    out = []
    for i in range(start, min(start + hours, len(times))):
        if i >= len(pm) or pm[i] is None:
            continue
        out.append({
            "t": times[i],
            "pm25": round(float(pm[i]), 1),
            "aqi": int(round(aq[i])) if i < len(aq) and aq[i] is not None else None,
        })
    return out


def fetch_air(lat, lon):
    q = urllib.parse.urlencode({
        "latitude": str(float(lat)),
        "longitude": str(float(lon)),
        "current": "us_aqi,pm2_5,pm10",
        "hourly": "pm2_5,us_aqi",
        "forecast_days": "3",
        "timezone": "auto",
    })
    d = get_json(AIR_URL + q)
    c = d.get("current") or {}
    if c.get("us_aqi") is None or c.get("pm2_5") is None:
        raise ValueError("Air quality data is not available")
    return {
        "aqi": int(round(c["us_aqi"])),
        "pm25": round(float(c["pm2_5"]), 1),
        "pm10": round(float(c["pm10"]), 1) if c.get("pm10") is not None else None,
        "time": c.get("time"),
        "forecast": build_forecast(d, c.get("time")),
    }


# ---------------------------------------------------------------- decision engine
def aqi_level(aqi):
    for max_aqi, level, color in AQI_BANDS:
        if aqi <= max_aqi:
            return {"level": level, "color": color}


def group_decision(pm25, group):
    yes_max, limited_max = THRESH[group]
    if pm25 <= yes_max:
        return "YES"
    if pm25 <= limited_max:
        return "LIMITED"
    return "INDOOR"


def regular_groups(s):
    g = [k for k in REGULAR if count_of(s, k) > 0]
    return g or list(REGULAR)


def decide_school(pm25, s):
    """Return (per-group decisions, school-wide decision).
    School-wide = most restrictive among the regular age groups present."""
    groups = {g: group_decision(pm25, g) for g in regular_groups(s)}
    overall = max(groups.values(), key=lambda x: RANK[x])
    if count_of(s, "sensitive") > 0:
        groups["sensitive"] = group_decision(pm25, "sensitive")
    return groups, overall


def hour_of(t):
    return int(t[11:13])


def date_of(t):
    return t[:10]


def day_label(date, today):
    try:
        d = datetime.strptime(date, "%Y-%m-%d").date()
        diff = (d - datetime.strptime(today, "%Y-%m-%d").date()).days
    except Exception:
        return date
    if diff in (0, 1):
        return {0: "Today", 1: "Tomorrow"}[diff]
    return "%d %s" % (d.day, d.strftime("%b"))


def best_windows(forecast, s, today):
    """Cleanest 2-hour slot inside school hours, for each day in the forecast."""
    by_date = {}
    for i in range(len(forecast) - 1):
        a, b = forecast[i], forecast[i + 1]
        if date_of(a["t"]) != date_of(b["t"]):
            continue
        hr = hour_of(a["t"])
        if hr < SCHOOL_START or hr + 2 > SCHOOL_END or hour_of(b["t"]) != hr + 1:
            continue
        avg = round((a["pm25"] + b["pm25"]) / 2, 1)
        date = date_of(a["t"])
        if date not in by_date or avg < by_date[date]["avg_pm25"]:
            by_date[date] = {"date": date, "start": hr, "end": hr + 2, "avg_pm25": avg}
    wins = sorted(by_date.values(), key=lambda w: w["date"])
    for w in wins:
        w["day"] = day_label(w["date"], today)
        w["label"] = "%02d:00 - %02d:00" % (w["start"], w["end"])
        w["decision"] = decide_school(w["avg_pm25"], s)[1]
    best = min(wins, key=lambda w: w["avg_pm25"]) if wins else None
    return wins, best


def impact_estimate(forecast, s):
    """Student-hours of outdoor exposure that following the advice avoids
    during school hours in the next 48h (LIMITED hours count as half)."""
    counts = {g: count_of(s, g) for g in REGULAR}
    total, hours = 0.0, 0
    for f in forecast:
        if f["pm25"] is None or not (SCHOOL_START <= hour_of(f["t"]) < SCHOOL_END):
            continue
        hours += 1
        for g, n in counts.items():
            total += n * IMPACT_WEIGHT[group_decision(f["pm25"], g)]
    return {"student_hours": int(round(total)), "school_hours": hours,
            "students": sum(counts.values())}


def evaluate(s, air):
    pm25 = air["pm25"]
    groups, overall = decide_school(pm25, s)
    info = DECISIONS[overall]
    lvl = aqi_level(air["aqi"])
    today = (air["time"] or "")[:10]
    forecast = [{**f, "decision": decide_school(f["pm25"], s)[1]} for f in air["forecast"]]
    windows, best = best_windows(air["forecast"], s, today)
    advice = list(info["advice"])
    if pm25 >= 150:
        advice.append("PM2.5 is very high. Consider closing the school or holding online classes.")
    return {
        "id": s["id"],
        "school": s["name"],
        "place": s.get("place", ""),
        "district": s.get("district") or s.get("place") or "Unknown",
        "students": total_students(s),
        "time": air["time"],
        "aqi": air["aqi"],
        "pm25": pm25,
        "pm10": air["pm10"],
        "level": lvl["level"],
        "aqi_color": lvl["color"],
        "decision": overall,
        "color": info["color"],
        "outdoor_ok": overall == "YES",
        "headline": info["headline"],
        "advice": advice,
        "groups": [{
            "group": g,
            "name": GROUP_NAMES[g],
            "students": count_of(s, g),
            "decision": d,
            "yes_max": THRESH[g][0],
            "limited_max": THRESH[g][1],
        } for g, d in groups.items()],
        "best_window": best,
        "windows": windows,
        "forecast": forecast,
        "impact": impact_estimate(air["forecast"], s),
    }


def eval_school(s):
    try:
        return evaluate(s, fetch_air(s["lat"], s["lon"]))
    except Exception as e:
        print("EVAL FAIL", s.get("id"), repr(e))
        return {"id": s["id"], "school": s.get("name", ""), "place": s.get("place", ""),
                "district": s.get("district") or s.get("place") or "Unknown",
                "error": "Data not available"}


def eval_all(items):
    if not items:
        return []
    with ThreadPoolExecutor(max_workers=min(8, len(items))) as pool:
        return list(pool.map(eval_school, items))


def save_reading(school_id, ev):
    ts = int(time.time())
    history.put_item(Item={
        "school_id": school_id,
        "ts": ts,
        "aqi": ev["aqi"],
        "pm25": dec(ev["pm25"]),
        "pm10": dec(ev["pm10"]),
        "level": ev["level"],
        "decision": ev["decision"],
        "expires": ts + 7 * 86400,
    })


# ---------------------------------------------------------------- API
def to_count(v, label):
    if v in (None, ""):
        return 0
    try:
        n = int(v)
    except (TypeError, ValueError):
        raise ValueError("%s must be a number" % label)
    if n < 0 or n > 100000:
        raise ValueError("%s is out of range" % label)
    return n


def create_school(body):
    name = (body.get("name") or "").strip()
    city = (body.get("city") or "").strip()
    lat_raw, lon_raw = body.get("lat"), body.get("lon")
    if not name:
        raise ValueError("School name is required")

    counts = {k: to_count(body.get(k), k) for k in REGULAR}
    counts["sensitive"] = to_count(body.get("sensitive"), "sensitive")
    if sum(counts[k] for k in REGULAR) <= 0:
        raise ValueError("Enter the number of students for at least one class group")

    if lat_raw not in (None, "") and lon_raw not in (None, ""):
        try:
            lat, lon = float(lat_raw), float(lon_raw)
        except (TypeError, ValueError):
            raise ValueError("Latitude and longitude must be numbers")
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError("Latitude or longitude is out of range")
        geo = {"lat": lat, "lon": lon,
               "place": city or "%.3f, %.3f" % (lat, lon),
               "district": (body.get("district") or "").strip() or city or "Unknown"}
    else:
        if not city:
            raise ValueError("Enter a city, or both latitude and longitude")
        geo = geocode(city)
        if not geo:
            raise ValueError("City not found. Please try again")
        if (body.get("district") or "").strip():
            geo["district"] = body["district"].strip()

    item = {
        "id": uuid.uuid4().hex[:8],
        "name": name[:80],
        "city": city[:80],
        "place": geo["place"],
        "district": geo["district"][:80],
        "lat": dec(geo["lat"]),
        "lon": dec(geo["lon"]),
        "primary": counts["primary"],
        "middle": counts["middle"],
        "senior": counts["senior"],
        "sensitive": counts["sensitive"],
        "last_level": "NONE",
        "last_decision": "NONE",
        "created": int(time.time()),
    }
    schools.put_item(Item=item)
    return resp(201, {"id": item["id"], "name": item["name"], "place": item["place"],
                      "district": item["district"]})


def list_schools():
    out = [{"id": s["id"], "name": s["name"], "place": s.get("place", ""),
            "district": s.get("district") or s.get("place") or "Unknown",
            "students": total_students(s)} for s in scan_all(schools)]
    return resp(200, out)


def get_school(school_id):
    return schools.get_item(Key={"id": school_id}).get("Item")


def school_status(school_id):
    s = get_school(school_id)
    if not s:
        return resp(404, {"error": "School not found"})
    ev = evaluate(s, fetch_air(s["lat"], s["lon"]))
    save_reading(school_id, ev)
    return resp(200, ev)


def school_history(school_id):
    r = history.query(
        KeyConditionExpression=Key("school_id").eq(school_id),
        ScanIndexForward=False,
        Limit=24,
    )
    items = list(reversed(r.get("Items", [])))
    return resp(200, [{"ts": i["ts"], "aqi": i["aqi"], "pm25": i.get("pm25"),
                       "level": i.get("level"), "decision": i.get("decision")}
                      for i in items])


def delete_school(school_id):
    schools.delete_item(Key={"id": school_id})
    r = history.query(KeyConditionExpression=Key("school_id").eq(school_id))
    with history.batch_writer() as bw:
        for i in r.get("Items", []):
            bw.delete_item(Key={"school_id": i["school_id"], "ts": i["ts"]})
    return resp(200, {"deleted": school_id})


def overview():
    items = scan_all(schools)
    results = eval_all(items)
    ok = [r for r in results if "error" not in r]
    failed = [r for r in results if "error" in r]

    rows = []
    for r in ok:
        rows.append({
            "id": r["id"], "school": r["school"], "place": r["place"],
            "district": r["district"], "students": r["students"],
            "pm25": r["pm25"], "aqi": r["aqi"], "decision": r["decision"],
            "color": r["color"], "best_window": r["best_window"],
            "student_hours": r["impact"]["student_hours"],
        })
    rows.sort(key=lambda x: x["pm25"], reverse=True)

    districts = {}
    for row in rows:
        districts.setdefault(row["district"], []).append(row)
    dist_out = []
    for name, rs in districts.items():
        worst = max((x["decision"] for x in rs), key=lambda d: RANK[d])
        dist_out.append({
            "district": name,
            "schools": rs,
            "school_count": len(rs),
            "students": sum(x["students"] for x in rs),
            "avg_pm25": round(sum(x["pm25"] for x in rs) / len(rs), 1),
            "worst_decision": worst,
            "color": DECISIONS[worst]["color"],
            "indoor_schools": sum(1 for x in rs if x["decision"] == "INDOOR"),
            "student_hours": sum(x["student_hours"] for x in rs),
        })
    dist_out.sort(key=lambda d: d["avg_pm25"], reverse=True)

    totals = {
        "schools": len(rows),
        "students": sum(x["students"] for x in rows),
        "yes": sum(1 for x in rows if x["decision"] == "YES"),
        "limited": sum(1 for x in rows if x["decision"] == "LIMITED"),
        "indoor": sum(1 for x in rows if x["decision"] == "INDOOR"),
        "student_hours": sum(x["student_hours"] for x in rows),
        "unavailable": len(failed),
    }
    return resp(200, {
        "totals": totals,
        "districts": dist_out,
        "schools": rows,
        "assumption": ("Impact = students x school-hours (08-16) in the next 48h where the "
                       "advice is Indoor (full) or Limited (half). It is an estimate."),
    })


def api_handler(event, context):
    try:
        route = event["routeKey"]
        params = event.get("pathParameters") or {}
        if route == "POST /schools":
            return create_school(json.loads(event.get("body") or "{}"))
        if route == "GET /schools":
            return list_schools()
        if route == "GET /overview":
            return overview()
        if route == "GET /schools/{id}/status":
            return school_status(params["id"])
        if route == "GET /schools/{id}/history":
            return school_history(params["id"])
        if route == "DELETE /schools/{id}":
            return delete_school(params["id"])
        return resp(404, {"error": "Route not found"})
    except ValueError as e:
        return resp(400, {"error": str(e)})
    except Exception as e:
        print("ERROR", repr(e))
        return resp(500, {"error": "Server error"})


# ---------------------------------------------------------------- scheduled jobs
def group_line(ev):
    return " | ".join("%s: %s" % (g["name"].split(" ")[0], DEC_TEXT[g["decision"]])
                      for g in ev["groups"])


def notify(ev, old):
    subject = "BreatheBell alert: %s - %s (PM2.5 %s)" % (
        ev["school"], DEC_TEXT[ev["decision"]], ev["pm25"])
    best = ev["best_window"]
    message = (
        "School: %s (%s)\nPM2.5: %s | AQI: %s\nBefore: %s -> Now: %s\n\n%s\n%s\n\n"
        "Best outdoor slot: %s\n\nAdvice:\n- %s"
        % (ev["school"], ev["place"], ev["pm25"], ev["aqi"],
           old, ev["decision"], ev["headline"], group_line(ev),
           ("%s %s (PM2.5 ~%s)" % (best["day"], best["label"], best["avg_pm25"])) if best else "no slot available",
           "\n- ".join(ev["advice"]))
    )
    sns.publish(TopicArn=TOPIC_ARN, Subject=subject[:99], Message=message)


def scheduled_handler(event, context):
    """Hourly: save readings and alert when a school enters/leaves INDOOR."""
    items = scan_all(schools)
    for s, ev in zip(items, eval_all(items)):
        if "error" in ev:
            continue
        try:
            save_reading(s["id"], ev)
            old = s.get("last_decision", "NONE")
            if ev["decision"] != old and (ev["decision"] == "INDOOR" or old == "INDOOR"):
                notify(ev, old)
            schools.update_item(
                Key={"id": s["id"]},
                UpdateExpression="SET last_level = :l, last_decision = :d",
                ExpressionAttributeValues={":l": ev["level"], ":d": ev["decision"]},
            )
        except Exception as e:
            print("FAIL", s.get("id"), repr(e))
    return {"ok": True}


def digest_handler(event, context):
    """Every morning 07:00 IST: one email with today's decision for every school."""
    items = scan_all(schools)
    results = [r for r in eval_all(items) if "error" not in r]
    if not results:
        return {"ok": True, "sent": False}
    results.sort(key=lambda r: r["pm25"], reverse=True)
    today = datetime.utcnow().strftime("%d %b %Y")
    blocks = []
    for ev in results:
        best = ev["best_window"]
        blocks.append(
            "%s (%s)\n  Now: PM2.5 %s | AQI %s -> %s\n  %s\n  Best outdoor slot: %s\n  Impact (48h): ~%s student-hours"
            % (ev["school"], ev["district"], ev["pm25"], ev["aqi"], DEC_TEXT[ev["decision"]],
               group_line(ev),
               ("%s %s (PM2.5 ~%s)" % (best["day"], best["label"], best["avg_pm25"])) if best else "no slot available",
               ev["impact"]["student_hours"]))
    message = "BreatheBell morning digest (%s)\nSchools with the worst air are listed first.\n\n%s" % (
        today, "\n\n".join(blocks))
    sns.publish(TopicArn=TOPIC_ARN, Subject=("BreatheBell morning digest - %s" % today)[:99],
                Message=message)
    return {"ok": True, "sent": True, "schools": len(results)}
