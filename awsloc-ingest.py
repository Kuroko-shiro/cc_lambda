# lambda_function.py
import os, json, time, datetime
import boto3

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"

s3 = boto3.client("s3", region_name=REGION)
location = boto3.client("location", region_name=REGION)

RAW_BUCKET  = os.environ["RAW_BUCKET"]
TRACKER     = os.environ.get("TRACKER_NAME")
PLACE_INDEX = os.environ.get("PLACE_INDEX") 

def _cors_headers():
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST",
        "Access-Control-Allow-Headers": "Content-Type,x-api-key"
    }

def _resp(code: int, body: dict):
    return {"statusCode": code, "headers": _cors_headers(), "body": json.dumps(body, ensure_ascii=False)}

def _to_ms(ts):
    if ts is None:
        return int(time.time() * 1000)
    if isinstance(ts, (int, float)):
        return int(ts if ts > 1e12 else ts * 1000)
    if isinstance(ts, str):
        try:
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            dt = datetime.datetime.fromisoformat(ts)
            return int(dt.timestamp() * 1000)
        except Exception:
            return int(time.time() * 1000)
    return int(time.time() * 1000)

def _iso_utc_from_ms(ms: int) -> str:
    return datetime.datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%dT%H:%M:%SZ")

def lambda_handler(event, context):
    if (
        event.get("httpMethod") == "OPTIONS"
        or event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS"
    ):
        return _resp(200, {"ok": True})
    try:
        body = json.loads(event.get("body") or "{}")
    except Exception:
        return _resp(400, {"ok": False, "error": "invalid_json"})

    device_id = str(body.get("deviceId") or "web-unknown")
    records = []
    if isinstance(body.get("locations"), list):
        for p in body["locations"]:
            try:
                lat = float(p.get("latitude", p.get("lat")))
                lon = float(p.get("longitude", p.get("lon")))
                ts_ms = _to_ms(p.get("timestamp"))
                records.append({"lat": lat, "lon": lon, "ts_ms": ts_ms})
            except Exception:
                continue
    else:
        try:
            lat = float(body["latitude"])
            lon = float(body["longitude"])
            ts_ms = _to_ms(body.get("timestamp"))
            records.append({"lat": lat, "lon": lon, "ts_ms": ts_ms})
        except Exception as e:
            return _resp(400, {"ok": False, "error": str(e)})

    if not records:
        return _resp(400, {"ok": False, "error": "no_valid_locations"})
    rid = (getattr(context, "aws_request_id", "") or "")[:8] or str(int(time.time()) % 100000)

    # 1) Location Tracker
    if TRACKER:
        updates = []
        for r in records:
            updates.append({
                "DeviceId": device_id,
                "Position": [r["lon"], r["lat"]],
                "SampleTime": _iso_utc_from_ms(r["ts_ms"])
            })
        for i in range(0, len(updates), 10):
            try:
                location.batch_update_device_position(TrackerName=TRACKER, Updates=updates[i:i+10])
            except Exception as e:
                print("tracker update error:", e)

    # 2) 逆ジオ（任意）: 結果を body に付加して raw として保存
    def reverse_geocode(lon, lat):
        if not PLACE_INDEX:
            return None
        try:
            res = location.search_place_index_for_position(
                IndexName=PLACE_INDEX, Position=[lon, lat], MaxResults=1
            )
            if res.get("Results"):
                return res["Results"][0]["Place"].get("Label")
        except Exception as e:
            print("reverse geocode error:", e)
        return None

    # 3) S3 保存
    saved_keys = []
    for idx, r in enumerate(records):
        enriched = {
            "deviceId": device_id,
            "timestamp": r["ts_ms"],
            "latitude":  r["lat"],
            "longitude": r["lon"]
        }
        label = reverse_geocode(r["lon"], r["lat"])
        if label:
            enriched["address"] = label
        key = f"raw/{device_id}/{r['ts_ms']}-{rid}-{idx}.json"
        try:
            s3.put_object(
                Bucket=RAW_BUCKET,
                Key=key,
                Body=json.dumps(enriched, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
                ContentType="application/json"
            )
            saved_keys.append(key)
        except Exception as e:
            print("s3 put error:", e)

    return _resp(200, {"ok": True, "saved": len(saved_keys), "keys": saved_keys})
