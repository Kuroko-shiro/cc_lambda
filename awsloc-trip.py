# awsloc-trips / lambda_function.py
import os
import json
import math
import logging
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger()
for h in list(logger.handlers or []):
    logger.removeHandler(h)
_handler = logging.StreamHandler()
_formatter = logging.Formatter(
    fmt='%(asctime)sZ\t%(levelname)s\t%(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'
)
_handler.setFormatter(_formatter)
logger.addHandler(_handler)

DEBUG_MODE = os.environ.get("DEBUG_MODE", "").lower() in ("1", "true", "yes", "on")
logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)

def jlog(level, **kv):
    """構造化ログ用の薄いラッパ。"""
    msg = json.dumps(kv, ensure_ascii=False, separators=(",", ":"))
    logger.log(level, msg)

S3_BUCKET        = os.environ.get("RAW_BUCKET")  # 例: awsloc-raw-xxx
AWS_REGION       = os.environ.get("AWS_REGION", "us-east-1")
ROUTE_CALCULATOR = os.environ.get("ROUTE_CALCULATOR")  # 無設定なら直線のみ

if not S3_BUCKET:
    logger.warning("RAW_BUCKET is not set; handler will skip objects from other buckets and may not match expectations.")

s3  = boto3.client("s3")
loc = boto3.client("location", region_name=AWS_REGION)


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088
    to_rad = math.radians
    dlat = to_rad(lat2 - lat1)
    dlon = to_rad(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(to_rad(lat1)) * math.cos(to_rad(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def read_json(bucket, key):
    """S3 から JSON を読み、型・サイズ情報をログ出し。"""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        size = head.get("ContentLength")
        etag = head.get("ETag")
        jlog(logging.INFO, event="s3_head", bucket=bucket, key=key, size=size, etag=etag)
    except Exception as e:
        jlog(logging.WARNING, event="s3_head_error", bucket=bucket, key=key, error=str(e))

    resp = s3.get_object(Bucket=bucket, Key=key)
    body_bytes = resp["Body"].read()
    text = body_bytes.decode(resp.get("ContentEncoding") or "utf-8")
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        jlog(logging.ERROR, event="json_decode_error", bucket=bucket, key=key, error=str(e), sample=text[:200])
        raise
    jlog(logging.INFO, event="s3_get_ok", bucket=bucket, key=key,
         type=type(data).__name__,
         list_len=(len(data) if isinstance(data, list) else None))
    if DEBUG_MODE and isinstance(data, list):
        # 先頭2件だけチラ見せ
        jlog(logging.DEBUG, event="stays_sample_head", head=data[:2])
    return data

def write_json(bucket, key, obj):
    s3.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json"
    )
    jlog(logging.INFO, event="s3_put_ok", bucket=bucket, key=key,
         bytes=len(json.dumps(obj, ensure_ascii=False)))

def _safe_float(v, name, idx=None):
    try:
        return float(v)
    except Exception:
        jlog(logging.WARNING, event="float_cast_fail", field=name, value=v, index=idx)
        return None

def _stay_center_ok(stay, idx=None):
    c = (stay or {}).get("center") or {}
    lat = c.get("lat")
    lon = c.get("lon")
    if lat is None or lon is None:
        jlog(logging.WARNING, event="stay_missing_center", index=idx, stay_keys=list((stay or {}).keys()))
        return False
    return True

def build_route_coords(src_lon, src_lat, dst_lon, dst_lat):
    """
    Amazon Location の CalculateRoute で折れ線を取得。
    - IncludeLegGeometry=True で Geometry を要求
    - 取れない/エラー時は直線 [[lon,lat],[lon,lat]] にフォールバック
    戻り値: (coords[[lon,lat],...], distance_km or None, used_fallback(bool))
    """
    fallback = [[src_lon, src_lat], [dst_lon, dst_lat]]
    if src_lon is None or src_lat is None or dst_lon is None or dst_lat is None:
        jlog(logging.WARNING, event="route_invalid_input",
             src=[src_lon, src_lat], dst=[dst_lon, dst_lat])
        return fallback, None, True

    if not ROUTE_CALCULATOR:
        dist = haversine_km(src_lat, src_lon, dst_lat, dst_lon)
        jlog(logging.DEBUG, event="route_calc_skipped_no_calculator",
             src=[src_lon, src_lat], dst=[dst_lon, dst_lat], dist_km=round(dist, 3))
        return fallback, dist, True

    try:
        resp = loc.calculate_route(
            CalculatorName=ROUTE_CALCULATOR,
            DeparturePosition=[src_lon, src_lat],
            DestinationPosition=[dst_lon, dst_lat],
            IncludeLegGeometry=True,
        )
        dist_km = None
        if "Summary" in resp and "Distance" in resp["Summary"]:
            dist_km = float(resp["Summary"]["Distance"])
        legs = resp.get("Legs") or []
        if not legs:
            jlog(logging.WARNING, event="route_no_legs", resp_summary=resp.get("Summary"))
            return fallback, (dist_km or haversine_km(src_lat, src_lon, dst_lat, dst_lon)), True
        geom = legs[0].get("Geometry") or {}
        line = geom.get("LineString")
        if not line or not isinstance(line, list):
            jlog(logging.WARNING, event="route_no_linestring")
            return fallback, (dist_km or haversine_km(src_lat, src_lon, dst_lat, dst_lon)), True

        coords = []
        for pt in line:
            if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                try:
                    coords.append([float(pt[0]), float(pt[1])])
                except Exception:
                    jlog(logging.DEBUG, event="route_point_cast_fail", point=pt)
        if not coords:
            jlog(logging.WARNING, event="route_empty_coords_after_parse")
            return fallback, (dist_km or haversine_km(src_lat, src_lon, dst_lat, dst_lon)), True

        jlog(logging.DEBUG, event="route_calc_ok", points=len(coords),
             dist_km=dist_km, fallback=False)
        return coords, (dist_km or haversine_km(src_lat, src_lon, dst_lat, dst_lon)), False

    except (ClientError, BotoCoreError) as e:
        jlog(logging.WARNING, event="route_client_error", error=str(e))
        return fallback, haversine_km(src_lat, src_lon, dst_lat, dst_lon), True
    except Exception as e:
        jlog(logging.WARNING, event="route_generic_error", error=str(e))
        return fallback, haversine_km(src_lat, src_lon, dst_lat, dst_lon), True

def make_trips_from_stays(stays):
    trips = []
    if not isinstance(stays, list):
        jlog(logging.ERROR, event="stays_not_list", actual_type=type(stays).__name__)
        return trips
    if len(stays) < 2:
        jlog(logging.INFO, event="stays_too_short", length=len(stays))
        return trips

    # 時系列で確実に並べる
    def _key(s):
        return s.get("end") or s.get("start") or ""

    stays_sorted = sorted(stays, key=_key)
    jlog(logging.INFO, event="stays_sorted", input_len=len(stays), sorted_len=len(stays_sorted))

    # スキップ統計
    skipped_missing_center = 0
    created = 0

    for i in range(len(stays_sorted) - 1):
        a = stays_sorted[i]
        b = stays_sorted[i + 1]

        if not _stay_center_ok(a, idx=i) or not _stay_center_ok(b, idx=i+1):
            skipped_missing_center += 1
            continue

        a_c = a.get("center") or {}
        b_c = b.get("center") or {}

        src_lat = _safe_float(a_c.get("lat"), "from.lat", i)
        src_lon = _safe_float(a_c.get("lon"), "from.lon", i)
        dst_lat = _safe_float(b_c.get("lat"), "to.lat", i+1)
        dst_lon = _safe_float(b_c.get("lon"), "to.lon", i+1)

        coords, dist_km, fallback = build_route_coords(src_lon, src_lat, dst_lon, dst_lat)

        trip = {
            "from": {
                "time": a.get("end") or a.get("start"),
                "lat": src_lat, "lon": src_lon,
                "label": a.get("label")
            },
            "to": {
                "time": b.get("start") or b.get("end"),
                "lat": dst_lat, "lon": dst_lon,
                "label": b.get("label")
            },
            "distance_km": round(float(dist_km), 3) if dist_km is not None else None,
            "fallback": bool(fallback)
        }
        trips.append((trip, coords))
        created += 1

        if DEBUG_MODE:
            jlog(logging.DEBUG, event="trip_created",
                 idx=i, from_time=trip["from"]["time"], to_time=trip["to"]["time"],
                 from_label=trip["from"].get("label"), to_label=trip["to"].get("label"),
                 distance_km=trip["distance_km"], fallback=trip["fallback"],
                 coord_points=len(coords))

    jlog(logging.INFO, event="trips_build_summary",
         created=created, skipped_missing_center=skipped_missing_center)
    return trips

def save_outputs(bucket, base_prefix, trips_with_coords):
    trips_only = [t for (t, _) in trips_with_coords]

    features = []
    for t, coords in trips_with_coords:
        feat = {
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {
                "type": "trip",
                "from_time": t["from"]["time"],
                "to_time": t["to"]["time"],
                "from_label": t["from"].get("label"),
                "to_label": t["to"].get("label"),
                "distance_km": t.get("distance_km"),
                "fallback": t.get("fallback", False),
            },
        }
        features.append(feat)

    geojson = {"type": "FeatureCollection", "features": features}
    trips_key = f"{base_prefix}/trips.json"
    geo_key   = f"{base_prefix}/geojson.json"

    write_json(bucket, trips_key, trips_only)
    write_json(bucket, geo_key, geojson)
    jlog(logging.INFO, event="outputs_written",
         bucket=bucket, base_prefix=base_prefix,
         trips=len(trips_only), features=len(features))


def lambda_handler(event, context):
    """
    S3 イベントを想定。stays_enriched.json に反応して trips/geojson を作る。
    ローカルテスト時は S3 のキーを直指定する event でも OK:
      {"bucket":"<BUCKET>","key":"processed/<dev>/date=YYYY-MM-DD/stays_enriched.json"}
    """
    jlog(logging.INFO, event="lambda_start",
         aws_region=AWS_REGION, route_calculator=bool(ROUTE_CALCULATOR),
         raw_bucket=S3_BUCKET, debug_mode=DEBUG_MODE)

    if not event:
        jlog(logging.WARNING, event="no_event")
        return {"ok": False, "error": "no event"}

    records = event.get("Records")
    if not records:
        bkt = event.get("bucket") or S3_BUCKET
        key = event.get("key")
        jlog(logging.INFO, event="manual_invoke", bucket=bkt, key=key)
        if not key:
            return {"ok": False, "error": "no key"}
        return _process_one(bkt, key)

    ok = 0
    seen = 0
    for rec in records:
        seen += 1
        s3i = rec.get("s3", {})
        bkt = (s3i.get("bucket") or {}).get("name") or S3_BUCKET
        key = (s3i.get("object") or {}).get("key")
        jlog(logging.DEBUG, event="record_received", raw_bucket=bkt, raw_key=key)

        if not key:
            jlog(logging.WARNING, event="record_missing_key")
            continue

        key = unquote_plus(key).lstrip("/")
        while "//" in key:
            key = key.replace("//", "/")
        jlog(logging.INFO, event="record_normalized", bucket=bkt, key=key)

        try:
            res = _process_one(bkt, key)
            if res.get("ok"):
                ok += 1
        except Exception as e:
            jlog(logging.ERROR, event="process_exception", bucket=bkt, key=key, error=str(e))

    return {"ok": ok > 0, "processed": ok, "records_seen": seen}

def _process_one(bucket, key):
    jlog(logging.INFO, event="process_one_begin",
         env_raw_bucket=S3_BUCKET, event_bucket=bucket, key=key)

    if S3_BUCKET and bucket != S3_BUCKET:
        jlog(logging.INFO, event="skip_other_bucket", bucket=bucket, expect=S3_BUCKET)
        return {"ok": False, "skip": True, "reason": "bucket_mismatch"}

    if not (key.endswith("stays_enriched.json") and key.startswith("processed/")):
        jlog(logging.INFO, event="skip_non_enriched_key", key=key)
        return {"ok": False, "skip": True, "reason": "key_not_target"}

    jlog(logging.INFO, event="processing_key", bucket=bucket, key=key)
    stays = read_json(bucket, key)

    base_prefix = key.rsplit("/", 1)[0]

    if not isinstance(stays, list) or not stays:
        jlog(logging.WARNING, event="stays_empty_or_invalid",
             type=type(stays).__name__, length=(len(stays) if isinstance(stays, list) else None),
             bucket=bucket, key=key)
        write_json(bucket, f"{base_prefix}/trips.json", [])
        write_json(bucket, f"{base_prefix}/geojson.json", {"type": "FeatureCollection", "features": []})
        return {"ok": True, "empty": True, "base_prefix": base_prefix}

    trips_with_coords = make_trips_from_stays(stays)
    jlog(logging.INFO, event="trips_built", count=len(trips_with_coords))
    save_outputs(bucket, base_prefix, trips_with_coords)

    return {"ok": True, "count": len(trips_with_coords), "base_prefix": base_prefix}
